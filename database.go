package gocouch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Database contains connection to couchdb instance and db name
// auth is inherited from Server on creation, but you can change it anytime
type Database struct {
	conn *connection
	auth Auth
	Name string
}

// DBInfo describes a database information
type DBInfo struct {
	CommitedUpdateSeq int    `json:"committed_update_seq"`
	CompactRunning    bool   `json:"compact_running"`
	Name              string `json:"db_name"`
	DiskVersion       int    `json:"disk_format_version"`
	DataSize          int    `json:"data_size"`
	DiskSize          int    `json:"disk_size"`
	DocCount          int    `json:"doc_count"`
	DocDelCount       int    `json:"doc_del_count"`
	StartTime         string `json:"instance_start_time"`
	PurgeSeq          int    `json:"purge_seq"`
	UpdateSeq         int    `json:"update_seq"`
}

// ViewResult contains result of a view request
type ViewResult struct {
	Offset    int                      `json:"offset"`
	Rows      []map[string]interface{} `json:"rows"`
	TotalRows int                      `json:"total_rows"`
	UpdateSeq int                      `json:"update_seq"`
}

// UpdateResult contains information about bulk request
type UpdateResult struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
	Ok  bool   `json:"ok"`
}

const appJSON string = "application/json"

func queryURL(path ...string) string {
	var URL = "/"
	for _, item := range path {
		URL = URL + "/" + item
	}
	return strings.TrimRight(URL, "/")
}

// GetDatabase checks existence of specified database on couchdb instance and return it
func (srv *Server) GetDatabase(name string, auth Auth) (*Database, error) {
	resp, err := srv.conn.request("HEAD", queryURL(name), nil, nil, auth, 0)
	if err != nil {
		if resp.StatusCode == 404 {
			return nil, errors.New("Not Found")
		}
		return nil, err
	}
	return &Database{conn: srv.conn, auth: auth, Name: name}, nil
}

// MustGetDatabase return database instance if it's present on server or creates new one
func (srv *Server) MustGetDatabase(name string, auth Auth) (*Database, error) {
	db, err := srv.GetDatabase(name, auth)
	if err != nil {
		if !strings.Contains(err.Error(), "Not Found") {
			return nil, err
		}
		db, err = srv.CreateDB(name)
		if err != nil {
			return nil, err
		}
		return db, nil
	}
	return db, nil
}

// Info returns DBInfo struct containing information about current database
func (db *Database) Info() (*DBInfo, error) {
	var out DBInfo
	resp, err := db.conn.request("GET", queryURL(db.Name), nil, nil, db.auth, 0)
	if err != nil {
		return nil, err
	}
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateDB creates database on couchdb instance and if successfull returns it
func (srv *Server) CreateDB(name string) (*Database, error) {
	_, err := srv.conn.request("PUT", queryURL(name), nil, nil, srv.auth, 0)
	if err != nil {
		return nil, err
	}
	return &Database{conn: srv.conn, auth: srv.auth, Name: name}, nil
}

// Delete deletes datanase on chouchdb instance
func (db *Database) Delete() error {
	_, err := db.conn.request("DELETE", queryURL(db.Name), nil, nil, db.auth, 0)
	return err
}

// Insert creates new document in database. If inserted struct does not contain
// `_id` field (you may set it using json tag) couchdb will generate
// itself _id for this document and return it to you with revision.
// `fullCommit` overrides commit policy of couchdb instance, preferred value is `false`,
// more - http://docs.couchdb.org/en/1.6.1/config/couchdb.html#couchdb/delayed_commits
// Note: client takes only exported fields of a document (also all json flags are supported)
func (db *Database) Insert(doc interface{}, batch, fullCommit bool) (id, rev string, err error) {
	headers := make(map[string]string)
	headers["Content-Type"] = appJSON
	if fullCommit {
		headers["X-Couch-Full-Commit"] = fmt.Sprint(fullCommit)
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", "", err
	}
	URL := queryURL(db.Name)
	if batch {
		URL = URL + "?batch=ok"
	}
	resp, err := db.conn.request("POST", URL, headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return "", "", err
	}
	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil {
		return "", "", err
	}
	switch result["id"].(type) {
	case string:
		id = result["id"].(string)
	}
	switch result["rev"].(type) {
	case string:
		rev = result["rev"].(string)
	}
	return id, rev, nil
}

// GetAllDocs function returns pointer to ViewResult
func (db *Database) GetAllDocs(options Options) (*ViewResult, error) {
	URL := ""
	for k, v := range options {
		URL = URL + fmt.Sprintf("&%s=%v", k, v)
	}
	if len(options) > 0 {
		URL = queryURL(db.Name, "_all_docs") + "?" + strings.Trim(URL, "&")
	} else {
		URL = queryURL(db.Name, "_all_docs")
	}
	resp, err := db.conn.request("GET", URL, nil, nil, db.auth, 0)
	if err != nil {
		return nil, err
	}
	var payload ViewResult
	if err := parseBody(resp, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetAllDocsByIDs does same as GetAllDocs but in defferent way
func (db *Database) GetAllDocsByIDs(keys []string, options Options) (*ViewResult, error) {
	URL := ""
	for k, v := range options {
		URL = URL + fmt.Sprintf("&%s=%v", k, v)
	}
	if len(options) > 0 {
		URL = queryURL(db.Name, "_all_docs") + "?" + strings.Trim(URL, "&")
	} else {
		URL = queryURL(db.Name, "_all_docs")
	}
	temp := make(map[string]interface{})
	headers := make(map[string]string)
	headers["Content-Type"] = appJSON
	temp["keys"] = keys
	payload, err := json.Marshal(temp)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", URL, headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var result ViewResult
	if err := parseBody(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update represents bulk operations on couchdb instance. It's highly customizable
// and not considered to use in common ways, use InsertMany or MustInsertMany instead.
//
// If you are inserting documents that have field that represent it's revision
// ensure that it has json tag "omitempty" otherwise coucbdb will generate an error
func (db *Database) Update(docs interface{}, atomic, updateRev, fullCommit bool) ([]UpdateResult, error) {
	request := make(map[string]interface{})
	headers := make(map[string]string)
	headers["Content-Type"] = appJSON
	if fullCommit {
		headers["X-Couch-Full-Commit"] = fmt.Sprint(fullCommit)
	}
	request["docs"] = docs
	if !updateRev {
		request["new_edits"] = false
	}
	if atomic {
		request["all_or_nothing"] = true
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_bulk_docs"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out []UpdateResult
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// InsertMany is a shortcut to Update method. It saves documents to database,
// optional parameters are set to defaults. Consider use this method as primary
// insert operator
func (db *Database) InsertMany(docs interface{}) ([]UpdateResult, error) {
	return db.Update(docs, false, true, true)
}

// MustInsertMany is a shortcut for Update method, acts like InsertMany but
// enshures atomicity of request.
func (db *Database) MustInsertMany(docs interface{}) ([]UpdateResult, error) {
	return db.Update(docs, true, true, true)
}

// DeleteMany is a shortcut to Update method. Use it when you have documents
// you want to delete from database.
//
// Argument may be `[]interface{}` or `[]map[string]interface{}`
//
// If you pass a slice of struct note that they must have fields with
// tags "_id" and "_rev", or error will be returned
// For maps here is a similar requirement, they must have both "_id" and
// "_rev" keys
func (db *Database) DeleteMany(docs interface{}) ([]UpdateResult, error) {
	var payload []map[string]interface{}
	in := reflect.TypeOf(docs)
	switch in.Kind() {
	case reflect.Slice:
		if in.Elem().Kind() == reflect.Map {
			for _, m := range docs.([]map[string]interface{}) {
				if _, ok := m["_id"]; !ok {
					return nil, errors.New("One of the maps, does not contain \"_id\" key")
				}
				if _, ok := m["_rev"]; !ok {
					return nil, errors.New("One of the maps, does not contain \"_rev\" key")
				}
				m["_deleted"] = true
				payload = append(payload, m)
			}
		} else if in.Elem().Kind() == reflect.Interface {
			for _, tempDoc := range docs.([]interface{}) {
				dt := reflect.TypeOf(tempDoc)
				dv := reflect.ValueOf(tempDoc)
				temp := make(map[string]interface{})
				for k := 0; k < dt.NumField(); k++ {
					if dt.Field(k).Tag.Get("json") == "_id" {
						temp["_id"] = dv.Field(k).Interface().(string)
					}
					if strings.Contains(dt.Field(k).Tag.Get("json"), "_rev") {
						temp["_rev"] = dv.Field(k).Interface().(string)
					}
				}
				if _, ok := temp["_id"]; !ok {
					return nil, errors.New("One of documents miss \"_id\" field or tag")
				}
				if _, ok := temp["_rev"]; !ok {
					return nil, errors.New("One of documents miss \"_rev\" field or tag")
				}
				temp["_deleted"] = true
				payload = append(payload, temp)
			}
		} else {
			return nil, errors.New("Unsupported document type")
		}
	}
	return db.Update(payload, false, true, true)
}
