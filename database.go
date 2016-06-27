package gocouch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"bufio"
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

type DatabaseChanges struct {
	LastSequence int `json:"last_seq"`
	Rows         []DatabaseEvent `json:"results"`
}

type DatabaseEvent struct {
	Changes []map[string]string `json:"changes"`
	ID      string `json:"id"`
	Seq     int `json:"seq"`
	Deleted bool `json:"deleted"`
}

type PurgeResult struct {
	PurgeSequence int `json:"purge_seq"`
	Purged        map[string][]string `json:"purged"`
}

const appJSON string = "application/json"

func queryURL(path ...string) string {
	var URL = ""
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

// Returns a copy of current database instance with newly created connection
func (db *Database) copy() (*Database, error) {
	conn, err := createConnection(db.conn.url, db.conn.client.Timeout)
	if err != nil {
		return nil, err
	}
	return &Database{conn: conn, auth: db.auth, Name: db.Name}, nil
}

// CreateDB creates database on couchdb instance and if successful returns it
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

// delete uses Update method to perform bult delete operations
func (db *Database) delete(docs interface{}, atomic, updateRev, fullCommit bool) (result []UpdateResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = errors.New("Invalid argument type, expected []map[string]interface{} or []interface{}")
		}
	}()
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
					// todo: add field checks
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
	default:
		return nil, errors.New("DeleteMany accepts only slices")
	}
	result, err = db.Update(payload, atomic, updateRev, fullCommit)
	return
}

// DeleteMany is a shortcut to delete method. Use it when you have documents
// you want to delete from database.
//
// Argument may be `[]interface{}` or `[]map[string]string`
//
// If you pass a slice of struct note that they must have fields with
// tags "_id" and "_rev", or error will be returned
// For maps here is a similar requirement, they must have both "_id" and
// "_rev" keys
func (db *Database) DeleteMany(docs interface{}) ([]UpdateResult, error) {
	return db.delete(docs, false, true, true)
}

// MustDeleteMany acts same like DeleteMany but ensures all documents to be deleted
func (db *Database) MustDeleteMany(docs interface{}) ([]UpdateResult, error) {
	return db.delete(docs, true, true, true)
}

// GetAllChanges fetches all changes for current database
func (db *Database) GetAllChanges(options Options) (*DatabaseChanges, error) {
	var query string
	if val, ok := options["feed"]; ok && val.(string) == "continuous" {
		return nil, errors.New("This method does not support listening for continuous events, use GetChangesChan instead")
	}
	for k, v := range options {
		query = query + fmt.Sprintf("&%s=%v", k, v)
	}
	if len(options) > 0 {
		query = "_changes?" + strings.Trim(query, "&")
	} else {
		query = "_changes"
	}
	resp, err := db.conn.request("GET", queryURL(db.Name, query), nil, nil, db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out DatabaseChanges
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (db *Database) GetChangesChan(options Options) (c chan DatabaseEvent, err error) {
	var query string
	c = make(chan DatabaseEvent)
	if val, ok := options["feed"]; ok && val.(string) != "continuous" {
		return nil, errors.New("This method supports only listening for continuous events, use GetAllChanges instead")
	} else if ok && val.(string) == "continuous" {
		delete(options, "feed")
	}
	for k, v := range options {
		query = query + fmt.Sprintf("%s=%v", k, v)
	}
	if len(options) > 0 {
		query = "_changes?feed=continuous" + strings.Trim(query, "&")
	} else {
		query = "_changes?feed=continuous"
	}
	cpdb, err := db.copy()
	if err != nil {
		return
	}
	resp, err := cpdb.conn.request("GET", queryURL(db.Name, query), nil, nil, cpdb.auth, 0)
	if err != nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// channel closed externally, close the connection
				resp.Body.Close()
				switch r.(type) {
				case error:
					// catch goroutine errors
					err = err
				}
			}
		}()
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				panic(errors.New("Error: Failed to read bytes from connection"))
			}
			var payload DatabaseEvent
			err = json.Unmarshal(line, &payload)
			if err != nil {
				panic(errors.New("Error:  Failed to unmarshal bytes to DatabaseEvent"))
			}
			c <- payload
		}
	}()
	return
}

func (db *Database) compact(doc_name string) error {
	var URL string
	headers := map[string]string{"Content-Type": "application/json"}
	if doc_name == "" {
		URL = queryURL(db.Name, "_compact")
	} else {
		URL = queryURL(db.Name, "_compact", doc_name)
	}
	resp, err := db.conn.request("POST", URL, headers, nil, db.auth, 0)
	if err != nil {
		return err
	}
	var result map[string]bool
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	if val, ok := result["ok"]; !val || !ok {
		return errors.New("Compaction of database failed")
	}
	return nil
}

func (db *Database) Compact() error {
	return db.compact("")
}

func (db *Database) CompactDesign(doc_name string) error {
	return db.compact(doc_name)
}

func (db *Database) EnsureFullCommit() error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_ensure_full_commit"), headers, nil, db.auth, 0)
	if err != nil {
		return err
	}
	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	if val, ok := result["ok"]; !ok || !val.(bool) {
		return errors.New("Commit failed")
	}
	return nil
}

func (db *Database) ViewCleanup() error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_view_cleanup"), headers, nil, db.auth, 0)
	if err != nil {
		return err
	}
	var result map[string]bool
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	return nil
}

func (db *Database) AddAdmin(login string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateAdmins(login, false)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) DeleteAdmin(login string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateAdmins(login, true)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) AddAdminRole(role string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateAdminRoles(role, false)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) DeleteAdminRole(role string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateAdminRoles(role, true)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) AddMember(login string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateMembers(login, false)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) DeleteMember(login string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateMembers(login, true)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) AddMemberRole(role string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateMemberRoles(role, false)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) DeleteMemberRole(role string) error {
	var so BaseSecurity
	if err := db.GetSecurity(&so); err != nil {
		return err
	}
	so.UpdateMemberRoles(role, true)
	if err := db.SetSecurity(&so); err != nil {
		return err
	}
	return nil
}

func (db *Database) GetTempView(_map, reduce string) (*ViewResult, error) {
	// todo: invoke common View method
	return nil, nil
}

func (db *Database) Purge(o map[string][]string) (*PurgeResult, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_purge"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out PurgeResult
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (db *Database) GetMissedRevs(o map[string][]string) (map[string]map[string][]string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_missing_revs"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out map[string]map[string][]string
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *Database) GetRevsDiff(o map[string][]string) (map[string]map[string][]string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(db.Name, "_revs_diff"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out map[string]map[string][]string
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *Database) GetRevsLimit() (count int, err error) {
	resp, err := db.conn.request("GET", queryURL(db.Name, "_revs_limit"), nil, nil, db.auth, 0)
	if err != nil {
		return 0, err
	}
	if err := parseBody(resp, &count); err != nil {
		return 0, err
	}
	return count, nil
}

func (db *Database) SetRevsLimit(count int) error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("PUT", queryURL(db.Name, "_revs_limit"), headers, bytes.NewBuffer([]byte(fmt.Sprint(count))), db.auth, 0)
	if err != nil {
		return err
	}
	var res map[string]bool
	if err := parseBody(resp, &res); err != nil || !res["ok"] {
		return err
	}
	return nil
}