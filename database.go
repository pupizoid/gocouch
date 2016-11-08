package gocouch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"io"
	"io/ioutil"
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

// DatabaseChanges represents all changes related to current database
type DatabaseChanges struct {
	LastSequence int             `json:"last_seq"`
	Rows         []DatabaseEvent `json:"results"`
}

// DatabaseEvent represents a single change to current database
type DatabaseEvent struct {
	Changes []map[string]string `json:"changes"`
	ID      string              `json:"id"`
	Seq     int                 `json:"seq"`
	Deleted bool                `json:"deleted"`
}

// PurgeResult provides object with result info of purge request
type PurgeResult struct {
	PurgeSequence int                 `json:"purge_seq"`
	Purged        map[string][]string `json:"purged"`
}

// Destination describes `id` of a document and allow to add some
// options to use in queries
type Destination struct {
	id      string
	options Options
}

// Attachment represents attachment to be stored in couchdb
type Attachment struct {
	Name, ContentType string
	Body              io.Reader
}

// AttachmentInfo provides information about attachment
type AttachmentInfo struct {
	Encoding string
	Length   int
	Type     string
	Hash     string
}

const appJSON string = "application/json"
const continuous string = "continuous"

func queryURL(path ...string) string {
	var URL = ""
	for _, item := range path {
		URL = URL + "/" + item
	}
	return strings.TrimRight(URL, "/")
}

func (d *Destination) String() (url string) {
	for k, v := range d.options {
		url = url + fmt.Sprintf("%s=%v&", k, v)
	}
	if len(d.options) > 0 {
		url = d.id + "?" + strings.Trim(url, "&")
	} else {
		url = d.id
	}
	return
}

// GetDatabase checks existence of specified database on couchdb instance and return it
func (srv *Server) GetDatabase(name string, auth Auth) (*Database, error) {
	var useAuth Auth
	if auth != nil {
		useAuth = auth
	} else {
		useAuth = srv.auth
	}
	resp, err := srv.conn.request("HEAD", queryURL(name), nil, nil, useAuth, 0)
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
func (db *Database) copy_db() (*Database, error) {
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
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_bulk_docs"), headers, bytes.NewReader(payload), db.auth, 0)
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

// delete uses Update method to perform bulk delete operations
func (db *Database) bulkDelete(docs interface{}, atomic, updateRev, fullCommit bool) (result []UpdateResult, err error) {
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
	return db.bulkDelete(docs, false, true, true)
}

// MustDeleteMany acts same like DeleteMany but ensures all documents to be deleted
func (db *Database) MustDeleteMany(docs interface{}) ([]UpdateResult, error) {
	return db.bulkDelete(docs, true, true, true)
}

// GetAllChanges fetches all changes for current database
func (db *Database) GetAllChanges(options Options) (*DatabaseChanges, error) {
	var query string
	if val, ok := options["feed"]; ok && val.(string) == continuous {
		return nil, errors.New(
			"This method does not support listening for continuous events, use GetChangesChan instead")
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

// GetChangesChan returns a channel from which a DatabaseEvents can be obtained
func (db *Database) GetChangesChan(options Options) (c chan DatabaseEvent, err error) {
	var query string
	c = make(chan DatabaseEvent)
	if val, ok := options["feed"]; ok && val.(string) != continuous {
		return nil, errors.New(
			"This method supports only listening for continuous events, use GetAllChanges instead")
	} else if ok && val.(string) == continuous {
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
	cpdb, err := db.copy_db()
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
					err = r.(error)
				}
			}
		}()
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				panic(errors.New(
					"Error: Failed to read bytes from connection"))
			}
			var payload DatabaseEvent
			err = json.Unmarshal(line, &payload)
			if err != nil {
				panic(errors.New(
					"Error:  Failed to unmarshal bytes to DatabaseEvent"))
			}
			c <- payload
		}
	}()
	return
}

// Low lewel Compact query to database
func (db *Database) compact(docName string) error {
	var URL string
	headers := map[string]string{"Content-Type": "application/json"}
	if docName == "" {
		URL = queryURL(db.Name, "_compact")
	} else {
		URL = queryURL(db.Name, "_compact", docName)
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

// Compact runs database compaction process
func (db *Database) Compact() error {
	return db.compact("")
}

// CompactDesign copacts the view indexes associated with specified design
// document
func (db *Database) CompactDesign(docName string) error {
	return db.compact(docName)
}

// EnsureFullCommit tells database to commit any recent changes to the
// specified database to disk
func (db *Database) EnsureFullCommit() error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_ensure_full_commit"), headers, nil, db.auth, 0)
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

// ViewCleanup removes view index files that are no longer required
// by couchdb instance
func (db *Database) ViewCleanup() error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_view_cleanup"), headers, nil, db.auth, 0)
	if err != nil {
		return err
	}
	var result map[string]bool
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	return nil
}

// GetTempView return a pointer to ViewResult with data from temporary view
// based on map and reduce functions passed into parameters
func (db *Database) GetTempView(_map, reduce string) (*ViewResult, error) {
	// todo: invoke common View method
	return nil, errors.New("not implemented yet")
}

// Purge permanently removes the referenses to deleted documents from the database
func (db *Database) Purge(o map[string][]string) (*PurgeResult, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_purge"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out PurgeResult
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetMissedRevs with a given list of revisions returns ones that do not exists
// in the database. List example:
//
// check_revs := map[string][]string{"document_id": []string{"rev1", "rev2"}}
//
func (db *Database) GetMissedRevs(o map[string][]string) (map[string]map[string][]string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_missing_revs"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out map[string]map[string][]string
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetRevsDiff returns the subset of those revs that do not correspond to stored
// in the database
func (db *Database) GetRevsDiff(o map[string][]string) (map[string]map[string][]string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("POST", queryURL(
		db.Name, "_revs_diff"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return nil, err
	}
	var out map[string]map[string][]string
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetRevsLimit gets the current database revision limit
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

// SetRevsLimit sets the current database revision limit
func (db *Database) SetRevsLimit(count int) error {
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := db.conn.request("PUT", queryURL(
		db.Name, "_revs_limit"), headers, bytes.NewBuffer([]byte(fmt.Sprint(count))), db.auth, 0)
	if err != nil {
		return err
	}
	var res map[string]bool
	if err := parseBody(resp, &res); err != nil || !res["ok"] {
		return err
	}
	return nil
}

// Exists checks if document with specified id is present in the database
func (db *Database) Exists(id string, options Options) (size int, rev string, err error) {
	var URL string
	for k, v := range options {
		URL = URL + fmt.Sprintf("%s=%v&", k, v)
	}
	if len(options) > 0 {
		URL = queryURL(db.Name, id) + "?" + strings.Trim(URL, "&")
	} else {
		URL = queryURL(db.Name, id)
	}
	resp, err := db.conn.request("HEAD", URL, nil, nil, db.auth, 0)
	defer resp.Body.Close()
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			err = errors.New("document not found")
		}
		return
	}
	size, err = strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return
	}
	rev = strings.Trim(resp.Header.Get("ETag"), "\"")
	return
}

// Get fetches single document by it's id
func (db *Database) Get(id string, o interface{}, options Options) error {
	var URL string
	for k, v := range options {
		URL = URL + fmt.Sprintf("%s=%v&", k, v)
	}
	if len(options) > 0 {
		URL = queryURL(db.Name, id) + "?" + strings.Trim(URL, "&")
	} else {
		URL = queryURL(db.Name, id)
	}
	resp, err := db.conn.request("GET", URL, nil, nil, db.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, o); err != nil {
		return err
	}
	return nil
}

// Put creates new document with specified id or adds new revision to the existing
// one.
// Note: to add new revision you must specify the latest rev of the document
// you want to update, otherwise couchdb will answer 409(Conflict)
func (db *Database) Put(id string, doc interface{}) (string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	resp, err := db.conn.request("PUT", queryURL(
		db.Name, id), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return "", err
	}
	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil {
		return "", err
	}
	if val, ok := result["ok"]; ok && val.(bool) {
		return result["rev"].(string), nil
	}
	return "", err
}

// Del adds new "_deleted" revision to the docuement with specified id
func (db *Database) Del(id, rev string) (string, error) {
	resp, err := db.conn.request("DELETE", queryURL(
		db.Name, id) + fmt.Sprintf("?rev=%s", rev), nil, nil, db.auth, 0)
	if err != nil {
		return "", err
	}
	var res map[string]interface{}
	if err := parseBody(resp, &res); err != nil {
		return "", err
	}
	if val, ok := res["ok"]; ok && val.(bool) {
		return res["rev"].(string), nil
	}
	return "", err
}

// Copy... copies docuement with specified id to newly created document, ot to
// existing one.
// Note: if you're copying document to the existing one, you must specify target
// latest revision, otherwise couchdb will return 409(Conflict)
func (db *Database) Copy(id string, dest Destination, options Options) (string, error) {
	var URL string
	for k, v := range options {
		URL = URL + fmt.Sprintf("%s=%v&", k, v)
	}
	if len(options) > 0 {
		URL = queryURL(db.Name, id) + "?" + strings.Trim(URL, "&")
	} else {
		URL = queryURL(db.Name, id)
	}
	resp, err := db.conn.request(
		"COPY", URL, map[string]string{"Destination": dest.String()}, nil, db.auth, 0)
	if err != nil {
		return "", err
	}
	var res map[string]interface{}
	if err := parseBody(resp, &res); err != nil {
		return "", err
	}
	if val, ok := res["ok"]; !ok || !val.(bool) {
		return "", err
	}
	return res["rev"].(string), nil
}

// SaveAttachment uploads given attachment to document
func (db *Database) SaveAttachment(id, rev string, a *Attachment) (map[string]interface{}, error) {
	headers := map[string]string{
		"Content-Type": a.ContentType,
	}
	resp, err := db.conn.request("PUT", queryURL(db.Name, id, fmt.Sprintf("%s?rev=%s", a.Name, rev)),
		headers, a.Body, db.auth, 0)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// AttachementInfo provides basic information about specified attachment
func (db *Database) AttachmentInfo(id, name string) (*AttachmentInfo, error) {
	resp, err := db.conn.request("HEAD", queryURL(db.Name, id, name), nil, nil, db.auth, 0)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	contentLength, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return nil, err
	}
	return &AttachmentInfo{
		Type: resp.Header.Get("Content-Type"),
		Length: contentLength,
		Hash: resp.Header.Get("Content-MD5"),
		Encoding: resp.Header.Get("Content-Encoding"),
	}, nil
}

// GetAttachment fetches attachement from database
func (db *Database) GetAttachment(id, name, rev string) (*Attachment, error) {
	var headers map[string]string
	if rev != "" {
		headers = map[string]string{"If-Match": rev}
	}
	info, err := db.AttachmentInfo(id, name)
	if err != nil {
		return nil, err
	}
	resp, err := db.conn.request("GET", queryURL(db.Name, id, name), headers, nil, db.auth, 0)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Attachment{
		Name: name,
		ContentType: info.Type,
		Body: bytes.NewReader(body),
	}, nil
}

// DelAttachment used for deleting document's attachments
func (db *Database) DelAttachment(id, name, rev string) error {
	var headers map[string]string
	if rev != "" {
		headers = map[string]string{"If-Match": rev}
	} else {
		return errors.New("Revision can't be empty")
	}

	resp, err := db.conn.request("DELETE", queryURL(db.Name, id, name), headers, nil, db.auth, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil { return err }

	if ok, val := result["ok"]; !ok || !val {
		return errors.New("Can't delete attachemnt")
	}
	return nil
}