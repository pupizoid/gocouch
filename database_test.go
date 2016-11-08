package gocouch

import (
	"strings"
	"testing"
	"bytes"
	"io/ioutil"
)

type TestDoc struct {
	SomeField1 string `json:"field1"`
	SomeField2 int    `json:"field2"`
}

type TestDoc2 struct {
	SomeField1 string
	SomeField2 int
	ID         string `json:"_id"`
	Rev        string `json:"_rev,omitempty"`
}

func getDatabase(t *testing.T) *Database {
	srv := getConnection(t)
	db, err := srv.GetDatabase("_users", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return nil
	}
	_, err = srv.GetDatabase("database_not_exist", BasicAuth{"admin", "admin"})
	if err != nil {
		if !strings.Contains(err.Error(), "Not Found") {
			t.Logf("Error: %v\n", err)
			t.Fail()
			return nil
		}
	}
	return db
}

func TestDatabase_Info(t *testing.T) {
	db := getDatabase(t)
	info, err := db.Info()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if info.Name != "_users" {
		t.Log("Incorrect db name")
		t.Fail()
		return
	}
}

func TestServer_CreateDB(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.CreateDB("creation_db")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	_, err = srv.GetDatabase(db.Name, srv.auth)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db.Delete()
}

func TestDatabase_Delete(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.CreateDB("creation_db2")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	err = db.Delete()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestServer_MustGetDatabase(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("any_database", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db.Delete()
	db, err = srv.MustGetDatabase("_users", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_Insert(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("insert", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	var doc1 TestDoc
	doc1.SomeField1 = "some string"
	doc1.SomeField2 = 10
	if _, _, err = db.Insert(&doc1, false, false); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	var doc2 TestDoc2
	doc2.SomeField1 = "some other field"
	doc2.SomeField2 = 123
	doc2.ID = "superID"
	id, _, err := db.Insert(&doc2, false, false)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if id != "superID" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db.Delete()
}

func TestDatabase_GetAllDocs(t *testing.T) {
	db := getDatabase(t)
	result, err := db.GetAllDocs(nil)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(result.Rows) < 1 {
		t.Log("Incorrect row count")
		t.Fail()
		return
	}
	result, err = db.GetAllDocs(Options{"include_docs": true})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if _, ok := result.Rows[0]["doc"]; !ok {
		t.Log("Expected doc but got nothing")
		t.Fail()
		return
	}
}

func TestDatabase_GetAllDocsByIDs(t *testing.T) {
	db := getDatabase(t)
	res, err := db.GetAllDocsByIDs([]string{"_design/_auth"}, nil)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(res.Rows) < 1 {
		t.Log("Incorrect row count")
		t.Fail()
		return
	}
	res, err = db.GetAllDocsByIDs([]string{"_design/_auth"}, Options{"include_docs": true})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if _, ok := res.Rows[0]["doc"]; !ok {
		t.Log("Expected doc but got nothing")
		t.Fail()
		return
	}
}

func TestDatabase_InsertMany(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("db_update", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	var payload []TestDoc
	// test docs with no ids
	payload = append(payload, TestDoc{"a", 1}, TestDoc{"b", 2})
	result, err := db.InsertMany(payload)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(result) != 2 {
		t.Log("Incorrect result count")
		t.Fail()
		return
	}
	// test docs with ids
	var payload2 []TestDoc2
	payload2 = append(payload2, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, err = db.MustInsertMany(payload2)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(result) != 2 || result[0].ID != "1" || result[1].ID != "2" {
		t.Log("Incorrect result with predefined ids")
		t.Fail()
		return
	}
	db.Delete()
}

func TestDatabase_DeleteMany(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("delete_many", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	var payload []TestDoc2
	payload = append(payload, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, _ := db.MustInsertMany(payload)
	var payload2 []interface{}
	for _, item := range result {
		payload2 = append(payload2, TestDoc2{ID: item.ID, Rev: item.Rev})
	}
	_, err = db.DeleteMany(payload2)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	res, err := db.GetAllDocs(nil)
	if len(res.Rows) > 0 {
		t.Log("Documents was not deleted")
		t.Fail()
		return
	}
	// test maps as arguments
	payload = []TestDoc2{}
	payload = append(payload, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, _ = db.MustInsertMany(payload)
	var payload3 []map[string]interface{}
	for _, item := range result {
		tmp := make(map[string]interface{})
		tmp["_id"] = item.ID
		tmp["_rev"] = item.Rev
		payload3 = append(payload3, tmp)
	}
	_, err = db.DeleteMany(payload3)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test invalid map or data type
	payload = []TestDoc2{}
	payload = append(payload, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, _ = db.MustInsertMany(payload)
	var payload4 []map[string]string
	for _, item := range result {
		tmp := make(map[string]string)
		tmp["_id"] = string(item.ID)
		tmp["_rev"] = string(item.Rev)
		payload4 = append(payload4, tmp)
	}
	_, err = db.MustDeleteMany(payload4)
	if err != nil {
		if !strings.Contains(err.Error(), "Invalid") {
			t.Logf("Error: %v\n", err)
			t.Fail()
			return
		}
	}
	db.Delete()
}

func TestDatabase_GetAllChanges(t *testing.T) {
	db := getDatabase(t)
	changes, err := db.GetAllChanges(nil)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if changes.LastSequence < 1 || len(changes.Rows) < 1 {
		t.Log("Incorrect changes object")
		t.Logf("%#v", changes)
		t.Fail()
		return
	}
	// check database chan
	srv := getConnection(t)
	db, err = srv.MustGetDatabase("db_changes", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	events, err := db.GetChangesChan(nil)
	defer func() {
		close(events)
	}()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	go func() {
		db.Insert(map[string]string{"_id": "id"}, false, true)
	}()
	if msg := <-events; len(msg.Changes) < 1 && msg.ID != "id" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db.Insert(map[string]string{"_id": "id_2"}, false, true)
	if msg := <-events; len(msg.Changes) < 1 && msg.ID != "id_2" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test channel overflow
	for i := 0; i < 5; i++ {
		db.Insert(map[string]string{"some_field": "id_2"}, false, true)
	}
	// channel can't accept new messages
	if msg := <-events; len(msg.Changes) < 1 {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db.Delete()
}

func TestDatabase_Compact(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("db_compaction", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	if err := db.Compact(); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_CompactDesign(t *testing.T) {
	db := getDatabase(t)
	if err := db.CompactDesign("_auth"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_EnsureFullCommit(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("full_commit", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if err := db.EnsureFullCommit(); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_ViewCleanup(t *testing.T) {
	db := getDatabase(t)
	if err := db.ViewCleanup(); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_AddAdmin(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_admin", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.AddAdmin("admin"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_DeleteAdmin(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_admin", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.DeleteAdmin("admin"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_AddAdminRole(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_admin_role", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.AddAdminRole("sudo"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_DeleteAdminRole(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_admin_role", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.DeleteAdminRole("sudo"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_AddMember(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_member", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.AddMember("member"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_DeleteMember(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_member", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.DeleteMember("member"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_AddMemberRole(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_member", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.AddMemberRole("dev"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_DeleteMemberRole(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("add_member", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	dbSec := db.GetDatabaseSecurity()
	if err := dbSec.DeleteMemberRole("dev"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_Purge(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("purge1", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	_, rev, _ := db.Insert(map[string]string{"_id": "test", "field": "some_field"}, false, true)
	var del []map[string]interface{}
	temp := make(map[string]interface{})
	temp["_id"] = "test"
	temp["_rev"] = rev
	del = append(del, temp)
	res, err := db.DeleteMany(del)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	res1, err := db.Purge(map[string][]string{"test": []string{res[0].Rev}})
	if err != nil || res1.Purged["test"][0] != res[0].Rev {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_GetMissedRevs(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("missing_revs", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	id, rev, err := db.Insert(map[string]string{"field": "value"}, false, false)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	payload := map[string][]string{id: []string{rev, "6-460637e73a6288cb24d532bf91f32969"}}
	result, err := db.GetMissedRevs(payload)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if result["missing_revs"][id][0] != "6-460637e73a6288cb24d532bf91f32969" {
		t.Logf("Incorrect result: %v\n", result)
		t.Fail()
		return
	}
}

func TestDatabase_GetRevsDiff(t *testing.T) {
	// todo: test it with single document update api
}

func TestDatabase_GetRevsLimit(t *testing.T) {
	db := getDatabase(t)
	rvl, err := db.GetRevsLimit()
	if err != nil && rvl != 1000 {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_SetRevsLimit(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("revs_limit", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	if err := db.SetRevsLimit(500); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	new, err := db.GetRevsLimit()
	if err != nil && new != 500 {
		t.Logf("Unexpected rev limit: %v\n", new)
		t.Fail()
		return
	}
}

func TestDatabase_Exists(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("doc_info", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	id, rev, err := db.Insert(map[string]string{"test": "test1"}, false, false)
	size, rev1, err := db.Exists(id, Options{"attachments": true})
	if size == 0 || err != nil || rev1 != rev {
		t.Logf("Unexpected size: %v\n", size)
		t.Logf("Unexpected rev: %v != %v\n", rev1, rev)
		t.Logf("Unexpected err: %v\n", err)
		t.Fail()
		return
	}
}

func TestDatabase_Get(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("get_doc", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	id, rev, _ := db.Insert(map[string]string{"test": "test"}, false, false)
	var sampleDoc struct {
		ID   string `json:"_id"`
		Rev  string `json:"_rev"`
		Test string `json:"test"`
	}
	if err := db.Get(id, &sampleDoc, nil); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if sampleDoc.Rev != rev || sampleDoc.Test != "test" {
		t.Logf("Got unexpected document: %#v\n", sampleDoc)
		t.Fail()
		return
	}
}

func TestDatabase_Put(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("doc_put", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	rev, err := db.Put("test_id", map[string]string{"test_field": "value"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	rev1, err := db.Put("test_id", map[string]string{"test_field": "value2", "_rev": rev})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	var res map[string]interface{}
	if err := db.Get("test_id", &res, nil); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if res["_rev"] != rev1 {
		t.Logf("Unexpected rev: %s != %s\n", rev, rev1)
		t.Fail()
		return
	}
}

func TestDatabase_Del(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("doc_del", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	rev, err := db.Put("test_del", map[string]string{"field": "value"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	rev1, err := db.Del("test_del", rev)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if rev1 == rev {
		t.Log("Revisions are equal")
		t.Fail()
		return
	}
}

func TestDatabase_Copy(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("copy_test", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	_, err = db.Put("copy_id", map[string]string{})
	rev1, err := db.Put("copy_id3", map[string]string{})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	rev2, err := db.Copy("copy_id", Destination{id: "copy_id2"}, nil)
	_, rev3, err := db.Exists("copy_id2", nil)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if rev3 != rev2 {
		t.Logf("Revs are not equal: %s != %s", rev3, rev2)
		t.Fail()
		return
	}
	rev4, err := db.Copy("copy_id", Destination{"copy_id3", Options{"rev": rev1}}, nil)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	_, rev5, _ := db.Exists("copy_id2", nil)
	if rev4 == rev5 {
		t.Logf("Revs are not equal: %s != %s", rev4, rev5)
		t.Fail()
		return
	}
}

func TestDatabase_Attachment(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("test_attachments", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	rev, err := db.Put("test_att_id", map[string]string{})
	att := &Attachment{"test_att", "text/plain", bytes.NewReader([]byte("test body"))}
	result, err :=  db.SaveAttachment("test_att_id", rev, att)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if result["ok"].(bool) != true {
		t.Log("Error: unexpected result")
		t.Fail()
		return
	}
	// Test Attachment info
	info, err := db.AttachmentInfo("test_att_id", "test_att")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if info.Type != "text/plain" {
		t.Log("Incorrect attachement type information")
		t.Fail()
		return
	}
	// Test get Attachment
	att1, err := db.GetAttachment("test_att_id", "test_att", "")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	payload, err := ioutil.ReadAll(att1.Body)
	//t.Logf("Payload: %s", payload)
	if string(payload) != "test body" || err != nil {
		t.Log("Incorrect attachement body")
		t.Fail()
		return
	}
	// test delete attachment
	err = db.DelAttachment("test_att_id", "test_att", result["rev"].(string))
	if err != nil {
		t.Log("Error deleting attachment")
		t.Fail()
		return
	}
}
