package gocouch

import (
	"strings"
	"testing"
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
	db, err := srv.GetDatabase("_users", nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	_, err = srv.GetDatabase("test_database_not_exist", nil)
	if err != nil {
		if !strings.Contains(err.Error(), "Not Found") {
			t.Logf("Error: %v", err)
			t.Fail()
		}
	}
	return db
}

func TestGetDBInfo(t *testing.T) {
	db := getDatabase(t)
	info, err := db.Info()
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if info.Name != "_users" {
		t.Log("Incorrect db name")
		t.Fail()
	}
}

func TestCreateDB(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.CreateDB("test_creation_db")
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	_, err = srv.GetDatabase(db.Name, srv.auth)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	db.Delete()
}

func TestDeleteDB(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.CreateDB("test_creation_db2")
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	err = db.Delete()
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
}

func TestMustGetDatabase(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("any_database", nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	db.Delete()
	db, err = srv.MustGetDatabase("_users", nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
}

func TestInsert(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("test_insert", nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	var doc1 TestDoc
	doc1.SomeField1 = "some string"
	doc1.SomeField2 = 10
	if _, _, err = db.Insert(&doc1, false, false); err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	var doc2 TestDoc2
	doc2.SomeField1 = "some other field"
	doc2.SomeField2 = 123
	doc2.ID = "superID"
	id, _, err := db.Insert(&doc2, true, true)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if id != "superID" {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	db.Delete()
}

func TestGetAllDocs(t *testing.T) {
	db := getDatabase(t)
	result, err := db.GetAllDocs(nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(result.Rows) != 1 {
		t.Log("Incorrect row count")
		t.Fail()
	}
	result, err = db.GetAllDocs(Options{"include_docs": true})
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if _, ok := result.Rows[0]["doc"]; !ok {
		t.Log("Expected doc but got nothing")
		t.Fail()
	}
}

func TestGetAllDocsByIDs(t *testing.T) {
	db := getDatabase(t)
	res, err := db.GetAllDocsByIDs([]string{"_design/_auth"}, nil)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(res.Rows) < 1 {
		t.Log("Incorrect row count")
		t.Fail()
	}
	res, err = db.GetAllDocsByIDs([]string{"_design/_auth"}, Options{"include_docs": true})
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if _, ok := res.Rows[0]["doc"]; !ok {
		t.Log("Expected doc but got nothing")
		t.Fail()
	}
}

func TestDBUpdate(t *testing.T) {
	srv := getConnection(t)
	db, _ := srv.MustGetDatabase("test_db_update", nil)
	var payload []TestDoc
	// test docs with no ids
	payload = append(payload, TestDoc{"a", 1}, TestDoc{"b", 2})
	result, err := db.InsertMany(payload)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(result) != 2 {
		t.Log("Incorrect result count")
		t.Fail()
	}
	// test docs with ids
	var payload2 []TestDoc2
	payload2 = append(payload2, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, err = db.MustInsertMany(payload2)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(result) != 2 || result[0].ID != "1" || result[1].ID != "2" {
		t.Log("Incorrect result with predefined ids")
		t.Fail()
	}
	db.Delete()
}

func TestDeleteMany(t *testing.T) {
	srv := getConnection(t)
	db, _ := srv.MustGetDatabase("test_delete_many", nil)
	var payload []TestDoc2
	payload = append(payload, TestDoc2{"a", 1, "1", ""}, TestDoc2{"b", 2, "2", ""})
	result, _ := db.MustInsertMany(payload)
	var payload2 []interface{}
	for _, item := range result {
		payload2 = append(payload2, TestDoc2{ID: item.ID, Rev: item.Rev})
	}
	_, err := db.DeleteMany(payload2)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	res, err := db.GetAllDocs(nil)
	if len(res.Rows) > 0 {
		t.Log("Documents was not deleted")
		t.Fail()
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
		t.Logf("Error: %v", err)
		t.Fail()
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
			t.Logf("Error: %v", err)
			t.Fail()
		}
	}
	db.Delete()
}
