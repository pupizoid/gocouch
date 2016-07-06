package gocouch

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func getConnection(t *testing.T) *Server {
	srv, err := Connect("localhost", 5984, BasicAuth{"admin", "admin"}, 0)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return nil
	}
	return srv
}

func TestServer_Info(t *testing.T) {
	srv := getConnection(t)
	info, err := srv.Info()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// check struct parse
	if info.Message == "" || info.UUID == "" || info.Version == "" {
		t.Logf("Incorrect struct returned: %#v", info)
		t.Fail()
		return
	}
}

func TestServer_GetActiveTasks(t *testing.T) {
	srv := getConnection(t)
	var result []map[string]interface{}
	err := srv.GetActiveTasks(&result)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// todo: add checking authorisation and some task appearance (continious replication)
}

func TestServer_GetAllDbs(t *testing.T) {
	srv := getConnection(t)
	var result []string
	err := srv.GetAllDbs(&result)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(result) < 1 {
		t.Log("Len of db names less than 1")
		t.Fail()
		return
	}
}

func TestServer_GetDBEvent(t *testing.T) {
	srv := getConnection(t)
	go func () {
		time.Sleep(time.Second)
		db, _ := srv.MustGetDatabase("db_events", BasicAuth{"admin", "admin"})
		defer db.Delete()
	}()
	var result map[string]interface{}
	if err := srv.GetDBEvent(&result, nil); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if _, ok := result["ok"]; !ok {
		t.Logf("Unexpected result: %v\n", result)
		t.Fail()
		return
	}
}

func TestServer_GetDBEventChan(t *testing.T) {
	srv := getConnection(t)
	events, err := srv.GetDBEventChan()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer func() {
		close(events)
	}()
	db, err := srv.MustGetDatabase("db_events_2", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if msg, ok := <- events; !ok || !strings.Contains(msg.Name, "db_events") {
		t.Logf("Error: %v\n", err)
		t.Logf("%#v\n", msg)
		t.Fail()
		return
	}
	db.Delete()
	if msg, ok := <- events; !ok || !strings.Contains(msg.Name, "db_events") {
		t.Logf("Error: %v\n", err)
		t.Logf("%#v\n", msg)
		t.Fail()
		return
	}
}

func TestServer_GetMembership(t *testing.T) {
	srv := getConnection(t)
	var result map[string][]string
	if err := srv.GetMembership(&result); err != nil {
		// membership only supported by couchdb 2.0
		if !strings.Contains(err.Error(), "Not supported") {
			t.Logf("Error: %v\n", err)
			t.Fail()
		return
		}
	}
}

func TestServer_GetLog(t *testing.T) {
	srv := getConnection(t)
	result, err := srv.GetLog(10000)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// check for `info` records, most likely you will see them in log
	if !bytes.Contains(result.Bytes(), []byte("info")) {
		t.Log("Got empty log, it's most likely an error")
		t.Fail()
		return
	}
}

func TestServer_Replicate(t *testing.T) {
	srv := getConnection(t)
	srv.MustGetDatabase("testing", BasicAuth{"admin", "admin"})
	result, err := srv.Replicate("testing", "testing2", Options{"create_target": true})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if result != nil && !result.Ok {
		t.Logf("Request was unsuccessfull, %#v\n", result)
		t.Fail()
		return
	}
	// todo: test continiuos replication and cancel it, related to ActiveTasks testing...
}

// todo: find a way to schedule this test to the end
//func TestRestart(t *testing.T) {
//	srv := getConnection(t)
//	err := srv.Restart()
//	if err != nil {
//		t.Logf("Error: %v\n", err)
//		t.Fail()
//	}
//}

func TestServer_Stats(t *testing.T) {
	srv := getConnection(t)
	var stats map[string]interface{}
	if err := srv.Stats([]string{"couchdb", "request_time"}, &stats); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
}

func TestServer_GetUUIDs(t *testing.T) {
	srv := getConnection(t)
	uuids, err := srv.GetUUIDs(15)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if len(uuids) != 15 {
		t.Log("UUIDs length mismatch")
		t.Fail()
		return
	}
}
