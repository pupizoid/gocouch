package gocouch

import (
	"bytes"
	"strings"
	"testing"
)

func getConnection(t *testing.T) *Server {
	srv, err := Connect("localhost", 5984, nil, 0)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	return srv
}

func TestInfo(t *testing.T) {
	srv := getConnection(t)
	info, err := srv.Info()
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	// check struct parse
	if info.Message == "" || info.UUID == "" || info.Version == "" {
		t.Logf("Incorrect struct returned: %#v", info)
		t.Fail()
	}
}

func TestActiveTasks(t *testing.T) {
	srv := getConnection(t)
	var result []map[string]interface{}
	err := srv.GetActiveTasks(&result)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	// todo: add checking authorisation and some task appearance (continious replication)
}

func TestGetAllDbs(t *testing.T) {
	srv := getConnection(t)
	var result []string
	err := srv.GetAllDbs(&result)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(result) < 1 {
		t.Log("Len of db names less than 1")
		t.Fail()
	}
}

func TestGetDBEvent(t *testing.T) {
	srv := getConnection(t)
	var result map[string]interface{}
	if err := srv.GetDBEvent(&result, Options{"timeout": "1"}); err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	// todo: add creating and deleting db tests
}

func TestGetDBEventChan(t *testing.T) {
	srv := getConnection(t)
	events, err := srv.GetDBEventChan(10)
	defer func() {
		close(events)
	}()
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	// todo: check blocking
	// todo: add tests with real events
	// todo: check resourses are released
	// todo: test error reporting
}

func TestGetMembership(t *testing.T) {
	srv := getConnection(t)
	var result map[string][]string
	if err := srv.GetMembership(&result); err != nil {
		// membership only supported by couchdb 2.0
		if !strings.Contains(err.Error(), "Not supported") {
			t.Logf("Error: %v", err)
			t.Fail()
		}
	}
}

func TestGetLog(t *testing.T) {
	srv := getConnection(t)
	result, err := srv.GetLog(10000)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	// check for `info` records, most likely you will see them in log
	if !bytes.Contains(result.Bytes(), []byte("info")) {
		t.Log("Got empty log, it's most likely an error")
		t.Fail()
	}
}

func TestReplicate(t *testing.T) {
	srv := getConnection(t)
	result, err := srv.Replicate("testing", "testing2", Options{"create_target": true})
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if !result.Ok {
		t.Logf("Request was unsuccessfull, %#v\n", result)
	}
}

// todo: find a way to schedule this test to the end
//func TestRestart(t *testing.T) {
//	srv := getConnection(t)
//	err := srv.Restart()
//	if err != nil {
//		t.Logf("Error: %v", err)
//		t.Fail()
//	}
//}

func TestStats(t *testing.T) {
	srv := getConnection(t)
	var stats map[string]interface{}
	if err := srv.Stats([]string{"couchdb", "request_time"}, &stats); err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
}

func TestUUIDs(t *testing.T) {
	srv := getConnection(t)
	uuids, err := srv.GetUUIDs(15)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}
	if len(uuids) != 15 {
		t.Log("UUIDs length mismatch")
		t.Fail()
	}
}
