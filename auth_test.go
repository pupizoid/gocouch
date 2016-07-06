package gocouch

import (
	"testing"
)

func TestBasicAuth_AddAuthHeaders(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("basic_auth_test", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	sec := db.GetDatabaseSecurity()
	if err := sec.AddAdmin("milk"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	//srv1 := getConnection(t)
	db, err = srv.MustGetDatabase("basic_auth_test", BasicAuth{"milk", "220162"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	_, err = db.Put("test_auth", map[string]string{})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestSession_AddAuthHeaders(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("basic_auth_test2", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	sec := db.GetDatabaseSecurity()
	if err := sec.AddAdmin("milk"); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	session, err := srv.NewSession("milk", "220162")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	db, err = srv.MustGetDatabase("basic_auth_test2", session)
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	_, err = db.Put("test_auth", map[string]string{})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestSession_Info(t *testing.T) {
	srv := getConnection(t)
	session, err := srv.NewSession("milk", "220162")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	res, err := session.Info()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if res["userCtx"].(map[string]interface{})["name"].(string) != "milk" {
		t.Log("Incorrect user")
		t.Fail()
	}
}

func TestSession_Close(t *testing.T) {
	srv := getConnection(t)
	session, err := srv.NewSession("milk", "220162")
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	if err := session.Close(); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}
