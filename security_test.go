package gocouch

import (
	"strings"
	"testing"
)

func TestDatabase_GetSecurity(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("security", BasicAuth{"admin", "admin"})
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	var sec DefaultSecurity
	if err := db.GetSecurity(&sec); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestDatabase_SetSecurity(t *testing.T) {
	srv := getConnection(t)
	db, err := srv.MustGetDatabase("security_2", BasicAuth{"admin", "admin"})
	//defer db.Delete()
	if err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	defer db.Delete()
	// test setting of custom security object
	type CustomSecurity struct {
		DefaultSecurity
		CustomField string `json:"custom_field"`
	}
	new := new(CustomSecurity)
	new.CustomField = "some_field_value"
	new.UpdateAdmins("admin", false)
	if err := db.SetSecurity(new); err != nil {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}

	var sec CustomSecurity
	if err := db.GetSecurity(&sec); err != nil && sec.CustomField == "some_field_value" {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
	// todo: test 401 code...
}

func TestBaseSecurity_UpdateAdminRoles(t *testing.T) {
	var sec DefaultSecurity
	if err := sec.UpdateAdminRoles("sudo", false); err != nil || sec.Admins.Roles[0] != "sudo" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test duplicate
	if err := sec.UpdateAdminRoles("sudo", false); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test delete
	if err := sec.UpdateAdminRoles("sudo", true); err != nil || len(sec.Admins.Roles) != 0 {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestBaseSecurity_UpdateMemberRoles(t *testing.T) {
	var sec DefaultSecurity
	if err := sec.UpdateMemberRoles("dev", false); err != nil || sec.Members.Roles[0] != "dev" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test duplicate
	if err := sec.UpdateMemberRoles("dev", false); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test delete
	if err := sec.UpdateMemberRoles("dev", true); err != nil || len(sec.Members.Roles) != 0 {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestBaseSecurity_UpdateAdmins(t *testing.T) {
	var sec DefaultSecurity
	if err := sec.UpdateAdmins("sudo", false); err != nil || sec.Admins.Names[0] != "sudo" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test duplicate
	if err := sec.UpdateAdmins("sudo", false); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test delete
	if err := sec.UpdateAdmins("sudo", true); err != nil || len(sec.Admins.Names) != 0 {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}

func TestBaseSecurity_UpdateMembers(t *testing.T) {
	var sec DefaultSecurity
	if err := sec.UpdateMembers("dev", false); err != nil || sec.Members.Names[0] != "dev" {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test duplicate
	if err := sec.UpdateMembers("dev", false); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Logf("Error: %v\n", err)
		t.Fail()
		return
	}
	// test delete
	if err := sec.UpdateMembers("dev", true); err != nil || len(sec.Members.Names) != 0 {
		t.Logf("Error: %v\n", err)
		t.Fail()
	}
}
