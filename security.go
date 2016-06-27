package gocouch

import (
	"errors"
	"encoding/json"
	"bytes"
)

type SecurityObject interface {
	UpdateAdmins(login string, delete bool) error
	UpdateMembers(login string, delete bool) error
	UpdateAdminRoles(role string, delete bool) error
	UpdateMemberRoles(role string, delete bool) error
}

type BaseSecurity struct {
	Admins  SecurityGroup `json:"admins"`
	Members SecurityGroup `json:"members"`
}

type SecurityGroup struct {
	Names []string `json:"names,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

func (bs *BaseSecurity) UpdateAdmins(login string, delete bool) error {
	for i, name := range bs.Admins.Names {
		if name == login {
			if delete {
				bs.Admins.Names = append(bs.Admins.Names[:i], bs.Admins.Names[i + 1:]...)
				return nil
			}
			return errors.New("Login already exists")
		}
	}
	bs.Admins.Names = append(bs.Admins.Names, login)
	return nil
}

func (bs *BaseSecurity) UpdateMembers(login string, delete bool) error {
	for i, name := range bs.Members.Names {
		if name == login {
			if delete {
				bs.Members.Names = append(bs.Members.Names[:i], bs.Members.Names[i + 1:]...)
				return nil
			}
			return errors.New("Login already exists")
		}
	}
	bs.Members.Names = append(bs.Members.Names, login)
	return nil
}

func (bs *BaseSecurity) UpdateAdminRoles(login string, delete bool) error {
	for i, name := range bs.Admins.Roles {
		if name == login {
			if delete {
				bs.Admins.Roles = append(bs.Admins.Roles[:i], bs.Admins.Roles[i + 1:]...)
				return nil
			}
			return errors.New("Role already exists")
		}
	}
	bs.Admins.Roles = append(bs.Admins.Roles, login)
	return nil
}

func (bs *BaseSecurity) UpdateMemberRoles(login string, delete bool) error {
	for i, name := range bs.Members.Roles {
		if name == login {
			if delete {
				bs.Members.Roles = append(bs.Members.Roles[:i], bs.Members.Roles[i + 1:]...)
				return nil
			}
			return errors.New("Role already exists")
		}
	}
	bs.Members.Roles = append(bs.Members.Roles, login)
	return nil
}

func (db *Database) GetSecurity(o SecurityObject) error {
	resp, err := db.conn.request("GET", queryURL(db.Name, "_security"), nil, nil, db.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, o); err != nil {
		return err
	}
	return nil
}

func (db *Database) SetSecurity(o SecurityObject) error {
	headers := map[string]string{"Content-Type": "application/json"}
	payload, err := json.Marshal(o)
	if err != nil {
		return err
	}
	resp, err := db.conn.request("PUT", queryURL(db.Name, "_security"), headers, bytes.NewReader(payload), db.auth, 0)
	if err != nil {
		return err
	}
	var result map[string]bool
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	return nil
}