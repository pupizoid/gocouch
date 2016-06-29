package gocouch

import (
	"bytes"
	"encoding/json"
	"errors"
)

// SecurityObject describes a common methods used by security mechanism in couchdb
type SecurityObject interface {
	UpdateAdmins(login string, delete bool) error
	UpdateMembers(login string, delete bool) error
	UpdateAdminRoles(role string, delete bool) error
	UpdateMemberRoles(role string, delete bool) error
}

// DatabaseSecurity is a default security object but referred to the database
type DatabaseSecurity struct {
	db *Database
	DefaultSecurity
}

// DefaultSecurity describes common security settings on a database
type DefaultSecurity struct {
	Admins  SecurityGroup `json:"admins"`
	Members SecurityGroup `json:"members"`
}

// SecurityGroup is a part of database security object
type SecurityGroup struct {
	Names []string `json:"names,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

// UpdateAdmins allows to add/delete admin into security object
func (bs *DefaultSecurity) UpdateAdmins(login string, delete bool) error {
	for i, name := range bs.Admins.Names {
		if name == login {
			if delete {
				bs.Admins.Names = append(bs.Admins.Names[:i], bs.Admins.Names[i+1:]...)
				return nil
			}
			return errors.New("Login already exists")
		}
	}
	bs.Admins.Names = append(bs.Admins.Names, login)
	return nil
}

// UpdateMembers allows to add/delete member role into security object
func (bs *DefaultSecurity) UpdateMembers(login string, delete bool) error {
	for i, name := range bs.Members.Names {
		if name == login {
			if delete {
				bs.Members.Names = append(bs.Members.Names[:i], bs.Members.Names[i+1:]...)
				return nil
			}
			return errors.New("Login already exists")
		}
	}
	bs.Members.Names = append(bs.Members.Names, login)
	return nil
}

// UpdateAdminRoles allows to add/delete admin role into security object
func (bs *DefaultSecurity) UpdateAdminRoles(login string, delete bool) error {
	for i, name := range bs.Admins.Roles {
		if name == login {
			if delete {
				bs.Admins.Roles = append(bs.Admins.Roles[:i], bs.Admins.Roles[i+1:]...)
				return nil
			}
			return errors.New("Role already exists")
		}
	}
	bs.Admins.Roles = append(bs.Admins.Roles, login)
	return nil
}

// UpdateMemberRoles allows to add/delete member role into security object
func (bs *DefaultSecurity) UpdateMemberRoles(login string, delete bool) error {
	for i, name := range bs.Members.Roles {
		if name == login {
			if delete {
				bs.Members.Roles = append(bs.Members.Roles[:i], bs.Members.Roles[i+1:]...)
				return nil
			}
			return errors.New("Role already exists")
		}
	}
	bs.Members.Roles = append(bs.Members.Roles, login)
	return nil
}

// GetSecurity fetches database security object
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

// SetSecurity sets database security object
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

// GetDatabaseSecurity returns security object of current database
func (db *Database) GetDatabaseSecurity() *DatabaseSecurity {
	return &DatabaseSecurity{db: db}

}

// AddAdmin adds admin to database
func (sec *DatabaseSecurity) AddAdmin(login string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateAdmins(login, false)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// DeleteAdmin deletes admin from database
func (sec *DatabaseSecurity) DeleteAdmin(login string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateAdmins(login, true)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// AddAdminRole adds admin role to database
func (sec *DatabaseSecurity) AddAdminRole(role string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateAdminRoles(role, false)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// DeleteAdminRole deletes admin role from database
func (sec *DatabaseSecurity) DeleteAdminRole(role string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateAdminRoles(role, true)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// AddMember adds member to database
func (sec *DatabaseSecurity) AddMember(login string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateMembers(login, false)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// DeleteMember deletes member from database
func (sec *DatabaseSecurity) DeleteMember(login string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateMembers(login, true)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// AddMemberRole adds membse role to database
func (sec *DatabaseSecurity) AddMemberRole(role string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateMemberRoles(role, false)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}

// DeleteMemberRole deletes member role to database
func (sec *DatabaseSecurity) DeleteMemberRole(role string) error {
	if err := sec.db.GetSecurity(sec); err != nil {
		return err
	}
	sec.UpdateMemberRoles(role, true)
	if err := sec.db.SetSecurity(sec); err != nil {
		return err
	}
	return nil
}
