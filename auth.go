package gocouch

import (
	"encoding/base64"
	"net/http"
	"fmt"
	"encoding/json"
	"bytes"
)

// Auth is a common interface that provides
// methods
type Auth interface {
	// AddAuthHeaders add authorisation headers
	AddAuthHeaders(*http.Request)
}

// BasicAuth provides simple user:password authentication
type BasicAuth struct {
	Username, Password string
}

// AddAuthHeaders used to add authentication headers to the request
func (ba BasicAuth) AddAuthHeaders(req *http.Request) {
	authString := []byte(ba.Username + ":" + ba.Password)
	header := "Basic " + base64.StdEncoding.EncodeToString(authString)
	req.Header.Add("Authorization", header)
}

// Session stores authentication cookie for current user at the CouchDB instance
type Session struct {
	cookie *http.Cookie
	srv    *Server
}

// AddAuthHeaders add cookie to request
func (s Session) AddAuthHeaders(req *http.Request) {
	req.AddCookie(s.cookie)
}

// UserRecord is userd to create new user in couchdb instance
type UserRecord struct {
	Login    string   `json:"name"`
	Type     string   `json:"type"`
	Roles    []string `json:"roles"`
	Password string   `json:"password"`
}

// CreateUser inserts user record to couchdb _users database
func (srv *Server) CreateUser(user *UserRecord) error {
	db, err := srv.MustGetDatabase("_users", srv.auth)
	if err != nil {
		return err
	}
	_, err = db.Put(fmt.Sprintf("org.couchdb.user:%s", user.Login), user)
	return err
}

// NewSession authenticates user and returns Session struct containing current
// session token
func (srv *Server) NewSession(user, pass string) (*Session, error) {
	request := map[string]string{"name": user, "password": pass}
	headers := map[string]string{"Content-Type": appJSON}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	resp, err := srv.conn.request("POST", "/_session", headers, bytes.NewReader(payload), srv.auth, 0)
	if err != nil {
		return nil, err
	}
	s := Session{srv: srv}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "AuthSession" {
			s.cookie = cookie
		}
	}
	return &s, nil
}

// Info returns information about current session info
func (s *Session) Info() (map[string]interface{}, error) {
	resp, err := s.srv.conn.request("GET", "/_session", nil, nil, s, 0)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := parseBody(resp, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Close deletes current session
func (s *Session) Close() error {
	_, err := s.srv.conn.request("DELETE", "/_session", nil, nil, s, 0)
	return err
}