package gocouch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

// Server represents couchdb instance and holds connection to it
type Server struct {
	auth Auth
	conn *connection
}

// ServerInfo provides couchdb instance inforation
type ServerInfo struct {
	Message string      `json:"couchdb"`
	UUID    string      `json:"uuid"`
	Vendor  interface{} `json:"vendor"`
	Version string      `json:"version"`
}

// ServerEvent represents couchdb instance information about databases
type ServerEvent struct {
	Name string `json:"db_name"`
	Ok   bool   `json:"ok"`
	Type string `json:"type"`
}

// ReplicationResult provides information about replication request
type ReplicationResult struct {
	History        []map[string]interface{} `json:"history"`
	Ok             bool                     `json:"ok"`
	ReplicationVer int                      `json:"replication_id_version"`
	SessionID      string                   `json:"session_id"`
	SourceLastSeq  int                      `json:"source_last_seq"`
}

// Connect tries to connect to the couchdb server using given host, port and optionally
// Auth and default request timeout
func Connect(host string, port int, auth Auth, timeout time.Duration) (*Server, error) {
	conn, err := createConnection(fmt.Sprintf("http://%s:%d", host, port), timeout)
	if err != nil {
		return nil, err
	}
	return &Server{auth: auth, conn: conn}, nil
}

// Copy returns a copy of server connection with same settings and auth information
// but with it's own http client instance
// todo: move this method to private!?
func (srv *Server) Copy() (*Server, error) {
	conn, err := createConnection(srv.conn.url, srv.conn.client.Timeout)
	if err != nil {
		return nil, err
	}
	return &Server{auth: srv.auth, conn: conn}, nil
}

// Info provides server information, also may be used to check server status
func (srv *Server) Info() (*ServerInfo, error) {
	resp, err := srv.conn.request("GET", "/", nil, nil, srv.auth, 0)
	if err != nil {
		return nil, err
	}
	var out ServerInfo
	if err := parseBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetActiveTasks returns slice of maps describing tasks running on server
func (srv *Server) GetActiveTasks(o interface{}) error {
	resp, err := srv.conn.request("GET", "/_active_tasks", nil, nil, srv.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, o); err != nil {
		return err
	}
	return nil
}

// GetAllDbs returns a list of databases present at the server
func (srv *Server) GetAllDBs() (dbList []string, err error) {
	resp, err := srv.conn.request("GET", "/_all_dbs", nil, nil, srv.auth, 0)
	if err != nil {
		return
	}
	if err := parseBody(resp, &dbList); err != nil {
		return nil, err
	}
	return
}

// GetDBEvent block execution of program until any db event on couchdb instance
// happens and then unmarshall this event into given variable.
// Query parameters are equal to official docs:
// 	http://docs.couchdb.org/en/1.6.1/api/server/common.html#db-updates
// Note: `timeout` option accepts milliseconds instead of seconds
func (srv *Server) GetDBEvent(o interface{}, options Options) error {
	url := ""
	for k, v := range options {
		url += fmt.Sprintf("&%s=%v", k, v)
	}
	if len(url) > 0 {
		url = strings.Trim(url, "&")
		url = "/_db_updates?" + url
	} else {
		url = "/_db_updates"
	}
	resp, err := srv.conn.request("GET", url, nil, nil, srv.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, o); err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil
		}
	}
	return err
}

// GetDBEventChan returns channel that provides events happened on couchdb instance,
// it's thread safe to use in other goroutines. Also, you must close channel after all things
// done to release resourses and prevent memory leaks.
func (srv *Server) GetDBEventChan() (c chan ServerEvent, err error) {
	c = make(chan ServerEvent)
	cpSrv, err := srv.Copy()
	if err != nil {
		return nil, err
	}
	resp, err := cpSrv.conn.request("GET", "/_db_updates?feed=continuous", nil, nil, srv.auth, 0)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// channel closed externally, close the connection
				resp.Body.Close()
				if err != nil {
					c = nil
					switch r.(type) {
					case error:
						err = r.(error)
					}
				}
			}
		}()
		reader := bufio.NewReader(resp.Body)
		for {
			line, intErr := reader.ReadBytes('\n')
			if intErr != nil {
				panic("Failed to read bytes from connection")
			}
			var payload ServerEvent
			intErr = json.Unmarshal(line, &payload)
			if intErr != nil {
				panic("Failed to unmarshal bytes to ServerEvent")
			}
			c <- payload
		}
	}()
	return c, err
}

// GetMembership returns lists of cluster and all nodes
func (srv *Server) GetMembership(o interface{}) error {
	resp, err := srv.conn.request("GET", "/_membership", nil, nil, srv.auth, 0)
	if err != nil {
		switch err.(type) {
		case *Error:
			if err.(*Error).StatusCode == 400 {
				return errors.New("Not supported by server")
			}
		default:
			return err
		}
	}
	return parseBody(resp, &o)
}

// GetLog returns you a buffer with given size, containing chunk from
// couchdb instance log file
func (srv *Server) GetLog(size int) (*bytes.Buffer, error) {
	var URL string
	if size > 0 {
		URL = fmt.Sprintf("/_log?bytes=%d", size)
	} else {
		URL = "/_log"
	}
	resp, err := srv.conn.request("GET", URL, nil, nil, srv.auth, 0)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}
	payload, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(payload), nil
}

// Replicate provides replication management
func (srv *Server) Replicate(source, target string, options Options) (*ReplicationResult, error) {
	request := make(map[string]interface{})
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	request["source"] = source
	request["target"] = target
	for k, v := range options {
		request[k] = v
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	resp, err := srv.conn.request("POST", "/_replicate", headers, bytes.NewReader(payload), srv.auth, 0)
	if err != nil {
		return nil, err
	}
	var result ReplicationResult
	if err := parseBody(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Restart restarts couchdb instance, admin privileges may be required
func (srv *Server) Restart() error {
	var result map[string]bool
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	resp, err := srv.conn.request("POST", "/_restart", headers, nil, srv.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, &result); err != nil {
		return err
	}
	if !result["ok"] {
		return errors.New("Restart failed")
	}
	return nil
}

// Stats provides couchdb usage statistics statistics
func (srv *Server) Stats(path []string, o interface{}) error {
	resp, err := srv.conn.request("GET", "/_stats/"+strings.Join(path, "/"), nil, nil, srv.auth, 0)
	if err != nil {
		return err
	}
	if err := parseBody(resp, &o); err != nil {
		return err
	}
	return nil
}

// GetUUIDs returns slice of uuids generated by couchdb instance
func (srv *Server) GetUUIDs(count int) ([]string, error) {
	var result map[string][]string
	if count < 1 {
		return nil, errors.New("Count must be greater than zero")
	}
	resp, err := srv.conn.request("GET", fmt.Sprintf("/_uuids?count=%d", count), nil, nil, srv.auth, 0)
	if err != nil {
		return nil, err
	}
	if err := parseBody(resp, &result); err != nil {
		return nil, err
	}
	return result["uuids"], nil
}
