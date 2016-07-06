// Package gocouch implements wrappers for the 95% of CouchDB HTTP API
package gocouch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type (
	// base connection used to request information from the database
	connection struct {
		url    string
		client *http.Client
	}

	couchError struct {
		Err, Reason string
	}

	// Options allow to specify request parameters
	Options map[string]interface{}

	// Error is a general struct containing information about failed request
	Error struct {
		StatusCode int
		URL        string
		Method     string
		ErrorCode  string
		Reason     string
	}
)

func createConnection(dest string, timeout time.Duration) (*connection, error) {
	validatedURL, err := url.Parse(dest)
	if err != nil {
		return nil, err
	}
	return &connection{validatedURL.String(), &http.Client{Timeout: timeout}}, nil
}

func (conn *connection) request(method, path string,
	headers map[string]string, body io.Reader, auth Auth, timeout time.Duration) (*http.Response, error) {

	req, err := http.NewRequest(method, conn.url+path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if err != nil {
		return nil, err
	}
	if auth != nil {
		auth.AddAuthHeaders(req)
	}
	return conn.processResponse(req)
}

func (conn *connection) processResponse(req *http.Request) (*http.Response, error) {
	resp, err := conn.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return resp, parseError(resp)
	}
	return resp, nil
}

//stringify the error
func (err Error) Error() string {
	return fmt.Sprintf("[Error]:%v: %v %v - %v %v",
		err.StatusCode, err.Method, err.URL, err.ErrorCode, err.Reason)
}

//Parse a CouchDB error response
func parseError(resp *http.Response) error {
	var couchReply couchError
	if resp.Request.Method != "HEAD" {
		err := parseBody(resp, &couchReply)
		if err != nil {
			return fmt.Errorf("Unknown error accessing CouchDB: %v", err)
		}
	}
	return &Error{
		StatusCode: resp.StatusCode,
		URL:        resp.Request.URL.String(),
		Method:     resp.Request.Method,
		ErrorCode:  couchReply.Err,
		Reason:     couchReply.Reason,
	}
}

//unmarshalls a JSON Response Body
func parseBody(resp *http.Response, o interface{}) error {
	err := json.NewDecoder(resp.Body).Decode(o)
	if err != nil {
		resp.Body.Close()
		return err
	}
	return resp.Body.Close()
}

// encodes a struct to JSON and returns an io.Reader,
// the buffer size, and an error (if any)
func encodeData(o interface{}) (io.Reader, int, error) {
	if o == nil {
		return nil, 0, nil
	}
	buf, err := json.Marshal(&o)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(buf), len(buf), nil
}
