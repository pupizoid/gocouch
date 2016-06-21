package gocouch

import (
	"encoding/base64"
	"net/http"
)

// Auth is a common interface that provides
// methods
type Auth interface {
	// AddAuthHeaders add authorisation headers
	AddAuthHeaders(*http.Request)
	// UpdateAuth updates authorisation headers for
	// futher requests
	UpdateAuth(*http.Response)
}

// BasicAuth provides simple user:password authentication
type BasicAuth struct {
	Username, Password string
}

// AddAuthHeaders used to add authentication headers to the request
func (ba *BasicAuth) AddAuthHeaders(req *http.Request) {
	authString := []byte(ba.Username + ":" + ba.Password)
	header := "Basic " + base64.StdEncoding.EncodeToString(authString)
	req.Header.Add("Authorisation", header)
}

// UpdateAuth changes authentication informartion
func (ba *BasicAuth) UpdateAuth(resp *http.Response) {}
