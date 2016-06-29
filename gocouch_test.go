package gocouch

import (
	"net/http"
	"testing"
)

var serverURL = "http://localhost:5984"

func TestConnection(t *testing.T) {
	client := &http.Client{}
	c := connection{
		url:    serverURL,
		client: client,
	}
	resp, err := c.request("GET", "/", nil, nil, nil, 0)
	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	} else if resp == nil {
		t.Fail()
	}
}
