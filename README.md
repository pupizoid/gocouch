[![Build Status](https://travis-ci.org/pupizoid/gocouch.svg?branch=master)](https://travis-ci.org/pupizoid/gocouch)
[![Go Report Card](https://goreportcard.com/badge/github.com/pupizoid/gocouch)](https://goreportcard.com/report/github.com/pupizoid/gocouch)
[![GoDoc](https://godoc.org/github.com/pupizoid/gocouch?status.svg)](https://godoc.org/github.com/pupizoid/gocouch)

###Connecting to server:

```go
defaultTimeout := time.Second * 10
conn, err := gocouch.Connect("127.0.0.1", 5984, nil, defaultTimeout)

// With credentials
auth := gocouch.BasicAuth{"admin", "pass"}
conn, err := gocouch.Connect("127.0.0.1", 5984, auth, defaultTimeout)
```

###Database operations:
Get existing database or create new one: 
```go
db, err := conn.MustGetDatabase("some_db", nil)
```
Non-nil `Auth` passed to this method has higher priority, so it will owerride previously used on `Connect`. By default this method use `Auth` object provided when connection was created.

Get all existing databases:
```go
list, err := conn.GetAllDBs()
```

Fetch new database related action on CouchDB instance: 
```go
var events map[string]interface{}
err := conn.GetDBEvent(&events, nil)
```
This request will block workflow until any event happens or default timeout (60 sec)  will be exceeded. If you try to specify custom operation timeout note that it accepts milliseconds. Also it's safe to use in goroutine.

If you want to fetch many events, you can use `GetDBEventChan`:
```go
eventChan, err := conn.GetDBEventChan()
// don't forget to close channel to release connection resourses
defer close(eventChan)
```

###Basic CRUD actions with a single document:
```go
package main

import (
	"github.com/pupizoid/gocouch"
	"time"
)

func main() {
	server, _ := gocouch.Connect("127.0.0.1", 5984, nil, time.Second * 10)

	type Doc struct {
		Field string `json:"field"`
		// Note: struct data will override `id` param in database methods,
		// to avoid conflicts struct fields representing document's
		// `_id` & `_rev` should always contain json tag `omitempty`
		Rev   string `json:"_rev,omitempty"`
		ID    string `json:"_id,omitempty"`
	}

	var document1, document2 Doc

	db, _ := server.MustGetDatabase("test_db", nil)

	document1.Field = "value"
  
    // Create document with specified id (also accepts map as a document)
	db.Put("test_id", &document1)
	// Get document by id
	db.Get("test_id", &document2, nil)
	// Delete document's revision
	db.Del(document2.ID, document2.Rev)

}
```

###Bulk operations:

```go

```

##TODO:
- [x] Server API
- [x] Database API
- [x] Document API
- [x] Authentication
- [ ] Design Docs & Views
- [ ] Attachments
- [ ] Coverage > 90%
