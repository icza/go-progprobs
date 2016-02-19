/*
This is the main package of the minidb application.

It is an HTTP server that implements an in-memory key/value store where each
key/value can be locked by a single user at a time (i.e. the lock provides
mutual exclusion). Each lock is identified by a lock ID, which the user with
the lock uses to identify ownership of it.

Operations on the mini database are done via HTTP calls.

Full specification can be found here:
https://github.com/arschles/go-progprobs/blob/master/minidb.md

Implementation notes

I was told it is preferable to use the standard library, so everything here
is done using only the standard library.

*/
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
)

const (
	PathReservations = "/reservations/" // Path of the /reservations/ endpoint
	PathValues       = "/values/"       // Path of the /values/ endpoint
	Port             = 8080             // Port to listen on
)

// valueWr struct is a wrapper which holds the value and its lock
type valueWr struct {
	Value  string      // The value
	LockId string      // Lock ID
	Mux    *sync.Mutex // Lock used to maintain mutual exclusion
}

// The in-memory store realized with a map which maps from key (string)
// to *Entry which contains the value ([]byte) and also the lock
var store map[string]*valueWr

// Mutex used to synchronize access to the store
var storeMux = &sync.RWMutex{} // RWMutex which would allow efficient read-only locking for future read-only queries

// reservationsHandler is a request handler which handles the endpoint
// mapped to /reservations
func reservationsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Bad request, POST method expected!", http.StatusBadRequest)
		return
	}

	// POST /reservations/{key}

	w.Write([]byte(r.URL.Path))
}

// valuesHandler is a request handler which handles the endpoints
// mapped to /values.
func valuesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		// TODO uncomment
		//http.Error(w, "Bad request, POST or PUT method expected!", http.StatusBadRequest)
		//return
	}

	key := r.URL.Path[len(PathValues):] // Path is at least len(PathValues) long, else we would not be called
	if err := checkKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPut {
		// PUT /values/{key}
		storeMux.Lock()
		vw := store[key]
		if vw == nil {
			// Key doesn't already exist: create and lock
			vw = &valueWr{LockId: genLockId(), Mux: &sync.Mutex{}}
			store[key] = vw
		}
		vw.Mux.Lock()
		// Body is the value:
		content, err := ioutil.ReadAll(r.Body)
		if err != nil {
			// We should return an error, something like http.StatusInternalServerError (and unlock storeMux),
			// But spec says to always return 200 OK.
		}
		vw.Value = string(content)
		storeMux.Unlock()
	} else {
		// POST /values/{key}/{lock_id}?release={true, false}
	}

	w.Write([]byte(r.URL.Path))
}

// sendJsonResp sends a JSON response, marshaling the specified value.
func sendJsonResp(w http.ResponseWriter, v interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(v)
}

var (
	ErrKeyMissing = errors.New("Key is missing!")
	ErrKeyInvalid = errors.New("Key must not contain '/'!")
)

// checkKey checks the specified key and reports if it is not valid.
// For example since key
func checkKey(key string) error {
	if key == "" {
		return ErrKeyMissing
	}
	if strings.IndexByte(key, '/') >= 0 {
		return ErrKeyInvalid
	}
	return nil
}

// genLockId generates a new, unique lock id.
func genLockId() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// TODO handle this
		log.Println("")
	}
	return hex.EncodeToString(buf)
}

// main is the entry point of the application.
func main() {
	log.Printf("Starting minidb application on port %d...", Port)

	http.HandleFunc(PathReservations, reservationsHandler)
	http.HandleFunc(PathValues, valuesHandler)

	addr := fmt.Sprintf(":%d", Port)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Failed to start server: %v", err)
	}
}
