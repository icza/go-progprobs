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
	LockIdLength     = 16               // Length of lock ids (in bytes, will be double when encoded to hex)
)

// valueWr struct is a wrapper which holds the value and its lock
type valueWr struct {
	Value  string      // The value
	LockId string      // Lock ID
	Mux    *sync.Mutex // Lock used to maintain mutual exclusion
}

// Lock waits for the value to be available and acquires the lock,
// and generates a new lock id.
// Should only be called from a handler (because it unlocks store mutex while waiting).
func (vw *valueWr) Lock() {
	// While we wait, we have to release the store mutex
	// else noone else would be able to release the value we're waiting for:
	storeMux.Unlock()
	defer storeMux.Lock()

	vw.Mux.Lock()
	vw.LockId = genLockId()
}

// Unlock releases the lock for the value and invalidates previous lock id.
func (vw *valueWr) Unlock() {
	vw.LockId = ""
	vw.Mux.Unlock()
}

// SendJSONResp sends a JSON response inlcuding the LockId, and optionally the Value.
func (vw *valueWr) SendJSONResp(w http.ResponseWriter, sendValue bool) error {
	w.Header().Set("Content-Type", "application/json")
	m := map[string]string{"lock_id": vw.LockId}
	if sendValue {
		m["value"] = vw.Value
	}
	return json.NewEncoder(w).Encode(m)
}

// The in-memory store realized with a map which maps from key (string)
// to *valueWr which contains the value (string) and also its the lock.
var store = make(map[string]*valueWr)

// Mutex used to synchronize access to the store.
var storeMux = &sync.RWMutex{} // RWMutex which would allow efficient read-only locking for future read-only queries

// reservationsHandler is a request handler which handles the endpoint
// mapped to /reservations/.
func reservationsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Bad request, POST method expected!", http.StatusBadRequest)
		return
	}

	// POST /reservations/{key}
	key := r.URL.Path[len(PathReservations):] // Path length is at least len(PathReservations) else we wouldn't be here

	storeMux.Lock()
	defer storeMux.Unlock()

	vw := store[key]
	if vw == nil {
		http.NotFound(w, r)
		return
	}

	// Wait to be available and acquire lock:
	vw.Lock()
	vw.SendJSONResp(w, true)
}

// valuesHandler is a request handler which handles the endpoints
// mapped to /values/.
func valuesHandler(w http.ResponseWriter, r *http.Request) {
	// 0: empty, 1: "values", 2: key, 3: lockId
	parts := strings.Split(r.URL.Path, "/")
	// We expect key in all cases
	key := parts[2] // If there is no key, this will be empty string
	if err := checkKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	storeMux.Lock()
	defer storeMux.Unlock()

	switch r.Method {
	case http.MethodPost:
		// POST /values/{key}/{lock_id}?release={true, false}
		release := r.URL.Query().Get("release")
		// According to spec, if release is neither "true" nor "false", nothing should be set
		if len(parts) < 4 || (release != "false" && release != "true") {
			http.Error(w, "Bad request, missing lockId and/or release parameter (must be 'true' or 'false')!", http.StatusBadRequest)
			return
		}
		vw := store[key]
		if vw == nil {
			http.NotFound(w, r)
			return
		}
		if vw.LockId != parts[3] {
			http.Error(w, "401 Unauthorized", http.StatusUnauthorized)
			return
		}
		readBody(vw, r) // We ignore returned error
		if release == "true" {
			vw.Unlock()
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPut:
		// PUT /values/{key}
		vw := store[key]
		if vw == nil {
			// Key doesn't exist yet: create
			vw = &valueWr{Mux: &sync.Mutex{}}
			store[key] = vw
		}
		// Acquire lock
		vw.Lock()
		readBody(vw, r) // Spec says to always return 200, so we ignore returned error
		vw.SendJSONResp(w, false)
	default:
		http.Error(w, "Bad request, POST or PUT method expected!", http.StatusBadRequest)
	}
}

// readBody reads the request body and sets it as the new value.
func readBody(vw *valueWr, r *http.Request) error {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error reading request body:", err)
		return err
	}
	vw.Value = string(content)
	return nil
}

var (
	ErrKeyMissing = errors.New("Key is missing!")
	ErrKeyInvalid = errors.New("Key must not contain '/'!")
)

// checkKey checks the specified key and reports if it is not valid.
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
	buf := make([]byte, LockIdLength)
	if _, err := rand.Read(buf); err != nil {
		log.Println("Error reading secure random:", err)
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
		log.Println("Failed to start server:", err)
	}
}
