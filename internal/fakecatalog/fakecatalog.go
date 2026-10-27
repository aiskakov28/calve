// Package fakecatalog is a tiny in-memory Iceberg REST catalog for tests.
//
// It implements just enough of the spec for Calve's workloads, and it can be
// told to misbehave on purpose. Checkers are trusted only after they catch
// every bug planted here (milestone M4).
package fakecatalog

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// Bugs are faults the fake catalog can plant.
type Bugs struct {
	// LoseEvery acknowledges every Nth table commit with 200 but never
	// applies it. 0 disables the bug.
	LoseEvery int
}

type table struct {
	uuid     string
	schemaID int
	props    map[string]string
}

// Catalog is the fake. Use New, then Server.URL as the catalog base URL.
type Catalog struct {
	Server *httptest.Server
	Bugs   Bugs

	mu      sync.Mutex
	nss     map[string]bool
	tables  map[string]*table
	commits int
	nextID  int
}

// New starts the fake catalog on a local port.
func New(bugs Bugs) *Catalog {
	c := &Catalog{Bugs: bugs, nss: map[string]bool{}, tables: map[string]*table{}}
	c.Server = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

// Close stops the server.
func (c *Catalog) Close() { c.Server.Close() }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func errBody(code int, typ, msg string) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "type": typ, "message": msg}}
}

func (c *Catalog) serve(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/config":
		writeJSON(w, 200, map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})

	case r.Method == http.MethodPost && r.URL.Path == "/v1/namespaces":
		var body struct{ Namespace []string }
		json.NewDecoder(r.Body).Decode(&body)
		name := strings.Join(body.Namespace, ".")
		if c.nss[name] {
			writeJSON(w, 409, errBody(409, "AlreadyExistsException", "namespace exists"))
			return
		}
		c.nss[name] = true
		writeJSON(w, 200, map[string]any{"namespace": body.Namespace})

	// /v1/namespaces/{ns}/tables
	case len(parts) == 4 && parts[1] == "namespaces" && parts[3] == "tables" && r.Method == http.MethodPost:
		var body struct{ Name string }
		json.NewDecoder(r.Body).Decode(&body)
		key := parts[2] + "/" + body.Name
		if _, ok := c.tables[key]; ok {
			writeJSON(w, 409, errBody(409, "AlreadyExistsException", "table exists"))
			return
		}
		c.nextID++
		c.tables[key] = &table{uuid: fmt.Sprintf("uuid-%d", c.nextID), props: map[string]string{}}
		writeJSON(w, 200, c.loadBody(c.tables[key]))

	// /v1/namespaces/{ns}/tables/{table}
	case len(parts) == 5 && parts[1] == "namespaces" && parts[3] == "tables":
		key := parts[2] + "/" + parts[4]
		t, ok := c.tables[key]
		if !ok {
			writeJSON(w, 404, errBody(404, "NoSuchTableException", "no such table"))
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, 200, c.loadBody(t))
		case http.MethodDelete:
			delete(c.tables, key)
			w.WriteHeader(204)
		case http.MethodPost:
			c.commit(w, r, t)
		default:
			w.WriteHeader(405)
		}

	default:
		writeJSON(w, 404, errBody(404, "NotFound", "route not implemented by fake catalog"))
	}
}

func (c *Catalog) commit(w http.ResponseWriter, r *http.Request, t *table) {
	var change struct {
		Requirements []map[string]any `json:"requirements"`
		Updates      []map[string]any `json:"updates"`
	}
	if err := json.NewDecoder(r.Body).Decode(&change); err != nil {
		writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
		return
	}
	for _, req := range change.Requirements {
		switch req["type"] {
		case "assert-table-uuid":
			if req["uuid"] != t.uuid {
				writeJSON(w, 409, errBody(409, "CommitFailedException", "table uuid changed"))
				return
			}
		case "assert-current-schema-id":
			if id, _ := req["current-schema-id"].(float64); int(id) != t.schemaID {
				writeJSON(w, 409, errBody(409, "CommitFailedException", "schema changed"))
				return
			}
		}
	}

	c.commits++
	if c.Bugs.LoseEvery > 0 && c.commits%c.Bugs.LoseEvery == 0 {
		// Planted bug: say yes, do nothing.
		writeJSON(w, 200, c.loadBody(t))
		return
	}

	for _, u := range change.Updates {
		if u["action"] == "set-properties" {
			if kv, ok := u["updates"].(map[string]any); ok {
				for k, v := range kv {
					t.props[k] = fmt.Sprint(v)
				}
			}
		}
	}
	writeJSON(w, 200, c.loadBody(t))
}

func (c *Catalog) loadBody(t *table) map[string]any {
	props := map[string]string{}
	for k, v := range t.props {
		props[k] = v
	}
	return map[string]any{
		"metadata-location": "memory://" + t.uuid,
		"metadata": map[string]any{
			"format-version":    2,
			"table-uuid":        t.uuid,
			"current-schema-id": t.schemaID,
			"properties":        props,
			"snapshots":         []any{},
		},
	}
}
