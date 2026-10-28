// Package fakecatalog is a tiny in-memory Iceberg REST catalog for tests.
//
// It implements just enough of the spec for Calve's workloads, and it can be
// told to misbehave on purpose. Checkers are trusted only after they catch
// every bug planted here (milestone M4).
package fakecatalog

import (
	"context"
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
	// WriteSkew validates both tables, then skips rechecking read-only tables
	// after racing commits. AfterTransactionValidation can coordinate the race.
	WriteSkew                    bool
	IgnoreReadOnlyRequirements   bool
	LoseTransactions             bool
	RejectAppliedTransactions    bool
	UnknownAppliedTransactions   bool
	UnknownUnappliedTransactions bool
}

type table struct {
	uuid         string
	schemaID     int
	lastColumnID int
	schemas      []map[string]any
	props        map[string]string
}

// Catalog is the fake. Use New, then Server.URL as the catalog base URL.
type Catalog struct {
	Server *httptest.Server
	Bugs   Bugs
	// Configure before sending requests. Runs without mu, only for WriteSkew,
	// after initial validation and before publication. Must honor cancellation.
	AfterTransactionValidation func(context.Context)

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
		var body struct {
			Name   string
			Schema map[string]any
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Schema == nil {
			writeJSON(w, 400, errBody(400, "BadRequestException", "missing schema"))
			return
		}
		key := strings.ReplaceAll(parts[2], "\x1f", ".") + "/" + body.Name
		if _, ok := c.tables[key]; ok {
			writeJSON(w, 409, errBody(409, "AlreadyExistsException", "table exists"))
			return
		}
		c.nextID++
		body.Schema["schema-id"] = 0
		lastID := 0
		fields, ok := body.Schema["fields"].([]any)
		if !ok {
			writeJSON(w, 400, errBody(400, "BadRequestException", "missing fields"))
			return
		}
		for _, f := range fields {
			field, _ := f.(map[string]any)
			id, ok := field["id"].(float64)
			if !ok || id < 1 || id != float64(int(id)) {
				writeJSON(w, 400, errBody(400, "BadRequestException", "invalid field ID"))
				return
			}
			if int(id) > lastID {
				lastID = int(id)
			}
		}
		c.tables[key] = &table{uuid: fmt.Sprintf("uuid-%d", c.nextID), lastColumnID: lastID, schemas: []map[string]any{body.Schema}, props: map[string]string{}}
		writeJSON(w, 200, c.loadBody(c.tables[key]))

	case r.Method == http.MethodPost && r.URL.Path == "/v1/transactions/commit":
		c.commitTransaction(w, r)

	// /v1/namespaces/{ns}/tables/{table}
	case len(parts) == 5 && parts[1] == "namespaces" && parts[3] == "tables":
		key := strings.ReplaceAll(parts[2], "\x1f", ".") + "/" + parts[4]
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

type tableChange struct {
	Identifier *struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	} `json:"identifier"`
	Requirements []map[string]any `json:"requirements"`
	Updates      []map[string]any `json:"updates"`
}

func (c *Catalog) commit(w http.ResponseWriter, r *http.Request, t *table) {
	var change tableChange
	if err := json.NewDecoder(r.Body).Decode(&change); err != nil {
		writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
		return
	}
	if code, msg := validateRequirements(t, change.Requirements); code != 0 {
		writeJSON(w, code, errBody(code, "CommitFailedException", msg))
		return
	}
	staged := cloneTable(t)
	if err := applyUpdates(staged, change.Updates); err != nil {
		writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
		return
	}
	c.commits++
	if c.Bugs.LoseEvery > 0 && c.commits%c.Bugs.LoseEvery == 0 {
		// Planted bug: say yes, do nothing.
		writeJSON(w, 200, c.loadBody(t))
		return
	}
	*t = *staged
	writeJSON(w, 200, c.loadBody(t))
}

func validateRequirements(t *table, requirements []map[string]any) (int, string) {
	for _, req := range requirements {
		switch req["type"] {
		case "assert-table-uuid":
			if req["uuid"] != t.uuid {
				return 409, "table uuid changed"
			}
		case "assert-current-schema-id":
			if id, ok := req["current-schema-id"].(float64); !ok || id != float64(t.schemaID) {
				return 409, "schema changed"
			}
		default:
			return 400, "unknown requirement"
		}
	}
	return 0, ""
}

func applyUpdates(t *table, updates []map[string]any) error {
	lastAdded := -1
	for _, u := range updates {
		switch u["action"] {
		case "set-properties":
			if kv, ok := u["updates"].(map[string]any); ok {
				for k, v := range kv {
					t.props[k] = fmt.Sprint(v)
				}
			}
		case "add-schema":
			schema, ok := u["schema"].(map[string]any)
			if !ok {
				return fmt.Errorf("missing schema")
			}
			lastAdded = len(t.schemas)
			schema["schema-id"] = lastAdded
			t.schemas = append(t.schemas, schema)
			if id, ok := u["last-column-id"].(float64); ok {
				t.lastColumnID = int(id)
			}
		case "set-current-schema":
			id, ok := u["schema-id"].(float64)
			if !ok || id != float64(int(id)) {
				return fmt.Errorf("missing schema-id")
			}
			if id == -1 {
				id = float64(lastAdded)
			}
			if id < 0 || int(id) >= len(t.schemas) {
				return fmt.Errorf("unknown schema-id")
			}
			t.schemaID = int(id)
		default:
			return fmt.Errorf("unknown update")
		}
	}
	return nil
}

func cloneTable(t *table) *table {
	copy := *t
	copy.props = make(map[string]string, len(t.props))
	for k, v := range t.props {
		copy.props[k] = v
	}
	data, _ := json.Marshal(t.schemas)
	json.Unmarshal(data, &copy.schemas)
	return &copy
}

// serve holds mu across validation, staging, and publication of every table.
func (c *Catalog) commitTransaction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Changes []tableChange `json:"table-changes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Changes) == 0 {
		writeJSON(w, 400, errBody(400, "BadRequestException", "missing table changes"))
		return
	}
	staged := map[string]*table{}
	updated := map[string]bool{}
	for _, change := range body.Changes {
		if change.Identifier == nil {
			writeJSON(w, 400, errBody(400, "BadRequestException", "missing identifier"))
			return
		}
		key := strings.Join(change.Identifier.Namespace, ".") + "/" + change.Identifier.Name
		t := c.tables[key]
		if t == nil {
			writeJSON(w, 404, errBody(404, "NoSuchTableException", "no such table"))
			return
		}
		if staged[key] != nil {
			writeJSON(w, 400, errBody(400, "BadRequestException", "duplicate table"))
			return
		}
		if code, msg := validateRequirements(t, change.Requirements); code != 0 && !(c.Bugs.IgnoreReadOnlyRequirements && len(change.Updates) == 0) {
			writeJSON(w, code, errBody(code, "CommitFailedException", msg))
			return
		}
		next := cloneTable(t)
		if err := applyUpdates(next, change.Updates); err != nil {
			writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
			return
		}
		staged[key] = next
		updated[key] = len(change.Updates) != 0
	}
	if c.Bugs.WriteSkew {
		// serve owns mu. Deliberately separate validation from publication,
		// retaining checks only for tables that this transaction updates.
		func() {
			c.mu.Unlock()
			defer c.mu.Lock()
			if c.AfterTransactionValidation != nil {
				c.AfterTransactionValidation(r.Context())
			}
		}()
		if r.Context().Err() != nil {
			return
		}
		for _, change := range body.Changes {
			if len(change.Updates) == 0 {
				continue
			}
			key := strings.Join(change.Identifier.Namespace, ".") + "/" + change.Identifier.Name
			t := c.tables[key]
			if t == nil {
				writeJSON(w, 404, errBody(404, "NoSuchTableException", "no such table"))
				return
			}
			if code, msg := validateRequirements(t, change.Requirements); code != 0 {
				writeJSON(w, code, errBody(code, "CommitFailedException", msg))
				return
			}
		}
	}
	if c.Bugs.UnknownUnappliedTransactions {
		writeJSON(w, 503, errBody(503, "CommitStateUnknownException", "response without application"))
		return
	}
	if c.Bugs.LoseTransactions {
		w.WriteHeader(204)
		return
	}
	for key, next := range staged {
		// Read-only snapshots must not overwrite another transaction's update.
		if updated[key] {
			c.tables[key] = next
		}
	}
	if c.Bugs.RejectAppliedTransactions {
		writeJSON(w, 409, errBody(409, "CommitFailedException", "rejected after application"))
		return
	}
	if c.Bugs.UnknownAppliedTransactions {
		writeJSON(w, 503, errBody(503, "CommitStateUnknownException", "applied before response lost"))
		return
	}
	w.WriteHeader(204)
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
			"last-column-id":    t.lastColumnID,
			"schemas":           t.schemas,
			"properties":        props,
			"snapshots":         []any{},
		},
	}
}
