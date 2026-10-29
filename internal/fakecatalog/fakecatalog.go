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
	// after racing commits. SetAfterTransactionValidation can coordinate the race.
	WriteSkew                    bool
	IgnoreReadOnlyRequirements   bool
	LoseTransactions             bool
	RejectAppliedTransactions    bool
	UnknownAppliedTransactions   bool
	UnknownUnappliedTransactions bool
	// PartialTransactions publishes only the first updated table in request
	// order. It can be combined with unknown/rejected response modes.
	PartialTransactions bool
	// Snapshot faults apply only to add-snapshot/main-ref commits.
	IgnoreSnapshotRequirements bool
	PartialSnapshots           bool
	RejectAppliedSnapshots     bool
	UnknownAppliedSnapshots    bool
	UnknownUnappliedSnapshots  bool
	StaleSnapshotReads         bool
	// Schema faults apply only to single-table commits that add a schema.
	IgnoreSchemaRequirements bool
	RejectAppliedSchemas     bool
	UnknownAppliedSchemas    bool
	// Expiration faults apply only to remove-snapshots commits.
	// ExpireFromStaleMetadata removes the listed snapshots from the table as
	// its last GET returned it, discarding commits since that read.
	ExpireFromStaleMetadata bool
	ExpireRemovesHead       bool
	RejectAppliedExpiration bool
}

type table struct {
	uuid         string
	schemaID     int
	lastColumnID int
	schemas      []map[string]any
	props        map[string]string
	snapshots    map[int64]json.RawMessage
	current      *int64
	sequence     int64
}

// Catalog is the fake. Use New, then Server.URL as the catalog base URL.
type Catalog struct {
	Server *httptest.Server
	Bugs   Bugs

	mu              sync.Mutex
	afterValidation func(context.Context)
	nss             map[string]bool
	tables          map[string]*table
	commits         int
	nextID          int
	// reads keeps each table as its last GET returned it, by table UUID, for
	// ExpireFromStaleMetadata.
	reads map[string]*table
}

// New starts the fake catalog on a local port.
func New(bugs Bugs) *Catalog {
	c := &Catalog{Bugs: bugs, nss: map[string]bool{}, tables: map[string]*table{}, reads: map[string]*table{}}
	c.Server = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

// SetAfterTransactionValidation installs a hook that runs without mu, only for
// WriteSkew, after initial validation and before publication. It must honor
// cancellation. Safe to call after the server has started.
func (c *Catalog) SetAfterTransactionValidation(f func(context.Context)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.afterValidation = f
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
			observed := t
			if c.Bugs.StaleSnapshotReads && t.current != nil {
				observed = cloneTable(t)
				observed.current, observed.sequence, observed.snapshots = nil, 0, nil
			}
			c.reads[t.uuid] = cloneTable(t)
			writeJSON(w, 200, c.loadBody(observed))
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
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
		return
	}
	var actions struct {
		Updates []struct {
			Action string `json:"action"`
		} `json:"updates"`
	}
	json.Unmarshal(raw, &actions)
	addsSchema := false
	for _, update := range actions.Updates {
		if update.Action == "add-snapshot" || update.Action == "set-snapshot-ref" {
			c.commitSnapshot(w, raw, t)
			return
		}
		if update.Action == "remove-snapshots" {
			c.commitExpire(w, raw, t)
			return
		}
		addsSchema = addsSchema || update.Action == "add-schema"
	}
	var change tableChange
	if err := json.Unmarshal(raw, &change); err != nil {
		writeJSON(w, 400, errBody(400, "BadRequestException", err.Error()))
		return
	}
	requirements := change.Requirements
	if addsSchema && c.Bugs.IgnoreSchemaRequirements {
		// Planted bug: check identity but not the schema guard.
		requirements = nil
		for _, req := range change.Requirements {
			if req["type"] != "assert-current-schema-id" {
				requirements = append(requirements, req)
			}
		}
	}
	if code, msg := validateRequirements(t, requirements); code != 0 {
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
	switch {
	case addsSchema && c.Bugs.RejectAppliedSchemas:
		// Planted bug: apply the schema, then report a conflict.
		writeJSON(w, 409, errBody(409, "CommitFailedException", "schema changed"))
	case addsSchema && c.Bugs.UnknownAppliedSchemas:
		writeJSON(w, 503, errBody(503, "ServiceUnavailableException", "commit state unknown"))
	default:
		writeJSON(w, 200, c.loadBody(t))
	}
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
	copy.snapshots = make(map[int64]json.RawMessage, len(t.snapshots))
	for id, raw := range t.snapshots {
		copy.snapshots[id] = append(json.RawMessage(nil), raw...)
	}
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
			hook := c.afterValidation // Read under mu; the hook itself runs without it.
			c.mu.Unlock()
			defer c.mu.Lock()
			if hook != nil {
				hook(r.Context())
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
	published := 0
	for _, change := range body.Changes {
		key := strings.Join(change.Identifier.Namespace, ".") + "/" + change.Identifier.Name
		// Read-only snapshots must not overwrite another transaction's update.
		if updated[key] {
			if !c.Bugs.PartialTransactions || published == 0 {
				c.tables[key] = staged[key]
			}
			published++
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
			"format-version":       2,
			"table-uuid":           t.uuid,
			"current-schema-id":    t.schemaID,
			"last-column-id":       t.lastColumnID,
			"schemas":              t.schemas,
			"properties":           props,
			"snapshots":            snapshotList(t),
			"last-sequence-number": t.sequence,
			"current-snapshot-id":  t.current,
			"refs":                 snapshotRefs(t),
		},
	}
}
