//go:build ignore

// Race two guarded schema commits on one table and record every response.
//
// Both writers require the same table UUID and current schema ID, and each adds
// the next schema ID with its own column. The winner invalidates the loser's
// requirement, so the loser should get 409. Lakekeeper v0.14.2 instead inserts
// the loser's schema row before its compare-and-set, hits a duplicate key and
// answers 503. A sequential control first shows the catalog answers a stale
// commit with 409. Each request is sent once, without redirects; after every
// round the table is loaded to check that only the acknowledged column exists.
//
//	go run scripts/repro-lakekeeper-schema-cas.go -out results/lakekeeper-schema-repro
//
// Exit status: 3 reproduced, 0 not reproduced, 1 on any other failure.
// Standard library only; see docs/triage-lakekeeper-schema-cas.md.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// record is one line of http.jsonl: exactly what was sent and received.
type record struct {
	Label        string      `json:"label"`
	Method       string      `json:"method"`
	URL          string      `json:"url"`
	RequestBody  *string     `json:"request_body"`
	StartNS      int64       `json:"started_ns"`
	EndNS        int64       `json:"ended_ns"`
	Status       int         `json:"status,omitempty"`
	Headers      http.Header `json:"headers,omitempty"`
	ResponseBody string      `json:"response_body,omitempty"`
	Error        string      `json:"error,omitempty"`
}

type observation struct {
	TableUUID      string   `json:"table_uuid"`
	Statuses       []int    `json:"statuses"`
	VisibleColumns []string `json:"visible_columns"`
}

type summary struct {
	Catalog         string         `json:"catalog"`
	Warehouse       string         `json:"warehouse"`
	Control         *observation   `json:"control"`
	Rounds          []observation  `json:"rounds"`
	DuplicateKey503 int            `json:"duplicate_key_503"`
	Reproduced      bool           `json:"reproduced"`
	StatusPairs     map[string]int `json:"status_pairs"`
	Note            string         `json:"note"`
}

// contender is one writer's column and the response to its commit.
type contender struct {
	column string
	resp   record
}

var (
	catalog   = flag.String("catalog", "http://localhost:8182/catalog", "catalog root, without /v1")
	warehouse = flag.String("warehouse", "calve", "warehouse passed to /v1/config")
	namespace = flag.String("namespace", "calve_schema_cas_repro", "namespace for the test tables, dot-separated")
	rounds    = flag.Int("rounds", 10, "races, each on a fresh table")
	out       = flag.String("out", "", "new directory for http.jsonl and summary.json (required)")
	token     = os.Getenv("CATALOG_TOKEN") // optional bearer token; never written to the evidence
	client    = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	start     = time.Now()

	evidenceMu sync.Mutex
	evidence   *os.File
)

func call(label, method, path string, body any) (record, error) {
	root := strings.TrimRight(*catalog, "/")
	r := record{Label: label, Method: method, URL: root + path, StartNS: time.Since(start).Nanoseconds()}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return r, fmt.Errorf("%s: encode request: %w", label, err)
		}
		s := string(encoded)
		r.RequestBody = &s
		reader = bytes.NewReader(encoded)
	}
	err := func() error {
		req, err := http.NewRequest(method, root+path, reader)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		r.Status, r.Headers = resp.StatusCode, resp.Header
		data, err := io.ReadAll(resp.Body)
		r.ResponseBody = string(data)
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}
		return nil
	}()
	r.EndNS = time.Since(start).Nanoseconds()
	if err != nil {
		r.Error = err.Error()
	}
	line, merr := json.Marshal(r)
	if merr != nil {
		return r, errors.Join(err, fmt.Errorf("%s: encode evidence: %w", label, merr))
	}
	evidenceMu.Lock()
	_, werr := evidence.Write(append(line, '\n'))
	evidenceMu.Unlock()
	if werr != nil {
		return r, errors.Join(err, fmt.Errorf("%s: write evidence: %w", label, werr))
	}
	if err != nil {
		return r, fmt.Errorf("%s: %w", label, err)
	}
	return r, nil
}

func isStatus(status int, want ...int) bool {
	for _, w := range want {
		if status == w {
			return true
		}
	}
	return false
}

// expect checks the status and decodes a JSON body, keeping numbers exact.
func expect(r record, want ...int) (map[string]any, error) {
	if !isStatus(r.Status, want...) {
		return nil, fmt.Errorf("%s: HTTP %d: %s", r.Label, r.Status, r.ResponseBody)
	}
	out := map[string]any{}
	if r.ResponseBody == "" {
		return out, nil
	}
	dec := json.NewDecoder(strings.NewReader(r.ResponseBody))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", r.Label, err)
	}
	return out, nil
}

func metadataOf(r record) (map[string]any, error) {
	body, err := expect(r, 200)
	if err != nil {
		return nil, err
	}
	m, ok := body["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: response has no metadata object", r.Label)
	}
	return m, nil
}

func intOf(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, fmt.Errorf("want a number, got %v", v)
	}
	return n.Int64()
}

func schemas(meta map[string]any) []map[string]any {
	list, _ := meta["schemas"].([]any)
	var out []map[string]any
	for _, s := range list {
		if m, ok := s.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func currentSchema(meta map[string]any) (map[string]any, error) {
	id, err := intOf(meta["current-schema-id"])
	if err != nil {
		return nil, fmt.Errorf("current-schema-id: %w", err)
	}
	for _, s := range schemas(meta) {
		if sid, err := intOf(s["schema-id"]); err == nil && sid == id {
			return s, nil
		}
	}
	return nil, fmt.Errorf("current schema %d is not in the metadata", id)
}

func fieldNames(schema map[string]any) []string {
	fields, _ := schema["fields"].([]any)
	var names []string
	for _, f := range fields {
		if m, ok := f.(map[string]any); ok {
			if name, ok := m["name"].(string); ok {
				names = append(names, name)
			}
		}
	}
	return names
}

// schemaChange adds column to the current schema as the next schema ID, as the
// Java client does, guarded by the table UUID and current schema ID.
func schemaChange(base map[string]any, column string) (map[string]any, error) {
	current, err := currentSchema(base)
	if err != nil {
		return nil, err
	}
	// A deep copy through JSON, so the base metadata stays untouched.
	data, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var schema map[string]any
	if err := dec.Decode(&schema); err != nil {
		return nil, err
	}
	var maxID int64
	for _, s := range schemas(base) {
		id, err := intOf(s["schema-id"])
		if err != nil {
			return nil, fmt.Errorf("schema-id: %w", err)
		}
		maxID = max(maxID, id)
	}
	lastColumn, err := intOf(base["last-column-id"])
	if err != nil {
		return nil, fmt.Errorf("last-column-id: %w", err)
	}
	fieldID := lastColumn + 1
	schema["schema-id"] = maxID + 1
	fields, _ := schema["fields"].([]any)
	schema["fields"] = append(fields, map[string]any{"id": fieldID, "name": column, "required": false, "type": "long"})
	return map[string]any{
		"requirements": []any{
			map[string]any{"type": "assert-table-uuid", "uuid": base["table-uuid"]},
			map[string]any{"type": "assert-current-schema-id", "current-schema-id": base["current-schema-id"]},
		},
		"updates": []any{
			map[string]any{"action": "add-schema", "schema": schema, "last-column-id": fieldID},
			map[string]any{"action": "set-current-schema", "schema-id": -1},
		},
	}, nil
}

// errReproduced ends run after its evidence is written.
var errReproduced = errors.New("reproduced")

func main() {
	flag.Parse()
	switch err := run(); {
	case errors.Is(err, errReproduced):
		os.Exit(3)
	case err != nil:
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() (err error) {
	if *out == "" {
		return errors.New("-out is required")
	}
	if *rounds < 1 {
		return errors.New("-rounds must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	// The directory must be new, so evidence from two runs never mixes.
	if err := os.Mkdir(*out, 0o700); err != nil {
		return fmt.Errorf("evidence directory: %w", err)
	}
	// Server errors and response headers are in it: readable only by the owner.
	evidence, err = os.OpenFile(filepath.Join(*out, "http.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := evidence.Close(); cerr != nil {
			if errors.Is(err, errReproduced) {
				err = nil
			}
			err = errors.Join(err, fmt.Errorf("close evidence: %w", cerr))
		}
	}()

	cfg, err := call("config", "GET", "/v1/config?warehouse="+url.QueryEscape(*warehouse), nil)
	if err != nil {
		return err
	}
	config, err := expect(cfg, 200)
	if err != nil {
		return err
	}
	prefix := ""
	for _, key := range []string{"defaults", "overrides"} {
		if props, ok := config[key].(map[string]any); ok {
			if p, ok := props["prefix"].(string); ok {
				prefix = p
			}
		}
	}
	api := "/v1/"
	if p := strings.Trim(prefix, "/"); p != "" {
		api += (&url.URL{Path: p}).EscapedPath() + "/"
	}
	levels := strings.Split(*namespace, ".")
	ns, err := call("namespace", "POST", api+"namespaces", map[string]any{"namespace": levels})
	if err != nil {
		return err
	}
	if _, err := expect(ns, 200, 201, 204, 409); err != nil {
		return err
	}
	tables := api + "namespaces/" + url.PathEscape(strings.Join(levels, "\x1f")) + "/tables"

	setup := func(label string) (string, map[string]any, error) {
		nonce := make([]byte, 16)
		if _, err := rand.Read(nonce); err != nil {
			return "", nil, err
		}
		name := "schema_cas_" + hex.EncodeToString(nonce)
		create, err := call(label+"/create", "POST", tables, map[string]any{
			"name": name,
			"schema": map[string]any{"type": "struct", "schema-id": 0,
				"fields": []any{map[string]any{"id": 1, "name": "id", "required": true, "type": "long"}}},
		})
		if err != nil {
			return "", nil, err
		}
		if _, err := expect(create, 200); err != nil {
			return "", nil, err
		}
		path := tables + "/" + url.PathEscape(name)
		load, err := call(label+"/baseline", "GET", path, nil)
		if err != nil {
			return "", nil, err
		}
		base, err := metadataOf(load)
		return path, base, err
	}

	// observe loads the table and checks that the effect matches the answers:
	// an acknowledged column is current, a rejected one exists in no schema, and
	// exactly one contender's column is current. An absent column after a 503
	// fits a rollback but does not prove one.
	observe := func(label, path string, base map[string]any, cs []contender) (observation, error) {
		load, err := call(label+"/final", "GET", path, nil)
		if err != nil {
			return observation{}, err
		}
		final, err := metadataOf(load)
		if err != nil {
			return observation{}, err
		}
		if final["table-uuid"] != base["table-uuid"] {
			return observation{}, fmt.Errorf("%s: table UUID changed", label)
		}
		current, err := currentSchema(final)
		if err != nil {
			return observation{}, fmt.Errorf("%s: %w", label, err)
		}
		visible := map[string]bool{}
		for _, name := range fieldNames(current) {
			visible[name] = true
		}
		anywhere := map[string]bool{}
		for _, s := range schemas(final) {
			for _, name := range fieldNames(s) {
				anywhere[name] = true
			}
		}
		o := observation{TableUUID: fmt.Sprint(base["table-uuid"])}
		present := 0
		for _, c := range cs {
			o.Statuses = append(o.Statuses, c.resp.Status)
			if c.resp.Status >= 200 && c.resp.Status < 300 && !visible[c.column] {
				return o, fmt.Errorf("%s: acknowledged column %s is absent", label, c.column)
			}
			if c.resp.Status >= 400 && c.resp.Status < 500 && anywhere[c.column] {
				return o, fmt.Errorf("%s: rejected column %s is present", label, c.column)
			}
			if visible[c.column] {
				present++
			}
		}
		if present != 1 {
			return o, fmt.Errorf("%s: want exactly one contender's column in the current schema, got %d", label, present)
		}
		for name := range visible {
			o.VisibleColumns = append(o.VisibleColumns, name)
		}
		sort.Strings(o.VisibleColumns)
		return o, nil
	}

	s := summary{Catalog: *catalog, Warehouse: *warehouse, StatusPairs: map[string]int{},
		Note: "An absent column does not prove that a 503 commit was rejected."}

	// Control: a commit, then a stale one from the same baseline after it has finished.
	path, base, err := setup("control")
	if err != nil {
		return err
	}
	var cs []contender
	for _, step := range []struct {
		column string
		want   []int
	}{{"first", []int{200, 204}}, {"stale", []int{409}}} {
		change, err := schemaChange(base, step.column)
		if err != nil {
			return fmt.Errorf("control: %w", err)
		}
		resp, err := call("control/"+step.column, "POST", path, change)
		if err != nil {
			return err
		}
		if _, err := expect(resp, step.want...); err != nil {
			return err
		}
		cs = append(cs, contender{step.column, resp})
	}
	control, err := observe("control", path, base, cs)
	if err != nil {
		return err
	}
	s.Control = &control
	fmt.Println("Sequential control: first accepted, stale schema rejected with 409")

	for r := 0; r < *rounds; r++ {
		label := fmt.Sprintf("race-%d", r)
		path, base, err := setup(label)
		if err != nil {
			return err
		}
		columns := []string{"writer_a", "writer_b"}
		changes := make([]map[string]any, len(columns))
		for i, column := range columns {
			if changes[i], err = schemaChange(base, column); err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
		}
		cs := make([]contender, len(columns))
		errs := make([]error, len(columns))
		var ready, done sync.WaitGroup
		gate := make(chan struct{})
		ready.Add(len(columns))
		done.Add(len(columns))
		for i, column := range columns {
			go func() {
				defer done.Done()
				ready.Done()
				<-gate
				cs[i].column = column
				cs[i].resp, errs[i] = call(label+"/"+column, "POST", path, changes[i])
			}()
		}
		ready.Wait()
		close(gate)
		done.Wait()
		if err := errors.Join(errs...); err != nil {
			return err
		}

		acked := 0
		for _, c := range cs {
			switch {
			case c.resp.Status >= 200 && c.resp.Status < 300:
				acked++
			case c.resp.Status == 503 && strings.Contains(c.resp.ResponseBody, "table_schema_pkey"):
				s.DuplicateKey503++
			}
		}
		if acked != 1 {
			return fmt.Errorf("%s: want exactly one acknowledgment, got %d", label, acked)
		}
		for _, c := range cs {
			dup := c.resp.Status == 503 && strings.Contains(c.resp.ResponseBody, "table_schema_pkey")
			if !dup && !isStatus(c.resp.Status, 200, 204, 409) {
				return fmt.Errorf("%s: unexpected HTTP %d: %s", c.resp.Label, c.resp.Status, c.resp.ResponseBody)
			}
		}
		o, err := observe(label, path, base, cs)
		if err != nil {
			return err
		}
		s.Rounds = append(s.Rounds, o)
		pair := append([]int(nil), o.Statuses...)
		sort.Ints(pair)
		s.StatusPairs[fmt.Sprintf("%d/%d", pair[0], pair[1])]++
		fmt.Printf("%s: %v\n", label, o.Statuses)
	}
	s.Reproduced = s.DuplicateKey503 > 0

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "summary.json"), append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	fmt.Printf("duplicate-key 503s: %d; status pairs: %v; reproduced: %v\n", s.DuplicateKey503, s.StatusPairs, s.Reproduced)
	fmt.Printf("evidence in %s\n", *out)
	if s.Reproduced {
		return errReproduced
	}
	return nil
}
