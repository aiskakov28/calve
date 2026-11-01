// Package rest is a minimal Iceberg REST catalog client.
//
// It speaks raw HTTP and JSON on purpose: Calve tests catalogs, so it must
// see every status code and body exactly as the server sent them, without a
// client library retrying or reinterpreting anything.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

// Client talks to one Iceberg REST catalog.
type Client struct {
	// BaseURL is the catalog root, e.g. http://localhost:8181 (no trailing /v1).
	BaseURL string
	// Warehouse is passed to /v1/config when the catalog needs one.
	Warehouse string
	// Token is an optional bearer token.
	Token string
	// HTTP is the underlying client; NewClient sets a timeout.
	HTTP *http.Client

	prefix   string
	recorder *history.Recorder
}

// NewClient returns a client with a request timeout.
func NewClient(baseURL, warehouse, token string, timeout time.Duration) *Client {
	return &Client{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		Warehouse: warehouse,
		Token:     token,
		HTTP:      &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

// WithRecorder copies the client; configure it before using it concurrently.
func (c *Client) WithRecorder(rec *history.Recorder) *Client {
	copy := *c
	copy.recorder = rec
	return &copy
}

// ReloadTable repeats a recorded table GET using its negotiated routing prefix.
// It accepts only a table URL under this catalog root; it cannot replay writes
// or redirect observation to a different origin.
func (c *Client) ReloadTable(ctx context.Context, rawURL string) Response {
	base, baseErr := url.Parse(c.BaseURL)
	u, err := url.Parse(rawURL)
	if baseErr != nil || err != nil || u.Scheme != base.Scheme || u.Host != base.Host ||
		!strings.HasPrefix(u.EscapedPath(), strings.TrimRight(base.EscapedPath(), "/")+"/v1/") ||
		!strings.Contains(u.EscapedPath(), "/namespaces/") || !strings.Contains(u.EscapedPath(), "/tables/") ||
		u.RawQuery != "" || u.Fragment != "" {
		return Response{Err: fmt.Errorf("reload requires a table URL under this catalog root")}
	}
	path := strings.TrimPrefix(u.EscapedPath(), strings.TrimRight(base.EscapedPath(), "/"))
	return c.do(ctx, "load-table", http.MethodGet, path, nil)
}

// Response is what the catalog answered. Err includes request/transport/body
// errors; Status and partial Body are retained when a response was interrupted.
type Response struct {
	Status  int
	Headers http.Header
	Body    []byte
	Err     error
}

// Outcome classifies a response the way the history needs it.
//
//   - 2xx: the operation definitely happened ("ok").
//   - 4xx: the operation definitely did not happen ("fail"), e.g. 409 conflict.
//   - 5xx or no response: unknown ("info"). The Iceberg REST spec says a
//     commit answered with 500, 502 or 504 may or may not have been applied,
//     so Calve never treats these as failures.
func (r Response) Outcome() string {
	switch {
	case r.Err != nil:
		return "info"
	case r.Status >= 200 && r.Status < 300:
		return "ok"
	case r.Status >= 400 && r.Status < 500:
		return "fail"
	default:
		return "info"
	}
}

func (c *Client) do(ctx context.Context, f, method, path string, body any) (out Response) {
	var reader io.Reader
	var encoded []byte
	var encodeErr error
	if body != nil {
		encoded, encodeErr = json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if c.Token != "" {
		headers.Set("Authorization", "Bearer "+c.Token)
	}
	requestURL := c.BaseURL + path
	if c.recorder != nil {
		labels := history.Operation(ctx)
		op := history.Op{Process: labels.Process, F: labels.F, Phase: labels.Phase, Round: labels.Round, Value: labels.Value, Type: history.Invoke}
		if op.F == "" {
			op.F = f
		}
		op.Request = history.CaptureRequest(method, requestURL, headers, encoded, c.Token)
		invoke := c.recorder.Record(op)
		defer func() {
			done := history.Op{ID: invoke.ID, Process: invoke.Process, F: invoke.F, Phase: invoke.Phase, Round: invoke.Round, Value: invoke.Value, Status: out.Status, Type: history.Type(out.Outcome())}
			if out.Status != 0 {
				done.Response = history.CaptureResponse(out.Status, out.Headers, out.Body, c.Token)
			}
			if done.Type != history.OK {
				safe := out
				if done.Response != nil {
					safe.Body = done.Response.Body
				}
				done.Error = history.RedactText(Describe(safe), requestURL, c.Token)
			}
			c.recorder.Record(done)
		}()
	}
	if encodeErr != nil {
		return Response{Err: encodeErr}
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
	if err != nil {
		return Response{Err: err}
	}
	req.Header = headers
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Response{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		// The status arrived but the body did not finish: still unknown.
		return Response{Status: resp.StatusCode, Headers: resp.Header, Body: data, Err: err}
	}
	return Response{Status: resp.StatusCode, Headers: resp.Header, Body: data}
}

// path builds /v1/{prefix}/rest. The prefix comes from /v1/config and may
// contain several path segments, so it is inserted as-is, never re-escaped.
func (c *Client) path(rest string) string {
	if c.prefix == "" {
		return "/v1/" + rest
	}
	return "/v1/" + strings.Trim(c.prefix, "/") + "/" + rest
}

func ns(namespace string) string {
	// Multi-level namespaces are joined with the unit separator (0x1F).
	return url.PathEscape(strings.ReplaceAll(namespace, ".", "\x1f"))
}

// Config calls GET /v1/config and remembers the routing prefix.
func (c *Client) Config(ctx context.Context) (Response, error) {
	p := "/v1/config"
	if c.Warehouse != "" {
		p += "?warehouse=" + url.QueryEscape(c.Warehouse)
	}
	r := c.do(ctx, "config", http.MethodGet, p, nil)
	if r.Outcome() != "ok" {
		return r, fmt.Errorf("config: %s", describe(r))
	}
	var cfg struct {
		Overrides map[string]string `json:"overrides"`
		Defaults  map[string]string `json:"defaults"`
	}
	if err := json.Unmarshal(r.Body, &cfg); err != nil {
		return r, fmt.Errorf("config: decode: %w", err)
	}
	if p, ok := cfg.Overrides["prefix"]; ok {
		c.prefix = p
	} else if p, ok := cfg.Defaults["prefix"]; ok {
		c.prefix = p
	}
	return r, nil
}

// CreateNamespace calls POST /v1/{prefix}/namespaces.
func (c *Client) CreateNamespace(ctx context.Context, namespace string) Response {
	body := map[string]any{"namespace": strings.Split(namespace, ".")}
	return c.do(ctx, "create-namespace", http.MethodPost, c.path("namespaces"), body)
}

// CreateNamespaceWithProperties calls POST /v1/{prefix}/namespaces with
// properties, which the catalog stores only when it creates the namespace.
func (c *Client) CreateNamespaceWithProperties(ctx context.Context, namespace string, properties map[string]string) Response {
	body := map[string]any{"namespace": strings.Split(namespace, "."), "properties": properties}
	return c.do(ctx, "create-namespace", http.MethodPost, c.path("namespaces"), body)
}

// LoadNamespace calls GET /v1/{prefix}/namespaces/{ns}.
func (c *Client) LoadNamespace(ctx context.Context, namespace string) Response {
	return c.do(ctx, "load-namespace", http.MethodGet, c.path("namespaces/"+ns(namespace)), nil)
}

// Schema is an Iceberg struct schema.
type Schema struct {
	Type   string  `json:"type"`
	Fields []Field `json:"fields"`
}

// Field is one schema column.
type Field struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// CreateTable calls POST /v1/{prefix}/namespaces/{ns}/tables.
func (c *Client) CreateTable(ctx context.Context, namespace, table string, schema Schema) Response {
	body := map[string]any{"name": table, "schema": schema, "stage-create": false}
	return c.do(ctx, "create-table", http.MethodPost, c.path("namespaces/"+ns(namespace)+"/tables"), body)
}

// DropTable calls DELETE /v1/{prefix}/namespaces/{ns}/tables/{table}.
func (c *Client) DropTable(ctx context.Context, namespace, table string) Response {
	return c.do(ctx, "drop-table", http.MethodDelete, c.path("namespaces/"+ns(namespace)+"/tables/"+url.PathEscape(table)), nil)
}

// LoadTable calls GET /v1/{prefix}/namespaces/{ns}/tables/{table}.
func (c *Client) LoadTable(ctx context.Context, namespace, table string) Response {
	return c.do(ctx, "load-table", http.MethodGet, c.path("namespaces/"+ns(namespace)+"/tables/"+url.PathEscape(table)), nil)
}

// TableChange is one table's part of a commit: requirements the catalog must
// check, and updates it must apply atomically.
type TableChange struct {
	Identifier   *Identifier      `json:"identifier,omitempty"`
	Requirements []map[string]any `json:"requirements"`
	Updates      []map[string]any `json:"updates"`
}

// Identifier names a table.
type Identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

// CommitTable calls POST /v1/{prefix}/namespaces/{ns}/tables/{table}.
func (c *Client) CommitTable(ctx context.Context, namespace, table string, change TableChange) Response {
	return c.do(ctx, "commit-table", http.MethodPost, c.path("namespaces/"+ns(namespace)+"/tables/"+url.PathEscape(table)), change)
}

// CommitTransaction calls POST /v1/{prefix}/transactions/commit, the
// multi-table commit that apache/iceberg #18552 is about.
func (c *Client) CommitTransaction(ctx context.Context, changes []TableChange) Response {
	body := map[string]any{"table-changes": changes}
	return c.do(ctx, "commit-transaction", http.MethodPost, c.path("transactions/commit"), body)
}

// TableMetadata is the slice of loadTable's metadata Calve reads.
type TableMetadata struct {
	TableUUID         string            `json:"table-uuid"`
	CurrentSchemaID   int               `json:"current-schema-id"`
	LastColumnID      int               `json:"last-column-id"`
	Schemas           []json.RawMessage `json:"schemas"`
	CurrentSnapshotID *int64            `json:"current-snapshot-id"`
	Properties        map[string]string `json:"properties"`
	Snapshots         []map[string]any  `json:"snapshots"`
}

// ParseMetadata pulls table metadata out of a loadTable response.
func ParseMetadata(r Response) (TableMetadata, error) {
	if r.Outcome() != "ok" {
		return TableMetadata{}, errors.New(describe(r))
	}
	var body struct {
		Metadata TableMetadata `json:"metadata"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		return TableMetadata{}, err
	}
	return body.Metadata, nil
}

func describe(r Response) string {
	if r.Err != nil {
		return "no response: " + r.Err.Error()
	}
	text := strings.TrimSpace(string(r.Body))
	if len(text) > 300 {
		text = text[:300] + "..."
	}
	return fmt.Sprintf("HTTP %d: %s", r.Status, text)
}

// Describe renders a response for logs and error messages.
func Describe(r Response) string { return describe(r) }
