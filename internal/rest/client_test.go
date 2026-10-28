package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPHistoryOutcomesAndRedaction(t *testing.T) {
	for _, status := range []int{200, 409, 500, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			seen := make(chan []byte, 1)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Header.Get("Authorization") != "Bearer client-secret" {
					t.Error("recording changed authentication")
				}
				seen <- body
				w.Header().Set("Set-Cookie", "session=server-secret")
				w.WriteHeader(status)
				io.WriteString(w, `{"snapshot-id":9223372036854775806,"token":"server-secret"}`)
			}))
			defer s.Close()
			rec := history.NewRecorder()
			c := NewClient(s.URL, "", "client-secret", time.Second).WithRecorder(rec)
			ctx := history.WithOperation(context.Background(), history.Op{Process: 3, F: "skew", Phase: "workload", Round: "round-7", Value: json.RawMessage(`{"tables":["a","b"]}`)})
			response := c.CommitTransaction(ctx, []TableChange{{Identifier: &Identifier{Namespace: []string{"ns"}, Name: "a"}, Requirements: []map[string]any{}, Updates: []map[string]any{}}})
			ops := rec.Ops()
			if len(ops) != 2 {
				t.Fatalf("want one pair, got %d records", len(ops))
			}
			invoke, done := ops[0], ops[1]
			if invoke.ID == 0 || done.ID != invoke.ID || invoke.Type != history.Invoke || done.Type != history.Type(response.Outcome()) {
				t.Fatalf("unpaired/wrong outcomes: %+v", ops)
			}
			if done.Time < invoke.Time || done.Process != 3 || done.Round != "round-7" || done.F != "skew" {
				t.Fatal("lost labels or interval")
			}
			if invoke.Request.Method != "POST" || invoke.Request.URL != s.URL+"/v1/transactions/commit" || !bytes.Equal(invoke.Request.Body, <-seen) {
				t.Fatal("request evidence differs from sent request")
			}
			if done.Response.Status != status || !bytes.Contains(done.Response.Body, []byte("9223372036854775806")) || bytes.Contains(done.Response.Body, []byte("server-secret")) {
				t.Fatal("lost/unsafe response")
			}
			encoded, err := json.Marshal(ops)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"client-secret", "server-secret"} {
				if bytes.Contains(encoded, []byte(secret)) {
					t.Fatalf("secret in history: %s", secret)
				}
			}
			// Credential-bearing data remains available to the real caller.
			if !bytes.Contains(response.Body, []byte("server-secret")) {
				t.Fatal("recording changed caller response")
			}
		})
	}
}

type brokenBody struct{ sent bool }

func (b *brokenBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	return copy(p, "partial\xff"), io.ErrUnexpectedEOF
}
func (*brokenBody) Close() error { return nil }

func TestPartialResponseRemainsUnknownAndIsSaved(t *testing.T) {
	rec := history.NewRecorder()
	c := NewClient("http://catalog", "", "", time.Second).WithRecorder(rec)
	c.HTTP.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"request-42"}}, Body: &brokenBody{}}, nil
	})
	r := c.LoadTable(context.Background(), "ns", "table")
	if r.Outcome() != "info" || !errors.Is(r.Err, io.ErrUnexpectedEOF) || string(r.Body) != "partial\xff" {
		t.Fatalf("lost partial response: %+v", r)
	}
	op := rec.Ops()[1]
	if op.Type != history.Info || op.Status != 200 || !bytes.Equal(op.Response.Body, r.Body) || op.Response.Headers.Get("X-Request-Id") != "request-42" {
		t.Fatal("partial evidence lost")
	}
}

func TestCancelledRequestRecordsUnknown(t *testing.T) {
	started := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := history.NewRecorder()
	c := NewClient(s.URL, "", "", 5*time.Second).WithRecorder(rec)
	finished := make(chan Response, 1)
	go func() { finished <- c.LoadTable(ctx, "ns", "table") }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("server was not reached")
	}
	cancel()
	r := <-finished
	if r.Outcome() != "info" || !errors.Is(r.Err, context.Canceled) {
		t.Fatalf("cancelled call: %+v", r)
	}
	ops := rec.Ops()
	if len(ops) != 2 || ops[1].Type != history.Info || ops[1].Response != nil || ops[1].ID != ops[0].ID {
		t.Fatal("unknown call was not paired/preserved")
	}
}

func TestRedirectIsObservableInsteadOfFollowed(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer s.Close()
	rec := history.NewRecorder()
	c := NewClient(s.URL, "", "", time.Second).WithRecorder(rec)
	r := c.LoadTable(context.Background(), "ns", "table")
	if r.Status != 307 || calls.Load() != 1 || !strings.Contains(rec.Ops()[1].Response.Headers.Get("Location"), "/elsewhere") {
		t.Fatal("redirect hidden by client")
	}
}
