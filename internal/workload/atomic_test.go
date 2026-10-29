package workload

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

func TestAtomicWorkloadWritesEveryTableOncePerAttempt(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	cfg := AtomicConfig{Namespace: "test.nested", Tables: 3, Clients: 4, PerClient: 5}
	rec := history.NewRecorder()
	run, err := RunAtomic(context.Background(), client, cfg, rec)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var count int
	baselineEnd := 0
	for _, op := range rec.Ops() {
		if op.Request != nil && op.Request.Method == http.MethodDelete {
			t.Fatal("workload dropped a table")
		}
		if op.Type == history.OK && op.Phase == "baseline" {
			baselineEnd = op.Index
		}
		if op.Type != history.Invoke || op.F != "atomic-txn" {
			continue
		}
		count++
		if op.Index <= baselineEnd {
			t.Fatal("transaction started before baselines")
		}
		var label struct{ Key string }
		json.Unmarshal(op.Value, &label)
		if seen[label.Key] || !strings.HasPrefix(label.Key, AtomicKeyPrefix+run.RunID+".") {
			t.Fatal(label.Key)
		}
		seen[label.Key] = true
		var request struct {
			Changes []rest.TableChange `json:"table-changes"`
		}
		if json.Unmarshal(op.Request.Body, &request) != nil || len(request.Changes) != 3 {
			t.Fatal(string(op.Request.Body))
		}
		tables := map[string]bool{}
		for _, change := range request.Changes {
			if change.Identifier == nil || tables[change.Identifier.Name] || len(change.Requirements) != 1 ||
				change.Requirements[0]["type"] != "assert-table-uuid" || len(change.Updates) != 1 {
				t.Fatal(change)
			}
			tables[change.Identifier.Name] = true
			updates := change.Updates[0]["updates"].(map[string]any)
			if len(updates) != 1 || updates[label.Key] != "1" {
				t.Fatal(change)
			}
		}
	}
	if count != 20 || len(seen) != 20 {
		t.Fatal(count, len(seen))
	}
	for _, table := range run.Tables {
		meta, err := rest.ParseMetadata(client.LoadTable(context.Background(), cfg.Namespace, table.Name))
		if err != nil {
			t.Fatal(err)
		}
		for key := range seen {
			if meta.Properties[key] != "1" {
				t.Fatal("missing", table.Name, key)
			}
		}
	}
	// A second run uses disjoint tables and keys, rather than dropping old data.
	second, err := RunAtomic(context.Background(), client, AtomicConfig{Namespace: cfg.Namespace, Tables: 2, Clients: 1, PerClient: 1}, history.NewRecorder())
	if err != nil || second.RunID == run.RunID {
		t.Fatal(second, err)
	}
	for _, a := range run.Tables {
		for _, b := range second.Tables {
			if a.Name == b.Name {
				t.Fatal("table names reused")
			}
		}
	}
}

func TestAtomicWorkloadValidationAndCancellation(t *testing.T) {
	for _, cfg := range []AtomicConfig{
		{Namespace: "test", Tables: 1, Clients: 1, PerClient: 1}, {Namespace: "test", Tables: 33, Clients: 1, PerClient: 1},
		{Namespace: "a..b", Tables: 2, Clients: 1, PerClient: 1}, {Namespace: "test", Tables: 2, Clients: 0, PerClient: 1},
		{Namespace: "test", Tables: 2, Clients: int(^uint(0) >> 1), PerClient: 2},
	} {
		rec := history.NewRecorder()
		if _, err := RunAtomic(context.Background(), rest.NewClient("http://127.0.0.1:1", "", "", time.Second), cfg, rec); err == nil || len(rec.Ops()) != 0 {
			t.Fatal(cfg, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := history.NewRecorder()
	if _, err := RunAtomic(ctx, rest.NewClient("http://127.0.0.1:1", "", "", time.Second), AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, rec); err == nil {
		t.Fatal("canceled run succeeded")
	}
	for _, op := range rec.Ops() {
		if op.F == "atomic-txn" {
			t.Fatal("canceled setup started transactions")
		}
	}
}

func TestAtomicErrorsRedactBeforeTruncation(t *testing.T) {
	for _, stage := range []string{"config", "namespace", "create", "baseline"} {
		t.Run(stage, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			target, _ := url.Parse(fake.Server.URL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			token := strings.Repeat("private-error-token-", 40)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				match := stage == "config" && strings.HasSuffix(r.URL.Path, "/config") ||
					stage == "namespace" && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/namespaces") ||
					stage == "create" && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tables") ||
					stage == "baseline" && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tables/")
				if match {
					http.Error(w, "failed for "+token, http.StatusUnauthorized)
					return
				}
				proxy.ServeHTTP(w, r)
			}))
			defer server.Close()
			rec := history.NewRecorder()
			_, err := RunAtomic(context.Background(), rest.NewClient(server.URL, "", token, time.Second),
				AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, rec)
			if err == nil || strings.Contains(err.Error(), token[:32]) {
				t.Fatalf("truncated token leaked: %v", err)
			}
		})
	}
}
