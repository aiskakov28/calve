package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
	"github.com/aiskakov28/calve/internal/workload"
)

// A standalone workload refuses an existing --out before it sends anything, so
// it never runs only to be refused when it saves, and the earlier file stays.
func TestWorkloadsRefuseAnExistingHistoryBeforeAnyRequest(t *testing.T) {
	var requests atomic.Int32
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer catalog.Close()
	ctx := context.Background()
	c := rest.NewClient(catalog.URL, "", "", time.Second)
	manifests := t.TempDir()
	runs := map[string]func(out string) error{
		"append": func(out string) error {
			_, _, err := executeAppend(ctx, c, workload.AppendConfig{Namespace: "test", Table: "t", Clients: 1, PerClient: 1}, out, readbackConfig{})
			return err
		},
		"skew": func(out string) error {
			_, err := executeSkew(ctx, c, workload.SkewConfig{Namespace: "test", Rounds: 1}, out, readbackConfig{})
			return err
		},
		"atomic": func(out string) error {
			_, err := executeAtomic(ctx, c, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, out, readbackConfig{})
			return err
		},
		"schema": func(out string) error {
			_, err := executeSchema(ctx, c, workload.SchemaConfig{Namespace: "test", Clients: 2, Rounds: 1}, out, readbackConfig{})
			return err
		},
		"snapshot": func(out string) error {
			_, err := executeSnapshot(ctx, c, workload.SnapshotConfig{Namespace: "test", Clients: 1, Rounds: 1, ManifestRoot: manifests},
				storage.ManifestStore{}, out, time.Second, readbackConfig{})
			return err
		},
		"maintenance": func(out string) error {
			_, err := executeMaintenance(ctx, c, workload.MaintenanceConfig{Namespace: "test", Appends: 1, ExpireInterval: time.Millisecond, ManifestRoot: manifests},
				storage.ManifestStore{}, out, readbackConfig{})
			return err
		},
	}
	for name, run := range runs {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "history.jsonl")
			if err := os.WriteFile(out, []byte("earlier evidence"), 0o600); err != nil {
				t.Fatal(err)
			}
			requests.Store(0)
			if err := run(out); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("an existing history was not refused: %v", err)
			}
			if n := requests.Load(); n != 0 {
				t.Fatalf("sent %d requests before refusing", n)
			}
			if data, _ := os.ReadFile(out); string(data) != "earlier evidence" {
				t.Fatal("the earlier history was changed")
			}
		})
	}
}
