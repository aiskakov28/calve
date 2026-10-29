package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestSnapshotCommandAndOfflineReplay(t *testing.T) {
	for _, tt := range []struct {
		name, model string
		bugs        fakecatalog.Bugs
		code        int
	}{
		{"full", checker.SnapshotModelVersion, fakecatalog.Bugs{}, 0},
		{"current", checker.SnapshotCurrentModelVersion, fakecatalog.Bugs{}, 0},
		{"lost", checker.SnapshotModelVersion, fakecatalog.Bugs{LoseEvery: 1}, 3},
		{"unknown", checker.SnapshotModelVersion, fakecatalog.Bugs{UnknownUnappliedSnapshots: true}, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			path := filepath.Join(t.TempDir(), "history.jsonl")
			output, code := dispatch(t, "snapshot", "--catalog", fake.Server.URL, "--clients", "2", "--rounds", "2", "--model", tt.model,
				"--manifest-root", t.TempDir(), "--readback-timeout", "0", "--out", path)
			if code != tt.code || !strings.Contains(output, tt.model) {
				t.Fatal(code, output)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fake.Close()
			output, code = dispatch(t, "check-table", path)
			if code != tt.code {
				t.Fatal(code, output)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("replay modified evidence")
			}
		})
	}
}

type snapshotWriterFunc func(context.Context, string, []byte) error

func (f snapshotWriterFunc) Put(c context.Context, u string, b []byte) error { return f(c, u, b) }

func TestSnapshotErrorsPreserveEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	before := []byte("earlier evidence")
	os.WriteFile(path, before, 0600)
	c := rest.NewClient("http://127.0.0.1:1", "", "", time.Second)
	cfg := workload.SnapshotConfig{Namespace: "test", Clients: 1, Rounds: 1, ManifestRoot: t.TempDir()}
	if _, err := executeSnapshot(context.Background(), c, cfg, storage.ManifestStore{}, path, time.Second, readbackConfig{}); err == nil {
		t.Fatal("setup succeeded")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("setup failure replaced prior evidence")
	}
	if _, err := executeSnapshot(context.Background(), c, cfg, storage.ManifestStore{}, path, 0, readbackConfig{}); err == nil {
		t.Fatal("invalid search budget")
	}
	after, _ = os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("invalid arguments replaced prior evidence")
	}
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	c = rest.NewClient(fake.Server.URL, "", "", time.Second)
	_, err := executeSnapshot(context.Background(), c, cfg, snapshotWriterFunc(func(context.Context, string, []byte) error { return os.ErrPermission }),
		path, time.Second, readbackConfig{})
	if err == nil {
		t.Fatal("expected storage error")
	}
	ops := readSaved(t, path)
	if len(ops) != 3 || ops[1].Phase != "baseline" {
		t.Fatal("checked prefix was not retained", ops)
	}
}

func TestSnapshotReadbackOnlyRetriesObservations(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{UnknownAppliedSnapshots: true})
	defer fake.Close()
	c := rest.NewClient(fake.Server.URL, "", "", time.Second)
	var writes atomic.Int32
	c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		labels := history.Operation(r.Context())
		if r.Method == http.MethodPost && labels.Phase == "workload" {
			writes.Add(1)
		}
		if labels.Phase == "final" {
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: http.NoBody}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	path := filepath.Join(t.TempDir(), "history.jsonl")
	res, err := executeSnapshot(context.Background(), c, workload.SnapshotConfig{Namespace: "test", Clients: 1, Rounds: 1, ManifestRoot: t.TempDir()},
		storage.ManifestStore{}, path, time.Second, readbackConfig{Timeout: time.Second, Interval: time.Millisecond})
	if err != nil || res.Verdict != checker.Valid || res.Unknown != 1 || res.ResolvedCommitted != 1 || writes.Load() != 1 {
		t.Fatal(res, err, writes.Load())
	}
	fake.Close()
	replayed, err := checker.CheckSnapshotHistory(readSaved(t, path), time.Second)
	if err != nil || replayed.Verdict != res.Verdict {
		t.Fatal(replayed, err)
	}
}
