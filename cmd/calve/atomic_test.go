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
	"github.com/aiskakov28/calve/internal/workload"
)

func TestAtomicCommandAndOfflineReplay(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		code int
	}{
		{"correct", fakecatalog.Bugs{}, 0},
		{"lost", fakecatalog.Bugs{LoseTransactions: true}, 3},
		{"partial unknown", fakecatalog.Bugs{PartialTransactions: true, UnknownAppliedTransactions: true}, 3},
		{"absent unknown", fakecatalog.Bugs{UnknownUnappliedTransactions: true}, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			path := filepath.Join(t.TempDir(), "atomic.jsonl")
			output, code := dispatch(t, "atomic", "--catalog", fake.Server.URL, "--tables", "3", "--clients", "2", "--per-client", "2",
				"--readers", "2", "--read-interval", "1ms", "--readback-timeout", "0", "--out", path)
			if code != tt.code {
				t.Fatalf("exit %d, %s", code, output)
			}
			if tt.name == "partial unknown" && !strings.Contains(output, "fresh table loads") {
				t.Fatal("missing model caveat", output)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fake.Close()
			output, code = dispatch(t, "check", path)
			if code != tt.code {
				t.Fatalf("offline exit %d, %s", code, output)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("replay changed evidence")
			}
		})
	}
}

func TestAtomicReadbackRetriesOnlyReads(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{UnknownAppliedTransactions: true})
	defer fake.Close()
	var finals, posts atomic.Int32
	client := rest.NewClient(fake.Server.URL, "", "private-token", time.Second)
	client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
			posts.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tables/") && history.Operation(r.Context()).Phase == "final" {
			finals.Add(1)
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: http.NoBody}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	path := filepath.Join(t.TempDir(), "atomic.jsonl")
	res, err := executeAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, path,
		readbackConfig{Timeout: time.Second, Interval: time.Millisecond})
	if err != nil || res.Verdict != checker.Valid || res.ResolvedCommitted != 1 || res.Unknown != 1 || posts.Load() != 1 || finals.Load() != 2 {
		t.Fatalf("%+v %v; posts %d, final reads %d", res, err, posts.Load(), finals.Load())
	}
	ops := readSaved(t, path)
	var recovery int
	for _, op := range ops {
		if op.Type == history.Invoke && op.Phase == "recovery" {
			recovery++
		}
	}
	if recovery != 2 {
		t.Fatal("unexpected recovery reads", recovery)
	}
	fake.Close()
	replayed, err := checker.CheckAtomic(ops)
	if err != nil || replayed.Verdict != res.Verdict {
		t.Fatal(replayed, err)
	}
}

func TestAtomicErrorStillSavesEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "failed.jsonl")
	client := rest.NewClient("http://127.0.0.1:1", "", "", time.Second)
	_, err := executeAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, path, readbackConfig{})
	if err == nil {
		t.Fatal("expected setup error")
	}
	ops := readSaved(t, path)
	if len(ops) != 3 || ops[0].F != "run" || ops[1].F != "config" || ops[2].Type != history.Info {
		t.Fatal(ops)
	}
	_, err = executeAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1},
		filepath.Join(dir, "missing", "failed.jsonl"), readbackConfig{})
	if err == nil || !strings.Contains(err.Error(), "config") || !strings.Contains(err.Error(), "save atomic history") {
		t.Fatal(err)
	}
}
