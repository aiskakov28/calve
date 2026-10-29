package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestAtomicCLIConcurrentReaderCoverageAndReplay(t *testing.T) {
	for _, count := range []int{1, 2} {
		name := "default reader"
		if count == 2 {
			name = "two readers"
		}
		t.Run(name, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			target, _ := url.Parse(fake.Server.URL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			started, observed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var mu sync.Mutex
			loads := map[string]int{}
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
					close(started)
					select {
					case <-observed:
					case <-r.Context().Done():
						return
					}
				}
				if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tables/") {
					mu.Lock()
					loads[r.URL.Path]++
					n := loads[r.URL.Path]
					mu.Unlock()
					if n > 1 {
						select {
						case <-started:
						case <-r.Context().Done():
							return
						}
						// A reader finished A before it can issue B. Keep the
						// commit pending through that successful observation.
						if strings.HasSuffix(r.URL.Path, "_t1") {
							once.Do(func() { close(observed) })
						}
					}
				}
				proxy.ServeHTTP(w, r)
			}))
			defer front.Close()
			path := filepath.Join(t.TempDir(), "atomic.jsonl")
			args := []string{"atomic", "--catalog", front.URL, "--tables", "2", "--clients", "1", "--per-client", "1",
				"--timeout", "2s", "--readback-timeout", "0", "--out", path}
			if count == 2 {
				args = append(args, "--readers", "2")
			}
			output, code := dispatch(t, args...)
			if code != 0 {
				t.Fatalf("exit %d: %s", code, output)
			}
			ops := readSaved(t, path)
			var manifest workload.AtomicRun
			if json.Unmarshal(ops[0].Value, &manifest) != nil || manifest.Readers != count {
				t.Fatal(manifest)
			}
			res, err := checker.CheckAtomic(ops)
			if err != nil || res.ConcurrentReads == 0 || res.ReadCoverageMissing {
				t.Fatal(res, err)
			}
			front.Close()
			fake.Close()
			if replay, code := dispatch(t, "check", path); code != 0 || replay == "" {
				t.Fatal(code, replay)
			}
		})
	}
}

func TestAtomicReadbackCannotReplaceConcurrentCoverage(t *testing.T) {
	for _, mode := range []string{"known", "resolved", "unresolved", "lost"} {
		t.Run(mode, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{
				UnknownAppliedTransactions:   mode == "resolved",
				UnknownUnappliedTransactions: mode == "unresolved",
				LoseTransactions:             mode == "lost",
			})
			defer fake.Close()
			read := make(chan struct{})
			var once sync.Once
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet && history.Operation(r.Context()).Phase == "workload" {
					once.Do(func() { close(read) })
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: http.NoBody}, nil
				}
				if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
					select {
					case <-read:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})
			path := filepath.Join(t.TempDir(), "atomic.jsonl")
			res, err := executeAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1,
				Readers: 1, ReadInterval: time.Millisecond}, path, readbackConfig{Timeout: 50 * time.Millisecond, Interval: time.Millisecond})
			want := checker.Inconclusive
			if mode == "lost" {
				want = checker.Invalid
			}
			if err != nil || res.Verdict != want || !res.ReadCoverageMissing || res.ConcurrentReads != 0 {
				t.Fatal(res, err)
			}
			recovery := false
			for _, op := range readSaved(t, path) {
				recovery = recovery || op.Phase == "recovery"
			}
			if recovery != (mode == "unresolved") {
				t.Fatalf("recovery=%v in mode %s", recovery, mode)
			}
		})
	}
}
