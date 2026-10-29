package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestAtomicReviewInvalidArgumentsPreserveExistingEvidence(t *testing.T) {
	for _, badReadback := range []bool{false, true} {
		name := "bad workload"
		if badReadback {
			name = "bad readback"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.jsonl")
			before := []byte("previous run's evidence\n")
			if err := os.WriteFile(path, before, 0600); err != nil {
				t.Fatal(err)
			}
			cfg := workload.AtomicConfig{Namespace: "test", Tables: 1, Clients: 1, PerClient: 1}
			budget := readbackConfig{}
			if badReadback {
				cfg.Tables = 2
				budget.Timeout = -time.Second
			}
			_, err := executeAtomic(context.Background(), rest.NewClient("http://127.0.0.1:1", "", "", time.Second), cfg, path, budget)
			after, readErr := os.ReadFile(path)
			if err == nil || readErr != nil || string(after) != string(before) {
				t.Fatalf("argument error replaced existing evidence: %q; %v; %v", after, err, readErr)
			}
		})
	}
}

func TestAtomicReviewSetupErrorsRedactConfiguredToken(t *testing.T) {
	for _, token := range []string{"review-private-token-123", strings.Repeat("long-review-private-token-", 40)} {
		t.Run(token[:12], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "authentication failed for token "+token, 401)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "history.jsonl")
			output, code := dispatch(t, "atomic", "--catalog", server.URL, "--token", token, "--out", path)
			if code != 1 || strings.Contains(output, token[:min(len(token), 32)]) {
				t.Fatalf("secret exposed in command error: exit %d, %s", code, output)
			}
			data, err := os.ReadFile(path)
			if err != nil || strings.Contains(string(data), token) {
				t.Fatalf("secret exposed in artifact: %v", err)
			}
			for _, op := range readSaved(t, path) {
				if op.Type != history.Meta && op.Response != nil && strings.Contains(string(op.Response.Body), token[:min(len(token), 32)]) {
					t.Fatal("secret exposed in decoded HTTP body")
				}
			}
		})
	}
}
