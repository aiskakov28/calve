package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func readSaved(t *testing.T, path string) []history.Op {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ops, err := history.ReadJSONL(f)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func appendConfig() workload.AppendConfig {
	return workload.AppendConfig{Namespace: "test", Table: "append_test", Clients: 4, PerClient: 3}
}

func TestAppendArtifactCanBeRecheckedWithoutCatalog(t *testing.T) {
	for _, loseEvery := range []int{0, 3} {
		t.Run(string(rune('0'+loseEvery)), func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{LoseEvery: loseEvery})
			t.Cleanup(fake.Close)
			path := filepath.Join(t.TempDir(), "history.jsonl")
			client := rest.NewClient(fake.Server.URL, "", "private-token", time.Second)
			result, _, err := executeAppend(context.Background(), client, appendConfig(), path)
			if err != nil {
				t.Fatal(err)
			}
			if result.OK != 12 || result.Valid != (loseEvery == 0) {
				t.Fatalf("unexpected check: %+v", result)
			}
			fake.Close()
			ops := readSaved(t, path)
			if ops[0].Type != history.Meta || ops[0].Version != history.Version {
				t.Fatal("missing versioned run record")
			}
			var manifest map[string]any
			if err := json.Unmarshal(ops[0].Value, &manifest); err != nil || manifest["run_id"] != "append_test" || manifest["clients"] != float64(4) {
				t.Fatalf("manifest: %v %v", manifest, err)
			}
			invocations := map[int]history.Op{}
			phases := map[string]bool{}
			var final rest.Response
			for _, op := range ops[1:] {
				if op.Version != history.Version {
					t.Fatal("unversioned record")
				}
				phases[op.Phase] = true
				if op.Type == history.Invoke {
					if op.ID == 0 || op.Request == nil {
						t.Fatal("missing request identity/evidence")
					}
					if _, exists := invocations[op.ID]; exists {
						t.Fatal("duplicate ID")
					}
					invocations[op.ID] = op
				} else {
					invoke, exists := invocations[op.ID]
					if !exists || op.Time < invoke.Time || op.F != invoke.F {
						t.Fatal("bad operation pair")
					}
					delete(invocations, op.ID)
				}
				if op.Phase == "final" && op.Type == history.OK {
					final = rest.Response{Status: op.Response.Status, Body: op.Response.Body}
				}
			}
			if len(invocations) != 0 || !phases["setup"] || !phases["baseline"] || !phases["workload"] || !phases["final"] {
				t.Fatal("incomplete evidence")
			}
			meta, err := rest.ParseMetadata(final)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := checker.CheckAppend(ops, workload.AppendKeys(meta))
			if err != nil || !reflect.DeepEqual(replayed, result) {
				t.Fatalf("offline verdict changed: %+v; %v", replayed, err)
			}
			bytes, _ := os.ReadFile(path)
			if strings.Contains(string(bytes), "private-token") {
				t.Fatal("token saved")
			}
		})
	}
}

func TestAppendSavesHistoryOnReadAndSetupFailures(t *testing.T) {
	for _, phase := range []string{"setup", "baseline", "final", "identity"} {
		t.Run(phase, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			var loads atomic.Int32
			client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				load := int32(0)
				if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/tables/append_test") {
					load = loads.Add(1)
				}
				fail := phase == "setup" && r.URL.Path == "/v1/config" || phase == "baseline" && load == 1 || phase == "final" && load == 2
				if fail {
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("<html>unavailable</html>"))}, nil
				}
				if phase == "identity" && load == 2 {
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"metadata":{"table-uuid":"replaced","properties":{}}}`))}, nil
				}
				return http.DefaultTransport.RoundTrip(r)
			})
			path := filepath.Join(t.TempDir(), "history.jsonl")
			result, _, err := executeAppend(context.Background(), client, appendConfig(), path)
			if err == nil || result.Valid {
				t.Fatal("failed/inconclusive run reported valid")
			}
			ops := readSaved(t, path)
			last := ops[len(ops)-1]
			if phase == "identity" {
				if !strings.Contains(err.Error(), "inconclusive") {
					t.Fatalf("wrong identity error: %v", err)
				}
			} else if last.Type != history.Info || last.Status != 503 || string(last.Response.Body) != "<html>unavailable</html>" {
				t.Fatalf("failure evidence lost: %+v", last)
			}
			if phase == "final" || phase == "identity" {
				completions := 0
				for _, op := range ops {
					if op.F == "append" && op.Type == history.OK {
						completions++
					}
				}
				if completions != 12 {
					t.Fatalf("lost workload evidence: %d", completions)
				}
			}
		})
	}
}

func TestAppendReportsBothRunAndSaveErrors(t *testing.T) {
	client := rest.NewClient("http://catalog", "", "", time.Second)
	client.HTTP.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("failed")), Header: make(http.Header)}, nil
	})
	_, _, err := executeAppend(context.Background(), client, appendConfig(), filepath.Join(t.TempDir(), "missing", "history.jsonl"))
	if err == nil || !strings.Contains(err.Error(), "config:") || !strings.Contains(err.Error(), "save history:") {
		t.Fatalf("missing combined error: %v", err)
	}
}

// skewFake starts a fake catalog. With WriteSkew it holds each concurrent pair
// after validation until both transactions arrive, so the race is deterministic.
func skewFake(t *testing.T, bugs fakecatalog.Bugs, rounds int) *fakecatalog.Catalog {
	t.Helper()
	fake := fakecatalog.New(bugs)
	t.Cleanup(fake.Close)
	if bugs.WriteSkew {
		var validated atomic.Int32
		gates := make([]chan struct{}, rounds)
		for i := range gates {
			gates[i] = make(chan struct{})
		}
		fake.AfterTransactionValidation = func(ctx context.Context) {
			n := int(validated.Add(1))
			if n > 2*rounds { // The sequential controls must not wait.
				return
			}
			gate := gates[(n-1)/2]
			if n%2 == 0 {
				close(gate)
			}
			select {
			case <-gate:
			case <-ctx.Done():
			}
		}
	}
	return fake
}

func TestSkewVerdictsAndOfflineRecheck(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bugs    fakecatalog.Bugs
		verdict checker.SkewVerdict
		skews   int
	}{
		{"correct", fakecatalog.Bugs{}, checker.SkewValid, 0},
		{"write-skew", fakecatalog.Bugs{WriteSkew: true}, checker.SkewInvalid, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := skewFake(t, tc.bugs, 3)
			path := filepath.Join(t.TempDir(), "history-skew.jsonl")
			client := rest.NewClient(fake.Server.URL, "", "private-token", 3*time.Second)
			res, err := executeSkew(context.Background(), client, workload.SkewConfig{Namespace: "test", Rounds: 3}, path)
			if err != nil {
				t.Fatal(err)
			}
			if res.Verdict != tc.verdict || len(res.Rounds) != 5 {
				t.Fatalf("want %s over 5 rounds, got %+v", tc.verdict, res)
			}
			if got := skewKinds(res.Anomalies); tc.skews > 0 && got != fmt.Sprintf("%d write-skew", tc.skews) {
				t.Fatalf("unexpected findings: %s", got)
			}
			fake.Close()
			again, err := checker.CheckSkew(readSaved(t, path))
			if err != nil {
				t.Fatal(err)
			}
			if again.Verdict != res.Verdict || len(again.Anomalies) != len(res.Anomalies) {
				t.Fatalf("offline recheck disagrees: %+v vs %+v", again, res)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "private-token") {
				t.Fatal("history contains the bearer token")
			}
		})
	}
}

func TestSkewSavesHistoryAndReportsBothErrors(t *testing.T) {
	client := rest.NewClient("http://catalog", "", "", time.Second)
	client.HTTP.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("failed")), Header: make(http.Header)}, nil
	})
	cfg := workload.SkewConfig{Namespace: "test", Rounds: 1}

	path := filepath.Join(t.TempDir(), "history-skew.jsonl")
	if _, err := executeSkew(context.Background(), client, cfg, path); err == nil || !strings.Contains(err.Error(), "config:") {
		t.Fatalf("want config error, got %v", err)
	}
	if ops := readSaved(t, path); len(ops) == 0 || ops[0].F != "run" {
		t.Fatalf("failed run did not save its history: %+v", ops)
	}

	_, err := executeSkew(context.Background(), client, cfg, filepath.Join(t.TempDir(), "missing", "history-skew.jsonl"))
	if err == nil || !strings.Contains(err.Error(), "config:") || !strings.Contains(err.Error(), "save history:") {
		t.Fatalf("missing combined error: %v", err)
	}
}

func TestSkewReasonsAreCountedOnce(t *testing.T) {
	res := checker.SkewResult{
		Reasons: []string{"run reason"},
		Rounds: []checker.SkewRoundResult{
			{Reasons: []string{"unresolved"}},
			{Reasons: []string{"unresolved", "missing read"}},
		},
	}
	got := skewReasons(res)
	want := []string{"1x run reason", "2x unresolved", "1x missing read"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestIsolationCaveatOnlyForModelFindings(t *testing.T) {
	contract := []checker.SkewAnomaly{{Kind: "lost", Basis: "catalog-contract"}, {Kind: "stale-requirement", Basis: "catalog-contract"}}
	if hasIsolationLead(contract) {
		t.Fatal("contract findings must not get the isolation-lead caveat")
	}
	if !hasIsolationLead(append(contract, checker.SkewAnomaly{Kind: "write-skew", Basis: "serializable-model"})) {
		t.Fatal("write-skew finding must get the isolation-lead caveat")
	}
}

func TestSkewSummaryRedactsCredentials(t *testing.T) {
	got := skewSummary("http://user:hunter2@catalog/iceberg?token=abc", "s3cret-token", checker.SkewResult{})
	for _, secret := range []string{"hunter2", "abc", "user:"} {
		if strings.Contains(got, secret) {
			t.Fatalf("summary leaks %q: %s", secret, got)
		}
	}
	if got := skewSummary("http://catalog/s3cret-token", "s3cret-token", checker.SkewResult{}); strings.Contains(got, "s3cret-token") {
		t.Fatalf("summary leaks the token: %s", got)
	}
}
