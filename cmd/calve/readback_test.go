package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func oneAppend() workload.AppendConfig {
	return workload.AppendConfig{Namespace: "test", Table: "append_test", Clients: 1, PerClient: 1}
}

// This proxy accepts a request and keeps doing server work after the client's
// actual HTTP timeout. It returns baseline/early-final reads before releasing
// the pending writes on the first read-back GET. Each commit is forwarded once.
func lateCommitProxy(t *testing.T, skew bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	fake := fakecatalog.New(fakecatalog.Bugs{})
	t.Cleanup(fake.Close)
	release := make(chan struct{})
	var once sync.Once
	var pending sync.WaitGroup
	count := 1
	if skew {
		count = 2
	}
	pending.Add(count)
	var posts atomic.Int32
	var mu sync.Mutex
	loads := map[string]int{}
	isTable := func(path string) bool {
		if skew {
			return strings.Contains(path, "_r0_")
		}
		return strings.HasSuffix(path, "/tables/append_test")
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		late := r.Method == http.MethodPost && isTable(r.URL.Path)
		if skew {
			late = r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/transactions/commit") &&
				bytes.Contains(body, []byte("_r0_"))
		}
		if late {
			posts.Add(1)
			<-release // Intentionally independent of r.Context(), like accepted server work.
			defer pending.Done()
		}
		if r.Method == http.MethodGet && isTable(r.URL.Path) {
			mu.Lock()
			loads[r.URL.Path]++
			n := loads[r.URL.Path]
			mu.Unlock()
			if n >= 3 {
				once.Do(func() { close(release) })
				pending.Wait()
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, r.Method, fake.Server.URL+r.URL.RequestURI(), bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, values := range resp.Header {
			w.Header()[k] = values
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		proxy.Close()
	})
	return proxy, &posts
}

func beforeReadback(ops []history.Op) []history.Op {
	for i, op := range ops {
		if op.Phase == "recovery" {
			return ops[:i]
		}
	}
	return ops
}

func TestAppendLateCommitResolvedOnlyAfterReadback(t *testing.T) {
	proxy, posts := lateCommitProxy(t, false)
	client := rest.NewClient(proxy.URL, "", "private-token", 200*time.Millisecond)
	path := filepath.Join(t.TempDir(), "history.jsonl")
	res, _, err := executeAppend(context.Background(), client, oneAppend(), path,
		readbackConfig{Timeout: time.Second, Interval: time.Millisecond})
	if err != nil || res.Verdict != checker.Valid || res.Unknown != 1 || res.ResolvedCommitted != 1 || posts.Load() != 1 {
		t.Fatalf("late commit was not resolved without resubmission: %+v; %v; posts=%d", res, err, posts.Load())
	}
	ops := readSaved(t, path)
	early, err := checker.CheckAppendHistory(beforeReadback(ops))
	if err != nil || early.Verdict != checker.Inconclusive || early.Unresolved != 1 || len(early.Anomalies) != 0 {
		t.Fatalf("early absence resolved a pending commit: %+v; %v", early, err)
	}
	proxy.Close()
	replayed, err := checker.CheckAppendHistory(ops)
	if err != nil || !reflect.DeepEqual(replayed, res) {
		t.Fatalf("offline resolution changed: %+v; %v", replayed, err)
	}
	operation := res.Operations[0]
	if operation.Outcome != history.Info || operation.CommitState != "committed" || len(operation.EvidenceIDs) != 1 {
		t.Fatalf("resolution replaced the raw outcome or omitted its read evidence: %+v", operation)
	}
	var timeout bool
	for _, op := range ops {
		if op.F == "append" && op.Type == history.Info && op.Status == 0 && strings.Contains(op.Error, "deadline") {
			timeout = true
		}
	}
	if !timeout {
		t.Fatal("test did not exercise a real client timeout")
	}
}

func TestSkewLateCommitDoesNotResolveAbsentPeer(t *testing.T) {
	proxy, posts := lateCommitProxy(t, true)
	client := rest.NewClient(proxy.URL, "", "", 200*time.Millisecond)
	path := filepath.Join(t.TempDir(), "skew.jsonl")
	res, err := executeSkew(context.Background(), client, workload.SkewConfig{Namespace: "test", Rounds: 1}, path,
		readbackConfig{Timeout: 150 * time.Millisecond, Interval: 2 * time.Millisecond})
	if err != nil || res.Verdict != checker.Inconclusive || len(res.Anomalies) != 0 ||
		res.Unknown != 2 || res.ResolvedCommitted != 1 || posts.Load() != 2 {
		t.Fatalf("late pair was misclassified or resubmitted: %+v; %v; posts=%d", res, err, posts.Load())
	}
	ops := readSaved(t, path)
	early, err := checker.CheckSkew(beforeReadback(ops))
	if err != nil || early.Verdict != checker.Inconclusive || early.ResolvedCommitted != 0 {
		t.Fatalf("early reads resolved pending transactions: %+v; %v", early, err)
	}
	proxy.Close()
	replayed, err := checker.CheckSkew(ops)
	if err != nil || !reflect.DeepEqual(replayed, res) {
		t.Fatalf("offline skew resolution changed: %+v; %v", replayed, err)
	}
}

func TestAppendUnknownReadbackCases(t *testing.T) {
	for _, tc := range []struct {
		name             string
		status           int
		apply            bool
		finalUnavailable bool
	}{
		{"500-applied", 500, true, false}, {"502-applied", 502, true, false},
		{"503-applied", 503, true, false}, {"504-applied", 504, true, false},
		{"503-unapplied", 503, false, false}, {"response-lost", 0, true, false},
		{"partial-success", 200, true, false}, {"failed-final-read", 503, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			t.Cleanup(fake.Close)
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			var posts atomic.Int32
			client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if tc.finalUnavailable && history.Operation(r.Context()).Phase == "final" {
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
				}
				if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/tables/append_test") {
					return http.DefaultTransport.RoundTrip(r)
				}
				posts.Add(1)
				if tc.apply {
					resp, err := http.DefaultTransport.RoundTrip(r)
					if err != nil {
						return resp, err
					}
					resp.Body.Close()
				}
				if tc.status == 0 {
					return nil, fmt.Errorf("connection reset after application")
				}
				body := io.ReadCloser(io.NopCloser(strings.NewReader("commit state unknown")))
				if tc.status == 200 {
					body = &brokenReadbackBody{}
				}
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: body}, nil
			})
			path := filepath.Join(t.TempDir(), "history.jsonl")
			res, _, err := executeAppend(context.Background(), client, oneAppend(), path,
				readbackConfig{Timeout: 35 * time.Millisecond, Interval: time.Millisecond})
			want := checker.Inconclusive
			if tc.apply {
				want = checker.Valid
			}
			if err != nil || res.Verdict != want || res.Unknown != 1 || len(res.Anomalies) != 0 || posts.Load() != 1 {
				t.Fatalf("wrong outcome: %+v; %v; posts=%d", res, err, posts.Load())
			}
			if res.Operations[0].Outcome != history.Info || res.ResolvedCommitted != map[bool]int{true: 1}[tc.apply] {
				t.Fatalf("raw outcome/resolution changed: %+v", res)
			}
			replayed, err := checker.CheckAppendHistory(readSaved(t, path))
			if err != nil || !reflect.DeepEqual(res, replayed) {
				t.Fatalf("offline replay changed: %+v; %v", replayed, err)
			}
		})
	}
}

type brokenReadbackBody struct{}

func (*brokenReadbackBody) Read(p []byte) (int, error) {
	copy(p, "{")
	return 1, io.ErrUnexpectedEOF
}
func (*brokenReadbackBody) Close() error { return nil }

func TestAppendHistoryRejectsFalseResolution(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	t.Cleanup(fake.Close)
	path := filepath.Join(t.TempDir(), "history.jsonl")
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	if _, _, err := executeAppend(context.Background(), client, oneAppend(), path, readbackConfig{}); err != nil {
		t.Fatal(err)
	}
	original := readSaved(t, path)
	for _, mode := range []string{"changed-uuid", "wrong-value", "foreign-origin", "mismatched-id", "unrelated-update", "missing-final", "missing-completion", "unknown-setup-drop"} {
		t.Run(mode, func(t *testing.T) {
			var data bytes.Buffer
			history.WriteJSONL(&data, original)
			ops, _ := history.ReadJSONL(&data)
			var filtered []history.Op
			for i := range ops {
				op := &ops[i]
				if mode == "unknown-setup-drop" && op.F == "drop-table" && op.Type != history.Invoke {
					op.Type, op.Status, op.Response.Status = history.Info, 503, 503
				}
				if op.F == "append" && op.Type == history.OK {
					op.Type, op.Status, op.Response.Status = history.Info, 503, 503
					if mode == "missing-completion" {
						continue
					}
				}
				if op.Phase == "final" {
					if mode == "missing-final" {
						continue
					}
					if op.Type == history.Invoke && mode == "foreign-origin" {
						op.Request.URL = "http://another-catalog/v1/namespaces/test/tables/append_test"
					}
					if op.Type == history.OK {
						var body map[string]any
						json.Unmarshal(op.Response.Body, &body)
						meta := body["metadata"].(map[string]any)
						switch mode {
						case "changed-uuid":
							meta["table-uuid"] = "different"
						case "wrong-value":
							meta["properties"].(map[string]any)["calve.append.0-0"] = "different"
						case "mismatched-id":
							op.ID += 1000
						}
						op.Response.Body, _ = json.Marshal(body)
					}
				}
				if mode == "unrelated-update" && op.F == "append" && op.Type == history.Invoke {
					op.Request.Body = []byte(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"different":"1"}}]}`)
				}
				filtered = append(filtered, *op)
			}
			res, err := checker.CheckAppendHistory(filtered)
			if mode == "missing-completion" {
				if err != nil || res.ResolvedCommitted != 1 || res.Operations[0].Outcome != history.Info {
					t.Fatalf("visible update did not resolve an incomplete invocation: %+v; %v", res, err)
				}
				return
			}
			if err == nil && (res.Verdict != checker.Inconclusive || res.ResolvedCommitted != 0 || len(res.Anomalies) != 0) {
				t.Fatalf("unusable evidence resolved a commit or produced a finding: %+v", res)
			}
		})
	}
}

func TestAppendResolutionSurvivesLaterObservations(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	t.Cleanup(fake.Close)
	path := filepath.Join(t.TempDir(), "history.jsonl")
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	if _, _, err := executeAppend(context.Background(), client, oneAppend(), path, readbackConfig{}); err != nil {
		t.Fatal(err)
	}
	original := readSaved(t, path)
	for _, mode := range []string{"missing-effect", "failed-read", "rejected-effect", "unexpected-effect"} {
		t.Run(mode, func(t *testing.T) {
			var buf bytes.Buffer
			history.WriteJSONL(&buf, original)
			ops, _ := history.ReadJSONL(&buf)
			var invocation, completion history.Op
			for i := range ops {
				op := &ops[i]
				if op.F == "append" && op.Type == history.OK {
					op.Type, op.Status, op.Response.Status = history.Info, 503, 503
					if mode == "rejected-effect" {
						op.Type, op.Status, op.Response.Status = history.Fail, 409, 409
					}
				}
				if op.Phase == "final" {
					if mode == "unexpected-effect" && op.Type == history.OK {
						var body map[string]any
						json.Unmarshal(op.Response.Body, &body)
						body["metadata"].(map[string]any)["properties"].(map[string]any)["calve.append.unexpected"] = "1"
						op.Response.Body, _ = json.Marshal(body)
					}
					if op.Type == history.Invoke {
						invocation = *op
					} else {
						completion = *op
					}
				}
			}
			// Add a later raw table observation, retaining the earlier positive
			// evidence and the original unknown/rejected commit response.
			for _, op := range []history.Op{invocation, completion} {
				op.ID += 100
				op.Phase, op.Index, op.Time = "recovery", len(ops), ops[len(ops)-1].Time+1
				if op.Type == history.OK {
					response := *op.Response
					op.Response = &response
					if mode == "missing-effect" || mode == "unexpected-effect" {
						var body map[string]any
						json.Unmarshal(op.Response.Body, &body)
						body["metadata"].(map[string]any)["properties"] = map[string]any{}
						if mode == "unexpected-effect" {
							body["metadata"].(map[string]any)["properties"] = map[string]any{"calve.append.0-0": "1"}
						}
						op.Response.Body, _ = json.Marshal(body)
					} else {
						op.Type, op.Status, op.Response.Status = history.Info, 503, 503
						op.Response.Body = []byte("unavailable")
					}
				}
				ops = append(ops, op)
			}
			res, err := checker.CheckAppendHistory(ops)
			if err != nil || res.Operations[0].CommitState != "committed" || len(res.Operations[0].EvidenceIDs) != 1 {
				t.Fatalf("later read erased commitment evidence: %+v; %v", res, err)
			}
			switch mode {
			case "missing-effect":
				if res.Verdict != checker.Invalid || len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "lost" {
					t.Fatalf("later disappearance was not retained: %+v", res)
				}
			case "failed-read":
				if res.Verdict != checker.Inconclusive || len(res.Anomalies) != 0 {
					t.Fatalf("failed read invented an anomaly/pass: %+v", res)
				}
			case "rejected-effect":
				if res.Verdict != checker.Invalid || len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "phantom-failed" {
					t.Fatalf("failed read erased an independent rejected effect: %+v", res)
				}
			case "unexpected-effect":
				if res.Verdict != checker.Invalid || len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "unexpected" {
					t.Fatalf("later absence erased an independently observed unexpected effect: %+v", res)
				}
			}
		})
	}
}

func TestReadBackHasIndependentBoundedBudget(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	client := rest.NewClient("http://catalog", "", "", time.Second)
	var calls atomic.Int32
	client.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Context().Err() != nil || r.Method != http.MethodGet {
			t.Error("read-back inherited cancellation or issued a write")
		}
		calls.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	start := time.Now()
	err := readBack(parent, client, readbackConfig{Timeout: 25 * time.Millisecond, Interval: time.Millisecond},
		[]readbackTarget{{URL: "http://catalog/v1/namespaces/test/tables/t"}},
		func() (bool, error) { return false, nil })
	if err != nil || calls.Load() != 1 || time.Since(start) > time.Second {
		t.Fatalf("observation budget was not independent/bounded: calls=%d elapsed=%s err=%v", calls.Load(), time.Since(start), err)
	}
}

func TestAppendCommandUnknownExitCodes(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(fmt.Sprint(apply), func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			t.Cleanup(fake.Close)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				commit := r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/tables/")
				if commit && !apply {
					http.Error(w, "commit state unknown", 503)
					return
				}
				req, _ := http.NewRequestWithContext(r.Context(), r.Method, fake.Server.URL+r.URL.RequestURI(), r.Body)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					http.Error(w, err.Error(), 502)
					return
				}
				defer resp.Body.Close()
				if commit {
					http.Error(w, "applied before response lost", 503)
					return
				}
				w.WriteHeader(resp.StatusCode)
				io.Copy(w, resp.Body)
			}))
			t.Cleanup(proxy.Close)
			path := filepath.Join(t.TempDir(), "append.jsonl")
			output, code := dispatch(t, "append", "--catalog", proxy.URL, "--namespace", "test",
				"--clients", "1", "--per-client", "1", "--out", path,
				"--readback-timeout", "25ms", "--readback-interval", "2ms")
			wantCode, wantText := 4, "INCONCLUSIVE:"
			if apply {
				wantCode, wantText = 0, "VALID:"
			}
			if code != wantCode || !strings.Contains(output, wantText) {
				t.Fatalf("append dispatch: exit %d, %s", code, output)
			}
			res, err := checker.CheckAppendHistory(readSaved(t, path))
			if err != nil || res.Unknown != 1 || res.Valid != apply {
				t.Fatalf("CLI and saved evidence disagree: %+v; %v", res, err)
			}
		})
	}
}
