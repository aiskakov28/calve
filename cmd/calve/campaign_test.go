package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

const (
	healthy int32 = iota
	// loseResponses applies each request, then answers 503: unknown but committed.
	loseResponses
	// down answers 503 without applying anything: unknown and never committed.
	down
)

// faultyCatalog fronts a fake catalog the way Toxiproxy fronts a real one, so a
// controller can break traffic without the workload knowing.
type faultyCatalog struct {
	inner http.Handler
	mode  atomic.Int32
}

func (f *faultyCatalog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch f.mode.Load() {
	case loseResponses:
		f.inner.ServeHTTP(httptest.NewRecorder(), r)
		fallthrough
	case down:
		http.Error(w, `{"error":{"code":503,"type":"ServiceUnavailable","message":"injected"}}`, http.StatusServiceUnavailable)
	default:
		f.inner.ServeHTTP(w, r)
	}
}

// modeController applies a fault by switching the front's mode.
type modeController struct {
	front   *faultyCatalog
	mode    int32
	healErr error
}

func (m *modeController) Apply(context.Context, fault.Injection) error {
	m.front.mode.Store(m.mode)
	return nil
}

func (m *modeController) Heal(context.Context, fault.Injection) error {
	if m.healErr != nil {
		return m.healErr
	}
	m.front.mode.Store(healthy)
	return nil
}

func (m *modeController) Observe(context.Context, fault.Injection) (fault.State, error) {
	if m.front.mode.Load() == healthy {
		return fault.Healthy, nil
	}
	return fault.Active, nil
}

// testCampaign plans two pause faults in about a second of appends.
// The returned run directory does not exist yet; executeCampaign creates it.
func testCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32, healErr error) (*campaign, *rest.Client, workload.AppendConfig, string) {
	t.Helper()
	fake := fakecatalog.New(bugs)
	t.Cleanup(fake.Close)
	front := &faultyCatalog{inner: fake.Server.Config.Handler}
	server := httptest.NewServer(front)
	t.Cleanup(server.Close)
	cfg := fault.Config{Targets: []string{"reference"}, Kinds: []fault.Kind{fault.Pause},
		RunDuration: 1200 * time.Millisecond, Warmup: 100 * time.Millisecond,
		MinHealthy: 100 * time.Millisecond, MaxHealthy: 100 * time.Millisecond,
		MinFault: 200 * time.Millisecond, MaxFault: 200 * time.Millisecond,
		ActionTimeout: 200 * time.Millisecond, CleanupTimeout: time.Second}
	plan, err := fault.Generate(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	controller := &modeController{front: front, mode: mode, healErr: healErr}
	camp := &campaign{
		plan: plan, controllers: map[fault.Kind]fault.Controller{fault.Pause: controller},
		probe: rest.NewClient(server.URL, "", "", time.Second), readyTimeout: time.Second,
	}
	appendCfg := workload.AppendConfig{Namespace: "test", Table: "run_test", Clients: 2, PerClient: 50, Interval: 20 * time.Millisecond}
	return camp, rest.NewClient(server.URL, "", "", time.Second), appendCfg, filepath.Join(t.TempDir(), "run")
}

func TestCampaignResolvesUnknownCommitsAfterHealing(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	res, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{Timeout: time.Second, Interval: 10 * time.Millisecond}, camp, dir, "reference")
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != checker.Valid || res.Unknown == 0 || res.ResolvedCommitted != res.Unknown {
		t.Fatalf("faults should leave unknowns that resolve as committed: %+v", res)
	}
	if n := len(camp.plan.Events); n == 0 || camp.result.Applied != n || camp.result.Healed != n {
		t.Fatalf("not every planned fault was applied and healed: %+v of %d", camp.result, n)
	}
	// The run directory alone re-checks offline to the same verdict.
	var out bytes.Buffer
	if code, err := checkRun(dir, &out); err != nil || code != 0 || !strings.Contains(out.String(), "VALID") {
		t.Fatalf("offline check disagrees: exit %d, %v\n%s", code, err, out.String())
	}
}

func TestCampaignStillReportsLostAppendsBesideUnknowns(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{LoseEvery: 7}, down, nil)
	res, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{Timeout: 200 * time.Millisecond, Interval: 10 * time.Millisecond}, camp, dir, "reference")
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != checker.Invalid || kindsOf(res)["lost"] == 0 || res.Unresolved == 0 {
		t.Fatalf("lost appends were hidden by unresolved faulted appends: %+v", res)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check of an invalid run: exit %d, %v", code, err)
	}
}

func TestCampaignFailedHealIsAnErrorNotAVerdict(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{}, down, errors.New("heal refused"))
	_, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference")
	if err == nil || !strings.Contains(err.Error(), "fault campaign") || !strings.Contains(err.Error(), "heal refused") {
		t.Fatalf("a campaign that could not heal must fail, got %v", err)
	}
	for _, name := range []string{planFile, faultsFile, historyFile, resultFile} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("a failed campaign must still save %s: %v", name, err)
		}
	}
	// Offline, the failed run stays an error; it never becomes a verdict.
	if _, err := checkRun(dir, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "ended with an error") {
		t.Fatalf("offline check turned a failed campaign into a verdict: %v", err)
	}
}

func TestCampaignAcceptsATrailingSlash(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	if _, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{Timeout: time.Second, Interval: 10 * time.Millisecond}, camp, dir+string(filepath.Separator), "reference"); err != nil {
		t.Fatalf("a run directory spelled with a trailing slash was refused: %v", err)
	}
	if _, err := checkRun(dir, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignRefusesAnExistingRunDirectory(t *testing.T) {
	camp, client, cfg, _ := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	dir := t.TempDir()
	if _, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference"); err == nil {
		t.Fatal("an existing run directory was reused")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a refused run wrote into the existing directory: %v", entries)
	}
}

// Each edit makes one file in a valid run directory disagree with the rest.
func TestCheckRefusesInconsistentRunDirectories(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	if _, _, err := executeCampaign(context.Background(), client, cfg, readbackConfig{Timeout: time.Second, Interval: 10 * time.Millisecond}, camp, dir, "reference"); err != nil {
		t.Fatal(err)
	}
	edits := map[string]struct{ file, old, new string }{
		"result verdict":  {resultFile, `"verdict": "valid"`, `"verdict": "invalid"`},
		"result run":      {resultFile, `"run_id": "run_test"`, `"run_id": "other_run"`},
		"plan seed":       {planFile, `"seed": 1,`, `"seed": 2,`},
		"history linkage": {historyFile, `"fault_seed":1`, `"fault_seed":2`},
	}
	for name, e := range edits {
		t.Run(name, func(t *testing.T) {
			copied := filepath.Join(t.TempDir(), "run")
			if err := os.CopyFS(copied, os.DirFS(dir)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(copied, e.file)
			data, _ := os.ReadFile(path)
			if !bytes.Contains(data, []byte(e.old)) {
				t.Fatalf("%s does not contain %s", e.file, e.old)
			}
			os.WriteFile(path, bytes.Replace(data, []byte(e.old), []byte(e.new), 1), 0o600)
			if code, err := checkRun(copied, &bytes.Buffer{}); err == nil {
				t.Fatalf("an inconsistent run directory checked as exit %d", code)
			}
		})
	}
	// Evidence for the first fault standing in for every fault: the counts
	// still match, but the second planned fault has no evidence.
	t.Run("one fault's evidence twice", func(t *testing.T) {
		copied := filepath.Join(t.TempDir(), "run")
		if err := os.CopyFS(copied, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		ops, err := readOps(filepath.Join(copied, faultsFile))
		if err != nil {
			t.Fatal(err)
		}
		for i, op := range ops {
			if op.F != "fault" {
				continue
			}
			var action fault.ActionRecord
			json.Unmarshal(op.Value, &action)
			action.Injection.Event = camp.plan.Events[0]
			ops[i].Value, _ = json.Marshal(action)
		}
		if err := history.SaveFile(filepath.Join(copied, faultsFile), ops); err != nil {
			t.Fatal(err)
		}
		if code, err := checkRun(copied, &bytes.Buffer{}); err == nil {
			t.Fatalf("duplicated evidence for one fault checked as exit %d", code)
		}
	})
	if err := os.Remove(filepath.Join(dir, faultsFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := checkRun(dir, &bytes.Buffer{}); err == nil {
		t.Fatal("a run directory without its fault history was accepted")
	}
}

func TestWaitReadyWaitsForTheCatalog(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer server.Close()
	probe := rest.NewClient(server.URL, "", "", time.Second)
	if err := waitReady(context.Background(), probe, 5*time.Second); err != nil || calls.Load() != 3 {
		t.Fatalf("readiness: %v after %d calls", err, calls.Load())
	}
	calls.Store(-1000)
	if err := waitReady(context.Background(), probe, 300*time.Millisecond); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("a catalog that never answers must time out, got %v", err)
	}
}

func TestCheckProxiedRefusesDirectCatalogURL(t *testing.T) {
	if err := checkProxied("http://localhost:28181", "0.0.0.0:28181"); err != nil {
		t.Fatal(err)
	}
	if err := checkProxied("http://localhost:8181", "0.0.0.0:28181"); err == nil {
		t.Fatal("a URL that bypasses the proxy would hide every network fault")
	}
}

func kindsOf(r checker.AppendResult) map[string]int {
	out := map[string]int{}
	for _, a := range r.Anomalies {
		out[a.Kind]++
	}
	return out
}

func TestFailedRunResultHoldsNoCatalogText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"token":"response-secret"}`, http.StatusInternalServerError)
	}))
	defer server.Close()
	plan, err := fault.Generate(1, fault.Config{Targets: []string{"reference"}, Kinds: []fault.Kind{fault.Pause},
		RunDuration: time.Second, MinFault: time.Millisecond, MaxFault: time.Millisecond, ActionTimeout: time.Millisecond, CleanupTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "run")
	camp := &campaign{plan: plan, controllers: map[fault.Kind]fault.Controller{}}
	client := rest.NewClient(server.URL, "", "", time.Second)
	_, _, err = executeCampaign(context.Background(), client, workload.AppendConfig{Namespace: "test", Table: "run_test", Clients: 1, PerClient: 1}, readbackConfig{}, camp, dir, "reference")
	if err == nil || !strings.Contains(err.Error(), "response-secret") {
		t.Fatalf("setup against a failing catalog should fail with its response: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, resultFile))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("response-secret")) || !bytes.Contains(data, []byte(`"verdict": "error"`)) {
		t.Fatalf("result.json must record the error without catalog text:\n%s", data)
	}
}
