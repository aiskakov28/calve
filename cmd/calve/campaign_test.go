package main

import (
	"context"
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
	dir := t.TempDir()
	controller := &modeController{front: front, mode: mode, healErr: healErr}
	camp := &campaign{
		plan: plan, controllers: map[fault.Kind]fault.Controller{fault.Pause: controller},
		planPath: filepath.Join(dir, "plan.json"), faultsPath: filepath.Join(dir, "faults.jsonl"),
		probe: rest.NewClient(server.URL, "", "", time.Second), readyTimeout: time.Second,
	}
	appendCfg := workload.AppendConfig{Namespace: "test", Table: "run_test", Clients: 2, PerClient: 50, Interval: 20 * time.Millisecond}
	return camp, rest.NewClient(server.URL, "", "", time.Second), appendCfg, filepath.Join(dir, "history.jsonl")
}

func TestCampaignResolvesUnknownCommitsAfterHealing(t *testing.T) {
	camp, client, cfg, out := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	res, _, err := executeAppendWith(context.Background(), client, cfg, out, readbackConfig{Timeout: time.Second, Interval: 10 * time.Millisecond}, camp)
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != checker.Valid || res.Unknown == 0 || res.ResolvedCommitted != res.Unknown {
		t.Fatalf("faults should leave unknowns that resolve as committed: %+v", res)
	}
	if n := len(camp.plan.Events); n == 0 || camp.result.Applied != n || camp.result.Healed != n {
		t.Fatalf("not every planned fault was applied and healed: %+v of %d", camp.result, n)
	}
	for _, path := range []string{camp.planPath, camp.faultsPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("campaign artifact missing: %v", err)
		}
	}
	// The workload history alone still re-checks offline to the same verdict.
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ops, err := history.ReadJSONL(f)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := checker.CheckAppendHistory(ops); err != nil || again.Verdict != checker.Valid {
		t.Fatalf("offline recheck disagrees: %+v; %v", again, err)
	}
}

func TestCampaignStillReportsLostAppendsBesideUnknowns(t *testing.T) {
	camp, client, cfg, out := testCampaign(t, fakecatalog.Bugs{LoseEvery: 7}, down, nil)
	res, _, err := executeAppendWith(context.Background(), client, cfg, out, readbackConfig{Timeout: 200 * time.Millisecond, Interval: 10 * time.Millisecond}, camp)
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != checker.Invalid || kindsOf(res)["lost"] == 0 || res.Unresolved == 0 {
		t.Fatalf("lost appends were hidden by unresolved faulted appends: %+v", res)
	}
}

func TestCampaignFailedHealIsAnErrorNotAVerdict(t *testing.T) {
	camp, client, cfg, out := testCampaign(t, fakecatalog.Bugs{}, down, errors.New("heal refused"))
	_, _, err := executeAppendWith(context.Background(), client, cfg, out, readbackConfig{}, camp)
	if err == nil || !strings.Contains(err.Error(), "fault campaign") || !strings.Contains(err.Error(), "heal refused") {
		t.Fatalf("a campaign that could not heal must fail, got %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("workload history must be saved on a failed campaign: %v", err)
	}
	if _, err := os.Stat(camp.faultsPath); err != nil {
		t.Fatalf("fault history must be saved on a failed campaign: %v", err)
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
