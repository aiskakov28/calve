package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

// fakeToxiproxy implements the slice of the Toxiproxy API the controller uses.
type fakeToxiproxy struct {
	mu       sync.Mutex
	disabled bool
	toxics   []toxic
}

func (f *fakeToxiproxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "proxies" || parts[1] != "reference" {
		http.Error(w, `{"error":"proxy not found"}`, http.StatusNotFound)
		return
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 2:
		json.NewEncoder(w).Encode(map[string]any{"name": "reference", "enabled": !f.disabled, "toxics": f.toxics})
	case r.Method == http.MethodPost && len(parts) == 3:
		var tx toxic
		json.NewDecoder(r.Body).Decode(&tx)
		for _, existing := range f.toxics {
			if existing.Name == tx.Name {
				http.Error(w, `{"error":"toxic already exists"}`, http.StatusConflict)
				return
			}
		}
		f.toxics = append(f.toxics, tx)
		json.NewEncoder(w).Encode(tx)
	case r.Method == http.MethodDelete && len(parts) == 4:
		for i, existing := range f.toxics {
			if existing.Name == parts[3] {
				f.toxics = append(f.toxics[:i], f.toxics[i+1:]...)
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		http.Error(w, `{"error":"toxic not found"}`, http.StatusNotFound)
	default:
		http.Error(w, "unexpected", http.StatusBadRequest)
	}
}

func (f *fakeToxiproxy) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, tx := range f.toxics {
		out = append(out, tx.Name)
	}
	return out
}

func networkInjection(kind Kind, direction string) Injection {
	p := Parameters{Direction: direction, Toxicity: 1}
	if kind == Latency {
		p.Latency = 300 * time.Millisecond
	}
	return Injection{RunID: "run-1", Event: Event{ID: "f001", Target: "reference", Kind: kind, Parameters: p}}
}

func fakeToxiproxyController(t *testing.T) (*Toxiproxy, *fakeToxiproxy) {
	t.Helper()
	fake := &fakeToxiproxy{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	return NewToxiproxy(server.URL, map[string]string{"reference": "reference"}), fake
}

func mustObserve(t *testing.T, c *Toxiproxy, inj Injection, want State) {
	t.Helper()
	got, err := c.Observe(context.Background(), inj)
	if err != nil || got != want {
		t.Fatalf("observe %s: want %s, got %s (%v)", inj.Event.Kind, want, got, err)
	}
}

func TestToxiproxyApplyObserveHeal(t *testing.T) {
	ctx := context.Background()
	for _, inj := range []Injection{
		networkInjection(Latency, "upstream"), networkInjection(Latency, "downstream"),
		networkInjection(Reset, "upstream"), networkInjection(Reset, "downstream"),
		networkInjection(Partition, "both"),
	} {
		t.Run(string(inj.Event.Kind)+"-"+inj.Event.Parameters.Direction, func(t *testing.T) {
			c, fake := fakeToxiproxyController(t)
			mustObserve(t, c, inj, Healthy)
			if err := c.Apply(ctx, inj); err != nil {
				t.Fatal(err)
			}
			mustObserve(t, c, inj, Active)
			if inj.Event.Kind == Partition && len(fake.names()) != 2 {
				t.Fatalf("partition must block both streams, got %v", fake.names())
			}
			// A repeated apply (after an uncertain first one) must not fail or duplicate.
			if err := c.Apply(ctx, inj); err != nil {
				t.Fatal(err)
			}
			mustObserve(t, c, inj, Active)
			if err := c.Heal(ctx, inj); err != nil {
				t.Fatal(err)
			}
			mustObserve(t, c, inj, Healthy)
			if err := c.Heal(ctx, inj); err != nil {
				t.Fatalf("heal must be idempotent: %v", err)
			}
		})
	}
}

func TestToxiproxyHealNeverAppliedAndOnlyOwnToxics(t *testing.T) {
	c, fake := fakeToxiproxyController(t)
	other := networkInjection(Latency, "upstream")
	other.RunID = "other-run"
	if err := c.Apply(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	inj := networkInjection(Partition, "both")
	if err := c.Heal(context.Background(), inj); err != nil {
		t.Fatalf("healing a never-applied fault must be a no-op: %v", err)
	}
	if names := fake.names(); len(names) != 1 || names[0] != other.ObjectName() {
		t.Fatalf("heal touched another run's toxic: %v", names)
	}
}

func TestToxiproxyObserveUnknownStates(t *testing.T) {
	ctx := context.Background()
	t.Run("partial partition", func(t *testing.T) {
		c, fake := fakeToxiproxyController(t)
		inj := networkInjection(Partition, "both")
		c.Apply(ctx, inj)
		fake.mu.Lock()
		fake.toxics = fake.toxics[:1]
		fake.mu.Unlock()
		if state, err := c.Observe(ctx, inj); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
	t.Run("toxic differs from plan", func(t *testing.T) {
		c, fake := fakeToxiproxyController(t)
		inj := networkInjection(Latency, "downstream")
		c.Apply(ctx, inj)
		fake.mu.Lock()
		fake.toxics[0].Attributes["latency"] = 5
		fake.mu.Unlock()
		if state, err := c.Observe(ctx, inj); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
	t.Run("disabled proxy", func(t *testing.T) {
		c, fake := fakeToxiproxyController(t)
		fake.disabled = true
		if state, err := c.Observe(ctx, networkInjection(Reset, "upstream")); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
}

func TestToxiproxyRejectsInvalidInjections(t *testing.T) {
	c, fake := fakeToxiproxyController(t)
	bad := map[string]Injection{}
	bad["process fault"] = networkInjection(Kill, "")
	inj := networkInjection(Latency, "upstream")
	inj.Event.Parameters.Jitter = time.Millisecond
	bad["jitter"] = inj
	inj = networkInjection(Latency, "upstream")
	inj.Event.Parameters.Latency = 1500 * time.Microsecond
	bad["fractional ms"] = inj
	inj = networkInjection(Reset, "upstream")
	inj.Event.Parameters.Toxicity = 0.5
	bad["toxicity"] = inj
	bad["direction"] = networkInjection(Reset, "both")
	bad["partition direction"] = networkInjection(Partition, "upstream")
	inj = networkInjection(Reset, "upstream")
	inj.Event.Target = "unknown"
	bad["target"] = inj
	for name, inj := range bad {
		if err := c.Apply(context.Background(), inj); err == nil {
			t.Errorf("%s: apply accepted an invalid injection", name)
		}
	}
	if len(fake.names()) != 0 {
		t.Fatalf("invalid injections created toxics: %v", fake.names())
	}
}

// The tests below need the compose cluster: docker compose up -d --wait.
// CALVE_TOXIPROXY is the API (http://localhost:8474); CALVE_TOXIPROXY_CATALOG
// is the reference catalog through its proxy (default http://localhost:28181).

func liveToxiproxy(t *testing.T) (*Toxiproxy, string) {
	t.Helper()
	api := os.Getenv("CALVE_TOXIPROXY")
	if api == "" {
		t.Skip("set CALVE_TOXIPROXY to test against a running Toxiproxy")
	}
	catalog := os.Getenv("CALVE_TOXIPROXY_CATALOG")
	if catalog == "" {
		catalog = "http://localhost:28181"
	}
	return NewToxiproxy(api, map[string]string{"reference": "reference"}), catalog
}

// probe sends one fresh-connection request through the proxy.
func probe(catalog string, timeout time.Duration) (time.Duration, error) {
	client := &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
	start := time.Now()
	resp, err := client.Get(catalog + "/v1/config")
	if err != nil {
		return time.Since(start), err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Since(start), fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return time.Since(start), nil
}

func TestToxiproxyLiveEffects(t *testing.T) {
	c, catalog := liveToxiproxy(t)
	ctx := context.Background()
	if _, err := probe(catalog, 2*time.Second); err != nil {
		t.Fatalf("catalog not reachable through the proxy before any fault: %v", err)
	}
	cases := []struct {
		inj    Injection
		effect func(time.Duration, error) error
	}{
		{networkInjection(Latency, "upstream"), delayed},
		{networkInjection(Latency, "downstream"), delayed},
		{networkInjection(Reset, "upstream"), failed},
		{networkInjection(Reset, "downstream"), failed},
		{networkInjection(Partition, "both"), timedOut},
	}
	for i, tc := range cases {
		tc.inj.RunID = fmt.Sprintf("live-%d-%d", time.Now().UnixNano(), i)
		t.Run(string(tc.inj.Event.Kind)+"-"+tc.inj.Event.Parameters.Direction, func(t *testing.T) {
			t.Cleanup(func() { c.Heal(context.Background(), tc.inj) })
			if err := c.Apply(ctx, tc.inj); err != nil {
				t.Fatal(err)
			}
			mustObserve(t, c, tc.inj, Active)
			if err := tc.effect(probe(catalog, time.Second)); err != nil {
				t.Fatalf("fault had no visible effect: %v", err)
			}
			if err := c.Heal(ctx, tc.inj); err != nil {
				t.Fatal(err)
			}
			mustObserve(t, c, tc.inj, Healthy)
			if elapsed, err := probe(catalog, 2*time.Second); err != nil || elapsed >= 300*time.Millisecond {
				t.Fatalf("connectivity not restored after heal: %s, %v", elapsed, err)
			}
		})
	}
}

func delayed(elapsed time.Duration, err error) error {
	if err != nil || elapsed < 300*time.Millisecond {
		return fmt.Errorf("want a successful request taking >= 300ms, got %s, %v", elapsed, err)
	}
	return nil
}

func failed(_ time.Duration, err error) error {
	if err == nil {
		return errors.New("request succeeded through a reset toxic")
	}
	return nil
}

func timedOut(elapsed time.Duration, err error) error {
	if err == nil || elapsed < 900*time.Millisecond {
		return fmt.Errorf("want the request to hang until the 1s timeout, got %s, %v", elapsed, err)
	}
	return nil
}

func TestToxiproxyLiveWithExecutor(t *testing.T) {
	c, catalog := liveToxiproxy(t)
	cfg := Config{Targets: []string{"reference"}, Kinds: []Kind{Latency, Partition, Reset},
		RunDuration: 12 * time.Second, Warmup: 500 * time.Millisecond, MinHealthy: 200 * time.Millisecond,
		MaxHealthy: 500 * time.Millisecond, MinFault: 500 * time.Millisecond, MaxFault: time.Second,
		ActionTimeout: 2 * time.Second, CleanupTimeout: 10 * time.Second, Latency: 300 * time.Millisecond}
	plan, err := Generate(7, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := Execute(context.Background(), plan, ExecuteOptions{
		RunID: fmt.Sprintf("exec-%d", time.Now().UnixNano()), PlanPath: filepath.Join(dir, "plan.json"),
		HistoryPath: filepath.Join(dir, "history.jsonl"), Recorder: history.NewRecorder(),
		Controllers: map[Kind]Controller{Latency: c, Reset: c, Partition: c},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed || res.Applied == 0 || res.Healed != res.Applied {
		t.Fatalf("unexpected execution result: %+v", res)
	}
	if _, err := probe(catalog, 2*time.Second); err != nil {
		t.Fatalf("catalog unreachable after the executor cleaned up: %v", err)
	}
	t.Logf("applied and healed %d of %d planned faults", res.Applied, len(plan.Events))
}
