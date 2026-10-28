package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// fakeDocker is a container state machine behind the docker CLI arguments
// the controller uses.
type fakeDocker struct {
	mu     sync.Mutex
	state  containerState
	policy string
	calls  []string
}

func (f *fakeDocker) run(_ context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, args[0])
	st := &f.state
	switch args[0] {
	case "inspect":
		data, _ := json.Marshal(st)
		return []byte(string(data) + "|" + f.policy + "\n"), nil
	case "kill":
		st.Running, st.Paused, st.ExitCode = false, false, 137
	case "pause":
		if !st.Running || st.Paused {
			return nil, errors.New("container is not running or already paused")
		}
		st.Paused = true
	case "unpause":
		if !st.Paused {
			return nil, errors.New("container is not paused")
		}
		st.Paused = false
	case "stop":
		st.Running, st.Paused, st.ExitCode = false, false, 143
	case "start":
		st.Running, st.ExitCode = true, 0
	default:
		return nil, fmt.Errorf("unexpected docker %s", args[0])
	}
	return nil, nil
}

func processInjection(kind Kind) Injection {
	p := Parameters{}
	switch kind {
	case Kill:
		p.Signal = "SIGKILL"
	case Restart:
		p.Signal, p.Grace = "SIGTERM", time.Second
	}
	return Injection{RunID: "run-1", Event: Event{ID: "f001", Target: "reference", Kind: kind, Parameters: p}}
}

func fakeDockerController() (*Docker, *fakeDocker) {
	fake := &fakeDocker{state: containerState{Running: true}, policy: "no"}
	d := NewDocker(map[string]string{"reference": "calve-rest-fixture-1"})
	d.run = fake.run
	return d, fake
}

func mustObserveDocker(t *testing.T, d *Docker, inj Injection, want State) {
	t.Helper()
	got, err := d.Observe(context.Background(), inj)
	if err != nil || got != want {
		t.Fatalf("observe %s: want %s, got %s (%v)", inj.Event.Kind, want, got, err)
	}
}

func TestDockerApplyObserveHeal(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []Kind{Kill, Pause, Restart} {
		t.Run(string(kind), func(t *testing.T) {
			d, _ := fakeDockerController()
			inj := processInjection(kind)
			mustObserveDocker(t, d, inj, Healthy)
			if err := d.Apply(ctx, inj); err != nil {
				t.Fatal(err)
			}
			mustObserveDocker(t, d, inj, Active)
			if err := d.Heal(ctx, inj); err != nil {
				t.Fatal(err)
			}
			mustObserveDocker(t, d, inj, Healthy)
			if err := d.Heal(ctx, inj); err != nil {
				t.Fatalf("heal must be idempotent: %v", err)
			}
		})
	}
}

func TestDockerHealNeverAppliedIsNoOp(t *testing.T) {
	d, fake := fakeDockerController()
	if err := d.Heal(context.Background(), processInjection(Pause)); err != nil {
		t.Fatal(err)
	}
	for _, call := range fake.calls {
		if call != "inspect" {
			t.Fatalf("healing a running container issued docker %s", call)
		}
	}
}

func TestDockerObserveUnknownStates(t *testing.T) {
	ctx := context.Background()
	t.Run("restart policy", func(t *testing.T) {
		d, fake := fakeDockerController()
		fake.policy = "unless-stopped"
		if state, err := d.Observe(ctx, processInjection(Kill)); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
	t.Run("stopped without SIGKILL is not a kill", func(t *testing.T) {
		d, fake := fakeDockerController()
		fake.state = containerState{ExitCode: 143}
		if state, err := d.Observe(ctx, processInjection(Kill)); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
	t.Run("stopped is not a pause", func(t *testing.T) {
		d, fake := fakeDockerController()
		fake.state = containerState{ExitCode: 137}
		if state, err := d.Observe(ctx, processInjection(Pause)); state != Unknown || err == nil {
			t.Fatalf("want unknown with error, got %s (%v)", state, err)
		}
	})
}

func TestDockerHealRepairsAnyFaultState(t *testing.T) {
	// After an uncertain apply the container may be paused or stopped either way.
	for name, st := range map[string]containerState{
		"paused":  {Running: true, Paused: true},
		"killed":  {ExitCode: 137},
		"stopped": {ExitCode: 143},
	} {
		d, fake := fakeDockerController()
		fake.state = st
		if err := d.Heal(context.Background(), processInjection(Restart)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		mustObserveDocker(t, d, processInjection(Restart), Healthy)
	}
}

func TestDockerRejectsInvalidInjections(t *testing.T) {
	d, fake := fakeDockerController()
	bad := map[string]Injection{}
	bad["network fault"] = networkInjection(Latency, "upstream")
	inj := processInjection(Kill)
	inj.Event.Parameters.Signal = "SIGTERM"
	bad["kill signal"] = inj
	inj = processInjection(Restart)
	inj.Event.Parameters.Grace = 1500 * time.Millisecond
	bad["fractional grace"] = inj
	inj = processInjection(Pause)
	inj.Event.Target = "unknown"
	bad["target"] = inj
	for name, inj := range bad {
		if err := d.Apply(context.Background(), inj); err == nil {
			t.Errorf("%s: apply accepted an invalid injection", name)
		}
	}
	if len(fake.calls) != 0 {
		t.Fatalf("invalid injections ran docker: %v", fake.calls)
	}
}

// The tests below kill, pause and restart the compose catalogs; they need
// docker compose up -d --wait and CALVE_DOCKER_FAULTS=1.

type liveCatalog struct {
	target, service, url, warehouse string
}

var liveCatalogs = []liveCatalog{
	{"reference", "rest-fixture", "http://localhost:8181", ""},
	{"lakekeeper", "lakekeeper", "http://localhost:8182/catalog", "calve"},
	{"nessie", "nessie", "http://localhost:8183/iceberg", ""},
}

func liveDocker(t *testing.T) *Docker {
	t.Helper()
	if os.Getenv("CALVE_DOCKER_FAULTS") == "" {
		t.Skip("set CALVE_DOCKER_FAULTS=1 to kill, pause and restart the compose catalogs")
	}
	containers := map[string]string{}
	for _, c := range liveCatalogs {
		out, err := exec.Command("docker", "compose", "-f", "../../docker-compose.yml", "ps", "-q", c.service).Output()
		id := strings.TrimSpace(string(out))
		if err != nil || id == "" {
			t.Fatalf("no running container for %s: %v", c.service, err)
		}
		containers[c.target] = id
	}
	return NewDocker(containers)
}

// markTable creates a table with a unique property, so survival can be checked.
func markTable(t *testing.T, c liveCatalog) (*rest.Client, string) {
	t.Helper()
	ctx := context.Background()
	client := rest.NewClient(c.url, c.warehouse, "", 5*time.Second)
	if _, err := client.Config(ctx); err != nil {
		t.Fatal(err)
	}
	if r := client.CreateNamespace(ctx, "calve"); r.Outcome() != "ok" && r.Status != 409 {
		t.Fatalf("create namespace: %s", rest.Describe(r))
	}
	table := fmt.Sprintf("survive_%d", time.Now().UnixNano())
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Required: true, Type: "long"}}}
	if r := client.CreateTable(ctx, "calve", table, schema); r.Outcome() != "ok" {
		t.Fatalf("create table: %s", rest.Describe(r))
	}
	change := rest.TableChange{Requirements: []map[string]any{}, Updates: []map[string]any{{"action": "set-properties", "updates": map[string]string{"calve.survive": table}}}}
	if r := client.CommitTable(ctx, "calve", table, change); r.Outcome() != "ok" {
		t.Fatalf("commit marker: %s", rest.Describe(r))
	}
	return client, table
}

func checkSurvived(t *testing.T, client *rest.Client, table string) {
	t.Helper()
	meta, err := rest.ParseMetadata(client.LoadTable(context.Background(), "calve", table))
	if err != nil || meta.Properties["calve.survive"] != table {
		t.Fatalf("table %s did not survive the fault: %v", table, err)
	}
}

// configProbe makes one fresh-connection config request.
func configProbe(c liveCatalog, timeout time.Duration) error {
	u := c.url + "/v1/config"
	if c.warehouse != "" {
		u += "?warehouse=" + c.warehouse
	}
	client := &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func waitReady(t *testing.T, c liveCatalog) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < 90*time.Second {
		if configProbe(c, 2*time.Second) == nil {
			return time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s not ready 90s after heal", c.target)
	return 0
}

func TestDockerLiveEffectsAndSurvival(t *testing.T) {
	d := liveDocker(t)
	ctx := context.Background()
	for _, c := range liveCatalogs {
		t.Run(c.target, func(t *testing.T) {
			client, table := markTable(t, c)
			for _, kind := range []Kind{Kill, Pause, Restart} {
				inj := processInjection(kind)
				inj.Event.Target = c.target
				inj.RunID = fmt.Sprintf("live-%d", time.Now().UnixNano())
				t.Cleanup(func() { d.Heal(context.Background(), inj) })
				if err := d.Apply(ctx, inj); err != nil {
					t.Fatal(err)
				}
				mustObserveDocker(t, d, inj, Active)
				if err := configProbe(c, time.Second); err == nil {
					t.Fatalf("%s: catalog still answered during the fault", kind)
				}
				if err := d.Heal(ctx, inj); err != nil {
					t.Fatal(err)
				}
				mustObserveDocker(t, d, inj, Healthy)
				ready := waitReady(t, c)
				checkSurvived(t, client, table)
				t.Logf("%s: ready %s after heal, table survived", kind, ready.Round(100*time.Millisecond))
			}
		})
	}
}

func TestDockerLiveWithExecutor(t *testing.T) {
	d := liveDocker(t)
	c := liveCatalogs[1] // Lakekeeper restarts fastest.
	client, table := markTable(t, c)
	cfg := Config{Targets: []string{c.target}, Kinds: []Kind{Kill, Pause, Restart},
		RunDuration: 20 * time.Second, Warmup: 500 * time.Millisecond, MinHealthy: time.Second,
		MaxHealthy: 2 * time.Second, MinFault: 500 * time.Millisecond, MaxFault: time.Second,
		ActionTimeout: 5 * time.Second, CleanupTimeout: 20 * time.Second, StopGrace: time.Second}
	plan, err := Generate(11, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := Execute(context.Background(), plan, ExecuteOptions{
		RunID: fmt.Sprintf("exec-%d", time.Now().UnixNano()), PlanPath: filepath.Join(dir, "plan.json"),
		HistoryPath: filepath.Join(dir, "history.jsonl"), Recorder: history.NewRecorder(),
		Controllers: map[Kind]Controller{Kill: d, Pause: d, Restart: d},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed || res.Applied == 0 || res.Healed != res.Applied {
		t.Fatalf("unexpected execution result: %+v", res)
	}
	waitReady(t, c)
	checkSurvived(t, client, table)
	t.Logf("applied and healed %d of %d planned faults; table survived", res.Applied, len(plan.Events))
}
