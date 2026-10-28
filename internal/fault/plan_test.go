package fault

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func testConfig() Config {
	c := DefaultConfig()
	c.Targets = []string{"nessie", "reference", "lakekeeper"}
	c.RunDuration = 2 * time.Minute
	return c
}

func TestPlanDeterministicAndIndependent(t *testing.T) {
	cfg := testConfig()
	original := append([]string(nil), cfg.Targets...)
	a, err := Generate(-21, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		rand.Uint64()
	} // Unrelated workload/global randomness.
	b, err := Generate(-21, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(cfg.Targets, original) {
		t.Fatal("schedule changed or caller configuration was mutated")
	}
	cfg.Targets = []string{"lakekeeper", "reference", "nessie"}
	cfg.Kinds = []Kind{Restart, Pause, Kill, Partition, Reset, Latency}
	b, _ = Generate(-21, cfg)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("input order changed the normalized schedule")
	}
	c, _ := Generate(-22, cfg)
	if reflect.DeepEqual(a.Events, c.Events) {
		t.Fatal("changing seed did not change this schedule")
	}
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(a)
	reloaded, err := Read(&buf)
	if err != nil || !reflect.DeepEqual(a, reloaded) {
		t.Fatalf("plan round trip changed: %v", err)
	}
}

func TestPlanBoundsAndMechanisms(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		plan, err := Generate(seed, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		ids := map[string]bool{}
		previous := plan.Config.Warmup
		for _, event := range plan.Events {
			if ids[event.ID] || event.Start < previous+plan.Config.MinHealthy || event.Start > previous+plan.Config.MaxHealthy || event.Duration < plan.Config.MinFault || event.Duration > plan.Config.MaxFault || event.HealAt != event.Start+event.Duration || event.HealAt+plan.Config.ActionTimeout > plan.Config.RunDuration {
				t.Fatalf("invalid timing/identity: %+v", event)
			}
			ids[event.ID] = true
			if event.Kind == Latency || event.Kind == Reset {
				if event.Parameters.Direction != "upstream" && event.Parameters.Direction != "downstream" || event.Parameters.Toxicity != 1 {
					t.Fatal("invalid network parameters")
				}
			}
			if event.Kind == Latency && (event.Parameters.Latency != plan.Config.Latency || event.Parameters.Jitter != 0) {
				t.Fatal("hidden latency randomness")
			}
			if event.Kind == Partition && (event.Parameters.Direction != "both" || event.Parameters.Timeout != 0 || event.Parameters.Toxicity != 1) {
				t.Fatal("partition is not blocked traffic on both streams")
			}
			if event.Kind == Kill && event.Parameters.Signal != "SIGKILL" {
				t.Fatal("kill changed to a graceful stop")
			}
			if event.Kind == Restart && (event.Parameters.Signal != "SIGTERM" || event.Parameters.Grace != plan.Config.StopGrace) {
				t.Fatal("restart lacks explicit stop parameters")
			}
			previous = event.HealAt + plan.Config.ActionTimeout
		}
	}
}

func TestPlanGolden(t *testing.T) {
	plan, err := Generate(42, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(plan)
	hash := sha256.Sum256(data)
	got := hex.EncodeToString(hash[:])
	const want = "52441df09cc755c6e8ccd2e4adb686e9f96e3e2b75f6b19ffdf9f9984fc29c40"
	if got != want {
		t.Fatalf("plan algorithm changed without updating its version/fixture: %s", got)
	}
}

func TestInvalidPlansAndConfigs(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Targets = nil }, func(c *Config) { c.Targets = []string{"a", "a"} }, func(c *Config) { c.Targets = []string{"http://user:secret@host"} },
		func(c *Config) { c.Kinds = []Kind{"unknown"} }, func(c *Config) { c.Kinds = []Kind{Pause, Pause} }, func(c *Config) { c.MinFault = 0 }, func(c *Config) { c.RunDuration = time.Second }, func(c *Config) { c.Warmup = -1 }, func(c *Config) { c.MinHealthy = c.MaxHealthy + 1 }, func(c *Config) { c.ActionTimeout = 0 }, func(c *Config) { c.CleanupTimeout = 0 }, func(c *Config) { c.Latency = 0 }, func(c *Config) { c.StopGrace = c.ActionTimeout },
	} {
		cfg := testConfig()
		change(&cfg)
		if _, err := Generate(1, cfg); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", cfg)
		}
	}
	plan, _ := Generate(1, testConfig())
	plan.Events[0].Duration++
	if plan.Validate() == nil {
		t.Fatal("edited plan accepted")
	}
	plan.Version = "unknown"
	if plan.Validate() == nil {
		t.Fatal("future plan version accepted")
	}
	if _, err := Read(bytes.NewBufferString(`{"version":"unknown","extra":true}`)); err == nil {
		t.Fatal("unknown fields/version accepted")
	}
	plan, _ = Generate(1, testConfig())
	data, _ := json.Marshal(plan)
	data = append(data, []byte(" {}")...)
	if _, err := Read(bytes.NewReader(data)); err == nil {
		t.Fatal("trailing data accepted")
	}
	cfg := testConfig()
	cfg.RunDuration = time.Second
	cfg.Warmup = 0
	cfg.MinHealthy = 0
	cfg.MaxHealthy = 0
	cfg.MinFault = 1
	cfg.MaxFault = 1
	cfg.ActionTimeout = 1
	cfg.Kinds = []Kind{Pause}
	if _, err := Generate(1, cfg); err == nil {
		t.Fatal("unbounded dense plan accepted")
	}
}

func TestSaveDoesNotReplaceArtifact(t *testing.T) {
	plan, _ := Generate(3, testConfig())
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := Save(path, plan); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	other, _ := Generate(4, testConfig())
	if err := Save(path, other); err == nil {
		t.Fatal("prior artifact overwritten")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed save changed artifact")
	}
}
