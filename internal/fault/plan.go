// Package fault generates reproducible fault plans and executes them through
// controllers: Toxiproxy for network faults and Docker for process faults.
package fault

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"reflect"
	"sort"
	"time"
)

// Version identifies both the plan schema and the generation algorithm. Change
// it when random draws, ordering, or scheduling rules change.
const Version = "calve-faults-v1-pcg"

type Kind string

const (
	Latency   Kind = "latency"
	Reset     Kind = "reset"
	Partition Kind = "partition"
	Kill      Kind = "kill"
	Pause     Kind = "pause"
	Restart   Kind = "restart"
)

// Config is normalized before generation. All JSON durations are nanoseconds.
// V1 serializes faults globally, reserving an action-timeout budget after each
// planned heal before any subsequent fault can start.
type Config struct {
	Targets        []string      `json:"targets"`
	Kinds          []Kind        `json:"kinds"`
	RunDuration    time.Duration `json:"run_duration_ns"`
	Warmup         time.Duration `json:"warmup_ns"`
	MinHealthy     time.Duration `json:"min_healthy_ns"`
	MaxHealthy     time.Duration `json:"max_healthy_ns"`
	MinFault       time.Duration `json:"min_fault_ns"`
	MaxFault       time.Duration `json:"max_fault_ns"`
	ActionTimeout  time.Duration `json:"action_timeout_ns"`
	CleanupTimeout time.Duration `json:"cleanup_timeout_ns"`
	Latency        time.Duration `json:"latency_ns"`
	StopGrace      time.Duration `json:"stop_grace_ns"`
}

func DefaultConfig() Config {
	return Config{Targets: []string{"reference"}, Kinds: []Kind{Latency, Reset, Partition, Kill, Pause, Restart}, RunDuration: time.Minute, Warmup: 5 * time.Second, MinHealthy: 2 * time.Second, MaxHealthy: 5 * time.Second, MinFault: time.Second, MaxFault: 3 * time.Second, ActionTimeout: 5 * time.Second, CleanupTimeout: 30 * time.Second, Latency: 250 * time.Millisecond, StopGrace: time.Second}
}

type Parameters struct {
	Direction string        `json:"direction,omitempty"`
	Toxicity  float64       `json:"toxicity,omitempty"`
	Latency   time.Duration `json:"latency_ns"`
	Jitter    time.Duration `json:"jitter_ns"`
	Timeout   time.Duration `json:"timeout_ns"`
	Signal    string        `json:"signal,omitempty"`
	Grace     time.Duration `json:"grace_ns"`
}

type Event struct {
	ID         string        `json:"id"`
	Target     string        `json:"target"`
	Kind       Kind          `json:"kind"`
	Start      time.Duration `json:"start_ns"`
	Duration   time.Duration `json:"duration_ns"`
	HealAt     time.Duration `json:"heal_at_ns"`
	HealAction string        `json:"heal_action"`
	Parameters Parameters    `json:"parameters"`
}

type Plan struct {
	Version string  `json:"version"`
	Seed    int64   `json:"seed"`
	Config  Config  `json:"config"`
	Events  []Event `json:"events"`
}

func normalize(cfg Config) (Config, error) {
	cfg.Targets = append([]string(nil), cfg.Targets...)
	cfg.Kinds = append([]Kind(nil), cfg.Kinds...)
	sort.Strings(cfg.Targets)
	sort.Slice(cfg.Kinds, func(i, j int) bool { return cfg.Kinds[i] < cfg.Kinds[j] })
	if len(cfg.Targets) == 0 || len(cfg.Kinds) == 0 {
		return cfg, errors.New("fault targets and kinds are required")
	}
	for i, target := range cfg.Targets {
		if !identifier(target) || i > 0 && cfg.Targets[i-1] == target {
			return cfg, errors.New("fault targets must be unique logical identifiers (letters, digits, dot, underscore, hyphen; at most 64 characters)")
		}
	}
	for i, kind := range cfg.Kinds {
		if !knownKind(kind) || i > 0 && cfg.Kinds[i-1] == kind {
			return cfg, fmt.Errorf("invalid or duplicate fault kind %q", kind)
		}
		if kind == Latency && cfg.Latency <= 0 {
			return cfg, errors.New("latency faults require a positive delay")
		}
		if kind == Restart && cfg.StopGrace >= cfg.ActionTimeout {
			return cfg, errors.New("restart grace must leave time for observation within action-timeout")
		}
	}
	if cfg.RunDuration <= 0 || cfg.Warmup < 0 || cfg.MinHealthy < 0 || cfg.MaxHealthy < cfg.MinHealthy || cfg.MinFault <= 0 || cfg.MaxFault < cfg.MinFault || cfg.ActionTimeout <= 0 || cfg.CleanupTimeout <= 0 || cfg.Latency < 0 || cfg.StopGrace < 0 {
		return cfg, errors.New("invalid fault timing bounds")
	}
	// Subtract budgets rather than overflowing sums of user-supplied durations.
	remaining := cfg.RunDuration
	for _, budget := range []time.Duration{cfg.Warmup, cfg.MaxHealthy, cfg.MaxFault, cfg.ActionTimeout} {
		if budget > remaining {
			return cfg, errors.New("run duration cannot fit warmup, one fault, and its healing budget")
		}
		remaining -= budget
	}
	return cfg, nil
}

func knownKind(kind Kind) bool {
	switch kind {
	case Latency, Reset, Partition, Kill, Pause, Restart:
		return true
	}
	return false
}

// Generate uses only a private RNG and normalized configuration. It never reads
// the clock, workload randomness, container state, or map iteration order.
func Generate(seed int64, cfg Config) (Plan, error) {
	cfg, err := normalize(cfg)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{Version: Version, Seed: seed, Config: cfg}
	rng := rand.New(rand.NewPCG(uint64(seed), 0x63616c76652d7631))
	draw := func(low, high time.Duration) time.Duration {
		if low == high {
			return low
		}
		return low + time.Duration(rng.Int64N(int64(high-low)+1))
	}
	cursor := cfg.Warmup
	for {
		gap, duration := draw(cfg.MinHealthy, cfg.MaxHealthy), draw(cfg.MinFault, cfg.MaxFault)
		remaining := cfg.RunDuration - cursor
		if gap > remaining {
			break
		}
		remaining -= gap
		if duration > remaining {
			break
		}
		remaining -= duration
		if cfg.ActionTimeout > remaining {
			break
		}
		start := cursor + gap
		event := Event{ID: fmt.Sprintf("fault-%06d", len(plan.Events)+1), Target: cfg.Targets[rng.Int64N(int64(len(cfg.Targets)))], Kind: cfg.Kinds[rng.Int64N(int64(len(cfg.Kinds)))], Start: start, Duration: duration, HealAt: start + duration}
		switch event.Kind {
		case Latency, Reset:
			event.Parameters.Direction = []string{"upstream", "downstream"}[rng.Int64N(2)]
			event.Parameters.Toxicity = 1
			event.HealAction = "remove-toxic"
			if event.Kind == Latency {
				event.Parameters.Latency = cfg.Latency
			}
		case Partition:
			event.Parameters.Direction = "both"
			event.Parameters.Toxicity = 1
			event.HealAction = "remove-toxics"
		case Kill:
			event.Parameters.Signal = "SIGKILL"
			event.HealAction = "start"
		case Pause:
			event.HealAction = "unpause"
		case Restart:
			event.Parameters.Signal = "SIGTERM"
			event.Parameters.Grace = cfg.StopGrace
			event.HealAction = "start"
		}
		plan.Events = append(plan.Events, event)
		if len(plan.Events) > 10000 {
			return Plan{}, errors.New("fault plan exceeds 10000 events; increase spacing or shorten the run")
		}
		cursor = event.HealAt + cfg.ActionTimeout
	}
	if len(plan.Events) == 0 {
		return Plan{}, errors.New("no fault fits the run duration")
	}
	return plan, nil
}

// Validate rejects edited, unsupported, or incomplete plans by regenerating
// them. Replay uses the saved plan, with the same algorithm version.
func (p Plan) Validate() error {
	if p.Version != Version {
		return fmt.Errorf("unsupported fault plan version %q", p.Version)
	}
	expected, err := Generate(p.Seed, p.Config)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(p, expected) {
		return errors.New("fault plan does not match its seed and normalized configuration")
	}
	return nil
}

// Save creates a new artifact, avoiding accidental replacement of a prior run.
// Sync/close errors are returned before a caller may begin executing faults.
func Save(path string, plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(plan)
	return errors.Join(writeErr, f.Sync(), f.Close())
}

func Read(r io.Reader) (Plan, error) {
	var plan Plan
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return plan, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return plan, errors.New("unexpected data after fault plan")
	}
	return plan, plan.Validate()
}
