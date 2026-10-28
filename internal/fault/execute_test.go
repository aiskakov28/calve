package fault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

type fakeClock struct {
	now  time.Time
	wait func(time.Time)
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) WaitUntil(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.wait != nil {
		c.wait(at)
	}
	if at.After(c.now) {
		c.now = at
	}
	return ctx.Err()
}

type fakeController struct {
	states  map[string]State
	applied []Injection
	healed  []Injection
	apply   func(context.Context, Injection) error
	heal    func(context.Context, Injection) error
	observe func(context.Context, Injection) (State, error)
}

func (c *fakeController) Apply(ctx context.Context, i Injection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.applied = append(c.applied, i)
	if c.apply != nil {
		return c.apply(ctx, i)
	}
	c.states[i.ObjectName()] = Active
	return nil
}
func (c *fakeController) Heal(ctx context.Context, i Injection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.healed = append(c.healed, i)
	if c.heal != nil {
		return c.heal(ctx, i)
	}
	delete(c.states, i.ObjectName())
	return nil
}
func (c *fakeController) Observe(ctx context.Context, i Injection) (State, error) {
	if err := ctx.Err(); err != nil {
		return Unknown, err
	}
	if c.observe != nil {
		return c.observe(ctx, i)
	}
	if s, ok := c.states[i.ObjectName()]; ok {
		return s, nil
	}
	return Healthy, nil
}

func executionFixture(t *testing.T) (Plan, ExecuteOptions, *fakeController, *fakeClock) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Kinds = []Kind{Pause}
	cfg.RunDuration = 30 * time.Second
	cfg.Warmup = time.Second
	cfg.MinHealthy = time.Second
	cfg.MaxHealthy = time.Second
	cfg.MinFault = 2 * time.Second
	cfg.MaxFault = 2 * time.Second
	cfg.ActionTimeout = time.Second
	cfg.StopGrace = 0
	plan, err := Generate(42, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	controller := &fakeController{states: map[string]State{}}
	clock := &fakeClock{now: time.Now()}
	opts := ExecuteOptions{RunID: "run-1", PlanPath: filepath.Join(dir, "plan.json"), HistoryPath: filepath.Join(dir, "history.jsonl"), Recorder: history.NewRecorder(), Controllers: map[Kind]Controller{Pause: controller}}
	return plan, opts, controller, clock
}

func savedOps(t *testing.T, path string) []history.Op {
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

func TestExecutePersistsPlanBeforeLoadAndControllers(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	checkPlan := func() {
		f, err := os.Open(opts.PlanPath)
		if err != nil {
			t.Fatal("controller/load called before plan saved")
		}
		defer f.Close()
		got, err := Read(f)
		if err != nil || len(got.Events) != len(plan.Events) {
			t.Fatal("saved plan is incomplete")
		}
	}
	c.observe = func(ctx context.Context, i Injection) (State, error) {
		checkPlan()
		if s, ok := c.states[i.ObjectName()]; ok {
			return s, nil
		}
		return Healthy, nil
	}
	var loadCtx context.Context
	opts.OnStart = func(ctx context.Context) { checkPlan(); loadCtx = ctx }
	res, err := execute(context.Background(), plan, opts, clock)
	if err != nil || !res.Completed || res.Applied != len(plan.Events) || res.Healed != len(plan.Events) || res.CleanupAttempts != len(plan.Events) || len(c.states) != 0 {
		t.Fatalf("execution: %+v %v", res, err)
	}
	if loadCtx == nil || loadCtx.Err() == nil {
		t.Fatal("workload was not stopped before return")
	}
	ops := savedOps(t, opts.HistoryPath)
	pending := map[int]history.Op{}
	started := false
	for _, op := range ops {
		if op.F == "workload-start" {
			started = true
		}
		if op.F != "fault" {
			continue
		}
		var payload ActionRecord
		if err := json.Unmarshal(op.Value, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Injection.RunID != opts.RunID || payload.Injection.Event.ID == "" {
			t.Fatal("missing fault identity")
		}
		if payload.Stage == "execution" && !started {
			t.Fatal("fault preceded workload start")
		}
		if op.Type == history.Invoke {
			pending[op.ID] = op
		} else {
			inv, ok := pending[op.ID]
			if !ok || op.Time < inv.Time || op.Type != history.OK {
				t.Fatal("invalid control pair")
			}
			delete(pending, op.ID)
		}
	}
	if len(pending) != 0 {
		t.Fatal("unfinished controller records")
	}
}

func TestExecuteCancelsLoadBeforeIndependentCleanup(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loadCtx context.Context
	opts.OnStart = func(ctx context.Context) { loadCtx = ctx }
	c.apply = func(ctx context.Context, i Injection) error {
		c.states[i.ObjectName()] = Active
		cancel()
		return ctx.Err()
	}
	c.heal = func(ctx context.Context, i Injection) error {
		if ctx.Err() != nil || loadCtx.Err() == nil {
			t.Error("cleanup inherited cancellation or load was not stopped")
		}
		delete(c.states, i.ObjectName())
		return nil
	}
	res, err := execute(ctx, plan, opts, clock)
	if !errors.Is(err, context.Canceled) || res.Completed || len(c.applied) != 1 || len(c.states) != 0 || res.CleanupAttempts != len(plan.Events) {
		t.Fatalf("cancel cleanup: %+v %v", res, err)
	}
	if len(savedOps(t, opts.HistoryPath)) == 0 {
		t.Fatal("error history not saved")
	}
}

func TestExecuteUncertainApplyAndFailedHealStopInjections(t *testing.T) {
	for _, mode := range []string{"uncertain-apply", "unverified-apply", "failed-heal", "late-heal", "cleanup-error"} {
		t.Run(mode, func(t *testing.T) {
			plan, opts, c, clock := executionFixture(t)
			heals := 0
			if mode == "uncertain-apply" {
				c.apply = func(ctx context.Context, i Injection) error {
					c.states[i.ObjectName()] = Active
					return errors.New("controller response lost")
				}
			}
			if mode == "unverified-apply" {
				c.apply = func(context.Context, Injection) error { return nil }
			}
			if mode == "failed-heal" || mode == "late-heal" || mode == "cleanup-error" {
				c.heal = func(ctx context.Context, i Injection) error {
					heals++
					if heals == 1 {
						if mode == "late-heal" {
							clock.now = clock.now.Add(plan.Config.ActionTimeout + time.Second)
						}
						if mode != "late-heal" {
							return errors.New("healing unavailable")
						}
					}
					if mode == "cleanup-error" {
						return errors.New("cleanup unavailable")
					}
					delete(c.states, i.ObjectName())
					return nil
				}
			}
			res, err := execute(context.Background(), plan, opts, clock)
			if err == nil || res.Completed || len(c.applied) != 1 || res.CleanupAttempts != len(plan.Events) {
				t.Fatalf("failed action did not stop new injections: %+v %v", res, err)
			}
			if mode != "cleanup-error" && len(c.states) != 0 {
				t.Fatal("uncertain/failed action was not reconciled")
			}
			if mode == "cleanup-error" && !strings.Contains(err.Error(), "cleanup") {
				t.Fatal("cleanup failure hidden")
			}
			found := false
			for _, op := range savedOps(t, opts.HistoryPath) {
				if op.F == "fault" && op.Type == history.Info {
					found = true
				}
			}
			if !found {
				t.Fatal("uncertain control outcome was not recorded")
			}
		})
	}
}

func TestExecuteLateStartDoesNotResampleOrInjectExpiredWindow(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	clock.wait = func(at time.Time) { clock.now = at.Add(time.Minute) }
	res, err := execute(context.Background(), plan, opts, clock)
	if err == nil || res.Completed || len(c.applied) != 0 || res.CleanupAttempts != len(plan.Events) {
		t.Fatal("expired plan window was silently moved or injected")
	}
}

func TestExecuteRefusesMissingBackendAndExistingArtifacts(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	opts.Controllers = nil
	if _, err := execute(context.Background(), plan, opts, clock); err == nil || len(c.applied) != 0 {
		t.Fatal("missing backend silently simulated")
	}
	if _, err := os.Stat(opts.PlanPath); !os.IsNotExist(err) {
		t.Fatal("invalid execution wrote an artifact")
	}
	opts.Controllers = map[Kind]Controller{Pause: c}
	if err := os.WriteFile(opts.PlanPath, []byte("prior artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(context.Background(), plan, opts, clock); err == nil || len(c.applied) != 0 {
		t.Fatal("execution continued after failed plan persistence")
	}
	data, _ := os.ReadFile(opts.PlanPath)
	if string(data) != "prior artifact" {
		t.Fatal("existing plan was overwritten")
	}
}

func TestControllerCallsHaveDeadlines(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	c.observe = func(ctx context.Context, i Injection) (State, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("control action has no deadline")
		}
		if s, ok := c.states[i.ObjectName()]; ok {
			return s, nil
		}
		return Healthy, nil
	}
	if _, err := execute(context.Background(), plan, opts, clock); err != nil {
		t.Fatal(err)
	}
}

func TestBlockingApplyHonorsTimeoutAndStillCleansUp(t *testing.T) {
	_, opts, c, _ := executionFixture(t)
	cfg := DefaultConfig()
	cfg.Kinds = []Kind{Pause}
	cfg.StopGrace = 0
	cfg.RunDuration = time.Second
	cfg.Warmup = 0
	cfg.MinHealthy = 0
	cfg.MaxHealthy = 0
	cfg.MinFault = 50 * time.Millisecond
	cfg.MaxFault = cfg.MinFault
	cfg.ActionTimeout = 5 * time.Millisecond
	plan, err := Generate(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.apply = func(ctx context.Context, _ Injection) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := Execute(ctx, plan, opts)
	if !errors.Is(err, context.DeadlineExceeded) || res.Completed || len(c.applied) != 1 || res.CleanupAttempts != len(plan.Events) {
		t.Fatalf("blocked apply was not bounded/cleaned: %+v %v", res, err)
	}
}

func TestCleanupBudgetDoesNotInheritWorkloadCancellation(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	plan.Config.CleanupTimeout = 5 * time.Millisecond
	plan, _ = Generate(plan.Seed, plan.Config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.apply = func(context.Context, Injection) error { cancel(); return context.Canceled }
	c.heal = func(ctx context.Context, _ Injection) error { <-ctx.Done(); return ctx.Err() }
	res, err := execute(ctx, plan, opts, clock)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || res.CleanupAttempts != len(plan.Events) || res.Completed {
		t.Fatalf("cleanup deadline/errors were not preserved: %+v %v", res, err)
	}
}

func TestRepeatedPlanUsesSeparateBackendObjectNames(t *testing.T) {
	plan, opts, c, clock := executionFixture(t)
	if _, err := execute(context.Background(), plan, opts, clock); err != nil {
		t.Fatal(err)
	}
	first := c.applied[0].ObjectName()
	dir := t.TempDir()
	opts.RunID = "run-2"
	opts.PlanPath = filepath.Join(dir, "plan.json")
	opts.HistoryPath = filepath.Join(dir, "history.jsonl")
	opts.Recorder = history.NewRecorder()
	c.applied = nil
	if _, err := execute(context.Background(), plan, opts, clock); err != nil {
		t.Fatal(err)
	}
	if first == c.applied[0].ObjectName() {
		t.Fatal("same seed caused backend object collisions across executions")
	}
}

func TestCleanupPrioritizesUnreconciledFaultOverUnstartedEvents(t *testing.T) {
	for _, mode := range []string{"uncertain-apply", "failed-heal"} {
		t.Run(mode, func(t *testing.T) {
			plan, opts, c, clock := executionFixture(t)
			plan.Config.CleanupTimeout = 100 * time.Millisecond
			var err error
			plan, err = Generate(plan.Seed, plan.Config)
			if err != nil {
				t.Fatal(err)
			}
			pendingIndex := 0
			if mode == "failed-heal" {
				pendingIndex = 1
			}
			pending := plan.Events[pendingIndex].ID
			cleanupStarted := false
			c.apply = func(ctx context.Context, injection Injection) error {
				c.states[injection.ObjectName()] = Active
				if mode == "uncertain-apply" && injection.Event.ID == pending {
					cleanupStarted = true
					return errors.New("apply response lost")
				}
				return nil
			}
			c.heal = func(ctx context.Context, injection Injection) error {
				if mode == "failed-heal" && injection.Event.ID == pending && !cleanupStarted {
					cleanupStarted = true
					return errors.New("scheduled heal failed")
				}
				if cleanupStarted && injection.Event.ID > pending {
					// A future event's unavailable backend uses the entire remaining
					// budget. It must not delay repair of the real, active fault.
					<-ctx.Done()
					return ctx.Err()
				}
				delete(c.states, injection.ObjectName())
				return nil
			}
			res, err := execute(context.Background(), plan, opts, clock)
			if err == nil || res.Completed || len(c.applied) != pendingIndex+1 || res.CleanupAttempts != len(plan.Events) {
				t.Fatalf("unexpected execution/cleanup: %+v %v", res, err)
			}
			if len(c.states) != 0 {
				t.Fatalf("active fault was left behind while cleaning unstarted events: %v", c.states)
			}
			var cleanupIDs []string
			seen := map[string]bool{}
			for _, op := range savedOps(t, opts.HistoryPath) {
				if op.F != "fault" || op.Phase != "cleanup" || op.Type != history.Invoke {
					continue
				}
				var payload ActionRecord
				if err := json.Unmarshal(op.Value, &payload); err != nil {
					t.Fatal(err)
				}
				id := payload.Injection.Event.ID
				if seen[id] {
					t.Fatal("cleanup visited a fault more than once")
				}
				seen[id] = true
				cleanupIDs = append(cleanupIDs, id)
			}
			if len(cleanupIDs) != len(plan.Events) || cleanupIDs[0] != pending {
				t.Fatalf("unreconciled fault was not first: %v", cleanupIDs)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("unavailable future backend did not exercise cleanup-budget exhaustion")
			}
		})
	}
}
