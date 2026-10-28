package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

type State string

const (
	Healthy State = "healthy"
	Active  State = "active"
	Unknown State = "unknown"
)

// Injection scopes backend objects to one execution, even when its seed repeats.
// Target is a logical ID; a backend resolves it to its configured proxy/container.
type Injection struct {
	RunID string `json:"run_id"`
	Event Event  `json:"event"`
}

func (i Injection) ObjectName() string { return i.RunID + "-" + i.Event.ID }

// Controller is the integration contract for issues #13 and #14. All methods
// must respect context deadlines and return only after their own mutations have
// finished or can be reconciled by Heal. Heal is idempotent, covers uncertain
// applies, and is a no-op for objects this run never changed. Observations and
// errors must be safe to save without credentials. Observe verifies actual state;
// a successful mutation response alone does not establish Active or Healthy.
type Controller interface {
	Apply(context.Context, Injection) error
	Heal(context.Context, Injection) error
	Observe(context.Context, Injection) (State, error)
}

type ExecuteOptions struct {
	RunID       string
	PlanPath    string
	HistoryPath string
	Recorder    *history.Recorder
	Controllers map[Kind]Controller
	// Called after preflight and workload-start. Start background load here and
	// return promptly. Its context is canceled before cleanup on every exit.
	// Waiting for catalog recovery and checking results remain caller duties.
	OnStart func(context.Context)
}

type ExecutionResult struct {
	RunID           string `json:"run_id"`
	Applied         int    `json:"applied"`
	Healed          int    `json:"healed"`
	CleanupAttempts int    `json:"cleanup_attempts"`
	Completed       bool   `json:"completed"`
}

// ActionRecord uses the recorder's operation ID for pairing, independently of
// the planned fault ID. History time bounds each control action; actual_offset
// and late_by use the scheduler's monotonic workload-start origin.
type ActionRecord struct {
	Injection     Injection     `json:"injection"`
	Action        string        `json:"action"`
	Stage         string        `json:"stage"`
	PlannedOffset time.Duration `json:"planned_offset_ns"`
	ActualOffset  time.Duration `json:"actual_offset_ns"`
	LateBy        time.Duration `json:"late_by_ns"`
	Observed      State         `json:"observed,omitempty"`
}

type schedulerClock interface {
	Now() time.Time
	WaitUntil(context.Context, time.Time) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) WaitUntil(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Until(at)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Execute persists the validated plan before any controller call or workload
// starts, then runs it without resampling. It cancels the load context before
// bounded cleanup of EVERY planned fault, including unacknowledged/unstarted
// actions. It saves history on normal success/error return, not after hard kill.
// The caller joins background load. V1 does not implement catalog recovery or
// a fault-aware checker; this history file is a controller-stage checkpoint.
func Execute(ctx context.Context, plan Plan, opts ExecuteOptions) (ExecutionResult, error) {
	return execute(ctx, plan, opts, realClock{})
}

func execute(ctx context.Context, plan Plan, opts ExecuteOptions, clock schedulerClock) (result ExecutionResult, err error) {
	result.RunID = opts.RunID
	if err := plan.Validate(); err != nil {
		return result, err
	}
	if !identifier(opts.RunID) || opts.Recorder == nil || opts.PlanPath == "" || opts.HistoryPath == "" {
		return result, errors.New("execution requires a run ID, recorder, and new plan/history artifact paths")
	}
	// Copy the registry and plan slices before running user callbacks.
	controllers := make(map[Kind]Controller, len(opts.Controllers))
	for kind, c := range opts.Controllers {
		controllers[kind] = c
	}
	for _, event := range plan.Events {
		if controllers[event.Kind] == nil {
			return result, fmt.Errorf("no controller registered for %s; generate a plan with --plan-only until the backend is available", event.Kind)
		}
	}
	plan.Events = append([]Event(nil), plan.Events...)
	plan.Config.Targets = append([]string(nil), plan.Config.Targets...)
	plan.Config.Kinds = append([]Kind(nil), plan.Config.Kinds...)
	file, err := os.OpenFile(opts.HistoryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return result, fmt.Errorf("create history: %w", err)
	}
	defer func() {
		saveErr := errors.Join(history.WriteJSONL(file, opts.Recorder.Ops()), file.Sync(), file.Close())
		if saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save fault history: %w", saveErr))
		}
		if err != nil {
			result.Completed = false
		}
	}()
	if err := Save(opts.PlanPath, plan); err != nil {
		return result, fmt.Errorf("save fault plan: %w", err)
	}
	encoded, _ := json.Marshal(map[string]any{"run_id": opts.RunID, "plan": plan})
	opts.Recorder.Record(history.Op{Type: history.Meta, Process: -2, F: "fault-plan", Value: encoded})
	loadCtx, stopLoad := context.WithCancel(ctx)
	var origin time.Time
	defer func() {
		stopLoad()
		// Cancellation of the workload must not cancel healing. A controller
		// must reconcile late applies before reporting Healthy here.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), plan.Config.CleanupTimeout)
		defer cancel()
		for i := len(plan.Events) - 1; i >= 0; i-- {
			event := plan.Events[i]
			result.CleanupAttempts++
			if actionErr := control(cleanup, controllers[event.Kind], Injection{opts.RunID, event}, "heal", "cleanup", 0, origin, clock.Now().Add(plan.Config.ActionTimeout), opts.Recorder, clock); actionErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup %s: %w", event.ID, actionErr))
			}
		}
		if err != nil {
			result.Completed = false
		}
	}()
	// Inspect every distinct backend/target before starting workload load.
	seen := map[string]bool{}
	for _, event := range plan.Events {
		key := string(event.Kind) + "\x00" + event.Target
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := control(ctx, controllers[event.Kind], Injection{opts.RunID, event}, "inspect", "preflight", 0, origin, clock.Now().Add(plan.Config.ActionTimeout), opts.Recorder, clock); err != nil {
			return result, fmt.Errorf("fault preflight: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	origin = clock.Now()
	opts.Recorder.Record(history.Op{Type: history.Meta, Process: -2, F: "workload-start", Phase: "workload", Value: json.RawMessage(`{"source":"fault-scheduler"}`)})
	if opts.OnStart != nil {
		opts.OnStart(loadCtx)
	}
	for _, event := range plan.Events {
		if err := clock.WaitUntil(ctx, origin.Add(event.Start)); err != nil {
			return result, err
		}
		deadline := origin.Add(event.HealAt + plan.Config.ActionTimeout)
		if !clock.Now().Before(deadline) {
			return result, fmt.Errorf("fault %s missed its healing deadline; no new injection attempted", event.ID)
		}
		if err := control(ctx, controllers[event.Kind], Injection{opts.RunID, event}, "apply", "execution", event.Start, origin, minTime(deadline, clock.Now().Add(plan.Config.ActionTimeout)), opts.Recorder, clock); err != nil {
			return result, fmt.Errorf("apply %s: %w", event.ID, err)
		}
		result.Applied++
		if err := clock.WaitUntil(ctx, origin.Add(event.HealAt)); err != nil {
			return result, err
		}
		if err := control(ctx, controllers[event.Kind], Injection{opts.RunID, event}, "heal", "execution", event.HealAt, origin, deadline, opts.Recorder, clock); err != nil {
			return result, fmt.Errorf("heal %s: %w", event.ID, err)
		}
		result.Healed++
	}
	if err := clock.WaitUntil(ctx, origin.Add(plan.Config.RunDuration)); err != nil {
		return result, err
	}
	result.Completed = true
	return result, nil
}

func control(ctx context.Context, c Controller, injection Injection, action, stage string, planned time.Duration, origin, deadline time.Time, rec *history.Recorder, clock schedulerClock) error {
	payload := ActionRecord{Injection: injection, Action: action, Stage: stage, PlannedOffset: planned}
	if !origin.IsZero() {
		payload.ActualOffset = clock.Now().Sub(origin)
		if stage == "execution" {
			payload.LateBy = max(0, payload.ActualOffset-planned)
		}
	}
	value, _ := json.Marshal(payload)
	invoke := rec.Record(history.Op{Type: history.Invoke, Process: -2, F: "fault", Phase: stage, Value: value})
	limit := deadline.Sub(clock.Now())
	actionCtx, cancel := context.WithTimeout(ctx, max(0, limit))
	defer cancel()
	var mutationErr error
	state := Unknown
	var observeErr error
	if actionCtx.Err() == nil {
		if action == "apply" {
			mutationErr = c.Apply(actionCtx, injection)
		} else if action == "heal" {
			mutationErr = c.Heal(actionCtx, injection)
		}
		state, observeErr = c.Observe(actionCtx, injection)
	}
	err := errors.Join(mutationErr, observeErr, actionCtx.Err())
	if clock.Now().After(deadline) {
		err = errors.Join(err, context.DeadlineExceeded)
	}
	expected := Healthy
	if action == "apply" {
		expected = Active
	}
	if state != expected {
		err = errors.Join(err, fmt.Errorf("observed %s, expected %s", state, expected))
	}
	payload.Observed = state
	if !origin.IsZero() {
		payload.ActualOffset = clock.Now().Sub(origin)
		if stage == "execution" {
			payload.LateBy = max(0, payload.ActualOffset-planned)
		}
	}
	value, _ = json.Marshal(payload)
	done := history.Op{ID: invoke.ID, Process: -2, Type: history.OK, F: "fault", Phase: stage, Value: value}
	if err != nil {
		done.Type = history.Info
		done.Error = err.Error()
	}
	rec.Record(done)
	return err
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func identifier(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if r < 'a' || r > 'z' {
			if r < 'A' || r > 'Z' {
				if r < '0' || r > '9' {
					if r != '_' && r != '-' && r != '.' {
						return false
					}
				}
			}
		}
	}
	return true
}
