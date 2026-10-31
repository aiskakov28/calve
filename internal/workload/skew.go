package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// SkewMode distinguishes the race from controls that establish a stale guard.
type SkewMode string

const (
	SkewConcurrent SkewMode = "concurrent"
	SkewAFirst     SkewMode = "control-a-first"
	SkewBFirst     SkewMode = "control-b-first"
)

// SkewConfig requests Rounds concurrent pairs plus one control in each direction.
type SkewConfig struct {
	Namespace string
	Rounds    int
	// Hooks drive the run under a fault campaign; nil runs without faults.
	// Under hooks every round's tables and baselines are set up before the plan
	// starts, concurrent rounds start Interval apart until the horizon, and the
	// controls run after healing.
	Hooks    *CampaignHooks
	Interval time.Duration
}

// SkewRun describes execution, not a checker verdict. HTTP evidence is in rec.
type SkewRun struct {
	RunID  string      `json:"run_id"`
	Rounds []SkewRound `json:"rounds"`
}

// SkewRound records whether attempts ran, overlapped, or left unknown commits.
// ControlEstablished means the first update was verified before the second call.
// Final observations after an unknown response do not establish quiescence.
type SkewRound struct {
	ID                 string             `json:"id"`
	Mode               SkewMode           `json:"mode"`
	Tables             [2]rest.Identifier `json:"tables"`
	Attempts           [2]SkewAttempt     `json:"attempts"`
	Overlapped         bool               `json:"overlapped"`
	ControlEstablished bool               `json:"control_established"`
	Unresolved         bool               `json:"unresolved"`
	SkippedReason      string             `json:"skipped_reason,omitempty"`
	Error              string             `json:"error,omitempty"`
}

// SkewAttempt is absent when Invoked is false; it is never an invented failure.
type SkewAttempt struct {
	Invoked     bool   `json:"invoked"`
	OperationID int    `json:"operation_id,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Status      int    `json:"status,omitempty"`
}

// SkewTxn labels a commit's target and unique added field. Its request body holds
// both tables' UUID/schema guards and the complete update, including empty arrays.
type SkewTxn struct {
	RunID       string          `json:"run_id"`
	Mode        SkewMode        `json:"mode"`
	Transaction string          `json:"transaction"`
	Target      rest.Identifier `json:"target"`
	Marker      rest.Field      `json:"marker"`
}

// RunSkew creates fresh tables per pair and sends each transaction at most once.
// It configures a private client copy and records all setup, reads, and commits.
// Tables remain available for inspection. The caller must save rec even on error;
// the checker and CLI are separate work. No fault healing or unknown resolution is
// implied by the post-attempt reads.
func RunSkew(ctx context.Context, c *rest.Client, cfg SkewConfig, rec *history.Recorder) (run SkewRun, err error) {
	if c == nil || rec == nil {
		return run, errors.New("skew requires a client and recorder")
	}
	if cfg.Rounds <= 0 || cfg.Rounds > int(^uint(0)>>1)-2 || cfg.Namespace == "" {
		return run, errors.New("skew requires a namespace and positive rounds")
	}
	if cfg.Interval < 0 {
		return run, errors.New("skew round interval must not be negative")
	}
	for _, part := range strings.Split(cfg.Namespace, ".") {
		if strings.TrimSpace(part) == "" {
			return run, errors.New("skew namespace contains an empty level")
		}
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return run, fmt.Errorf("skew run ID: %w", err)
	}
	run.RunID = "skew_" + hex.EncodeToString(nonce[:])
	c = c.WithRecorder(rec)
	defer func() { summarizeSkew(&run, rec) }()
	meta := map[string]any{
		"run_id": run.RunID, "started_at": time.Now().UTC(), "workload": "skew",
		"catalog":   history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		"warehouse": history.RedactText(c.Warehouse, "", c.Token),
		"namespace": cfg.Namespace, "rounds": cfg.Rounds, "controls": 2,
		"request_timeout_ns": c.HTTP.Timeout.Nanoseconds(),
	}
	if cfg.Hooks != nil {
		for k, v := range cfg.Hooks.Link {
			meta[k] = v
		}
	}
	recordSkewMeta(rec, "run", "", "setup", meta)
	setup := history.WithOperation(ctx, history.Op{Process: -1, Phase: "setup"})
	if _, err := c.Config(setup); err != nil {
		return run, err
	}
	if r := c.CreateNamespace(setup, cfg.Namespace); r.Outcome() != "ok" && r.Status != 409 {
		return run, skewResponseError("create namespace", r)
	}
	if cfg.Hooks != nil {
		return run, runSkewCampaign(ctx, c, cfg, &run, rec)
	}
	for i := 0; i < cfg.Rounds+2; i++ {
		if err := ctx.Err(); err != nil {
			return run, err
		}
		round := newSkewRound(cfg, run.RunID, i)
		recordSkewMeta(rec, "skew-round", round.ID, "setup", round)
		err := runSkewRound(ctx, c, run.RunID, &round)
		if err != nil {
			round.Error = err.Error()
		}
		run.Rounds = append(run.Rounds, round)
		if err != nil {
			return run, fmt.Errorf("%s: %w", round.ID, err)
		}
	}
	return run, nil
}

func newSkewRound(cfg SkewConfig, runID string, i int) SkewRound {
	mode := SkewConcurrent
	if i == cfg.Rounds {
		mode = SkewAFirst
	} else if i == cfg.Rounds+1 {
		mode = SkewBFirst
	}
	round := SkewRound{ID: fmt.Sprintf("%s_r%d", runID, i), Mode: mode}
	for p, suffix := range []string{"a", "b"} {
		round.Tables[p] = rest.Identifier{Namespace: strings.Split(cfg.Namespace, "."), Name: round.ID + "_" + suffix}
	}
	return round
}

// runSkewCampaign sets up every round before the plan starts, so the load
// phase sends only transactions. Concurrent rounds start cfg.Interval apart
// until the horizon, and rounds left then are not started. Final reads and
// both controls follow healing.
func runSkewCampaign(ctx context.Context, c *rest.Client, cfg SkewConfig, run *SkewRun, rec *history.Recorder) error {
	preps := make([]skewPrepared, 0, cfg.Rounds+2)
	for i := 0; i < cfg.Rounds+2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		round := newSkewRound(cfg, run.RunID, i)
		recordSkewMeta(rec, "skew-round", round.ID, "setup", round)
		prep, err := prepareSkewRound(ctx, c, run.RunID, &round)
		if err != nil {
			round.Error = err.Error()
		}
		run.Rounds = append(run.Rounds, round)
		if err != nil {
			return fmt.Errorf("%s: %w", round.ID, err)
		}
		preps = append(preps, prep)
	}
	cfg.Hooks.ready()
	probe := c.WithRecorder(nil) // Readiness polls are not evidence.
	started := 0
	start := time.Now()
	for started < cfg.Rounds && ctx.Err() == nil && cfg.Hooks.waitUntil(start.Add(time.Duration(started)*cfg.Interval)) {
		round := &run.Rounds[started]
		attemptSkewRound(ctx, c, round, preps[started], false) // A concurrent pair returns no error.
		started++
		// A pair sent into an outage would only add unknowns that never resolve.
		if round.Attempts[0].Outcome == "info" || round.Attempts[1].Outcome == "info" {
			cfg.Hooks.waitReachable(ctx, probe)
		}
	}
	for i := started; i < cfg.Rounds; i++ {
		run.Rounds[i].SkippedReason = "not started before the plan's horizon"
	}
	cfg.Hooks.waitHealed()
	for i := range run.Rounds {
		if i >= started && i < cfg.Rounds {
			continue
		}
		round := &run.Rounds[i]
		var err error
		if round.Mode != SkewConcurrent {
			err = attemptSkewRound(ctx, c, round, preps[i], true)
		}
		if err == nil {
			// A failed final read leaves the round inconclusive; read-back may
			// still observe the tables.
			err = finishSkewRound(ctx, c, round, preps[i], true)
		}
		if err != nil {
			round.Error = err.Error()
			return fmt.Errorf("%s: %w", round.ID, err)
		}
	}
	return ctx.Err()
}

// skewPrepared holds a round's baselines and the transactions built on them.
type skewPrepared struct {
	base     [2]rest.TableMetadata
	requests [2][]rest.TableChange
	labels   [2]SkewTxn
}

var skewInitial = rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Required: true, Type: "long"}}}

func runSkewRound(ctx context.Context, c *rest.Client, runID string, round *SkewRound) error {
	prep, err := prepareSkewRound(ctx, c, runID, round)
	if err == nil {
		err = attemptSkewRound(ctx, c, round, prep, false)
	}
	if err == nil {
		err = finishSkewRound(ctx, c, round, prep, false)
	}
	return err
}

func skewPhase(ctx context.Context, round *SkewRound, phase string) context.Context {
	return history.WithOperation(ctx, history.Op{Process: -1, Phase: phase, Round: round.ID})
}

// prepareSkewRound creates the round's fresh tables, reads their baselines and
// builds both transactions from them.
func prepareSkewRound(ctx context.Context, c *rest.Client, runID string, round *SkewRound) (prep skewPrepared, err error) {
	for _, table := range round.Tables {
		// Never drop an existing table: a collision must fail without overwriting it.
		r := c.CreateTable(skewPhase(ctx, round, "setup"), strings.Join(table.Namespace, "."), table.Name, skewInitial)
		if r.Outcome() != "ok" {
			return prep, skewResponseError("create table "+table.Name, r)
		}
	}
	base, err := loadSkewPair(skewPhase(ctx, round, "baseline"), c, round.Tables)
	if err != nil {
		return prep, fmt.Errorf("baseline: %w", err)
	}
	prep.base = base
	for p := range prep.requests {
		current, err := skewCurrentSchema(base[p])
		if err != nil || !reflect.DeepEqual(current.Fields, skewInitial.Fields) {
			return prep, fmt.Errorf("baseline %s is not the fresh one-column schema", round.Tables[p].Name)
		}
		if base[p].LastColumnID < 1 || base[p].LastColumnID >= 2147483647 {
			return prep, fmt.Errorf("baseline %s has an unusable last-column-id", round.Tables[p].Name)
		}
		prep.labels[p] = SkewTxn{
			RunID: runID,
			Mode:  round.Mode, Transaction: fmt.Sprintf("t%d", p+1), Target: round.Tables[p],
			Marker: rest.Field{ID: base[p].LastColumnID + 1, Name: round.ID + fmt.Sprintf("_t%d", p+1), Type: "long"},
		}
		prep.requests[p], err = skewChanges(round.Tables, base, p, prep.labels[p].Marker)
		if err != nil {
			return prep, fmt.Errorf("prepare transaction for %s: %w", round.Tables[p].Name, err)
		}
	}
	return prep, nil
}

// attemptSkewRound sends a concurrent pair at once, or a control's second
// transaction only after reading back the first. With tolerant, a failed
// verification read skips the stale attempt instead of ending the run.
func attemptSkewRound(ctx context.Context, c *rest.Client, round *SkewRound, prep skewPrepared, tolerant bool) error {
	call := func(p int) {
		value, _ := json.Marshal(prep.labels[p])
		callCtx := history.WithOperation(ctx, history.Op{Process: p, F: "skew-txn", Phase: "workload", Round: round.ID, Value: value})
		r := c.CommitTransaction(callCtx, prep.requests[p])
		round.Attempts[p] = SkewAttempt{Invoked: true, Outcome: r.Outcome(), Status: r.Status}
	}
	if round.Mode == SkewConcurrent {
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(2)
		done.Add(2)
		for p := range prep.requests {
			go func(p int) {
				defer done.Done()
				ready.Done()
				<-start
				call(p)
			}(p)
		}
		ready.Wait()
		close(start)
		done.Wait()
		return nil
	}
	first := 0
	if round.Mode == SkewBFirst {
		first = 1
	}
	call(first)
	if round.Attempts[first].Outcome != "ok" {
		round.SkippedReason = "first transaction was not acknowledged"
		return nil
	}
	observed, err := loadSkewTable(skewPhase(ctx, round, "control-verify"), c, round.Tables[first])
	if err != nil && tolerant && observed.TableUUID == "" {
		round.SkippedReason = "control verification read failed"
		return nil
	}
	if err != nil {
		return fmt.Errorf("control verification: %w", err)
	}
	current, _ := skewCurrentSchema(observed) // loadSkewTable validated it.
	expected := append(append([]rest.Field(nil), skewInitial.Fields...), prep.labels[first].Marker)
	if observed.TableUUID != prep.base[first].TableUUID {
		return errors.New("control verification: table UUID changed")
	}
	if observed.CurrentSchemaID == prep.base[first].CurrentSchemaID || !reflect.DeepEqual(current.Fields, expected) {
		round.SkippedReason = "first acknowledged schema update was not established"
	} else {
		round.ControlEstablished = true
		call(1 - first) // The original request, with its now-stale guard.
	}
	return nil
}

// finishSkewRound reads both tables after the attempts. A changed table UUID
// ends the run; with tolerant, a failed read is left to the checker.
func finishSkewRound(ctx context.Context, c *rest.Client, round *SkewRound, prep skewPrepared, tolerant bool) error {
	final, err := loadSkewPair(skewPhase(ctx, round, "final"), c, round.Tables)
	if err != nil && !tolerant {
		return fmt.Errorf("final observation: %w", err)
	}
	for p := range final {
		if final[p].TableUUID != "" && final[p].TableUUID != prep.base[p].TableUUID {
			return errors.New("final observation: table UUID changed")
		}
	}
	return nil
}

func skewChanges(tables [2]rest.Identifier, base [2]rest.TableMetadata, target int, marker rest.Field) ([]rest.TableChange, error) {
	nextID, err := nextSkewSchemaID(base[target].Schemas)
	if err != nil {
		return nil, err
	}
	changes := make([]rest.TableChange, 0, 2)
	// Put the updated table first, as in the upstream reproduction.
	for _, p := range []int{target, 1 - target} {
		table := tables[p]
		change := rest.TableChange{Identifier: &table, Requirements: []map[string]any{
			{"type": "assert-table-uuid", "uuid": base[p].TableUUID},
			{"type": "assert-current-schema-id", "current-schema-id": base[p].CurrentSchemaID},
		}, Updates: []map[string]any{}}
		if p == target {
			current, err := skewCurrentSchema(base[p])
			if err != nil {
				return nil, err
			}
			var schema map[string]json.RawMessage
			json.Unmarshal(current.Raw, &schema)
			// Match the Java client: propose an unused ID from the full history.
			// Some catalogs preserve this ID instead of assigning one themselves.
			schema["schema-id"], _ = json.Marshal(nextID)
			var fields []json.RawMessage
			json.Unmarshal(schema["fields"], &fields)
			added, _ := json.Marshal(marker)
			schema["fields"], _ = json.Marshal(append(fields, added))
			change.Updates = []map[string]any{
				{"action": "add-schema", "schema": schema, "last-column-id": marker.ID},
				{"action": "set-current-schema", "schema-id": -1},
			}
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func nextSkewSchemaID(schemas []json.RawMessage) (int, error) {
	maxID := -1
	for _, raw := range schemas {
		var schema struct {
			ID *int `json:"schema-id"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil || schema.ID == nil || *schema.ID < 0 {
			return 0, errors.New("cannot allocate schema ID from malformed schema history")
		}
		if *schema.ID >= 2147483647 {
			return 0, errors.New("schema ID space is exhausted")
		}
		maxID = max(maxID, *schema.ID)
	}
	if maxID < 0 {
		return 0, errors.New("cannot allocate schema ID from empty schema history")
	}
	return maxID + 1, nil
}

type skewSchema struct {
	ID     *int            `json:"schema-id"`
	Type   string          `json:"type"`
	Fields []rest.Field    `json:"fields"`
	Raw    json.RawMessage `json:"-"`
}

func skewCurrentSchema(meta rest.TableMetadata) (skewSchema, error) {
	var current skewSchema
	found := false
	ids := map[int]bool{}
	for _, raw := range meta.Schemas {
		var schema skewSchema
		if err := json.Unmarshal(raw, &schema); err != nil || schema.ID == nil || *schema.ID < 0 || ids[*schema.ID] {
			return current, errors.New("invalid or duplicate schema ID")
		}
		ids[*schema.ID] = true
		if *schema.ID == meta.CurrentSchemaID {
			schema.Raw = raw
			current, found = schema, true
		}
	}
	if !found || current.Type != "struct" || len(current.Fields) == 0 {
		return current, errors.New("current schema is missing or malformed")
	}
	return current, nil
}

func loadSkewTable(ctx context.Context, c *rest.Client, table rest.Identifier) (rest.TableMetadata, error) {
	r := c.LoadTable(ctx, strings.Join(table.Namespace, "."), table.Name)
	if r.Outcome() != "ok" {
		return rest.TableMetadata{}, skewResponseError("load "+table.Name, r)
	}
	meta, err := rest.ParseMetadata(r)
	if err != nil {
		return meta, errors.New("load " + table.Name + ": malformed metadata JSON")
	}
	// Missing IDs must not silently become valid zero-valued IDs.
	var presence struct {
		Metadata struct {
			Current *int `json:"current-schema-id"`
			Last    *int `json:"last-column-id"`
		} `json:"metadata"`
	}
	json.Unmarshal(r.Body, &presence)
	if meta.TableUUID == "" || presence.Metadata.Current == nil || presence.Metadata.Last == nil || meta.CurrentSchemaID < 0 || meta.LastColumnID < 1 {
		return meta, errors.New("load " + table.Name + ": missing or invalid identity/schema metadata")
	}
	if _, err := skewCurrentSchema(meta); err != nil {
		return meta, fmt.Errorf("load %s: %w", table.Name, err)
	}
	return meta, nil
}

func loadSkewPair(ctx context.Context, c *rest.Client, tables [2]rest.Identifier) (out [2]rest.TableMetadata, err error) {
	for p, table := range tables {
		var readErr error
		out[p], readErr = loadSkewTable(ctx, c, table)
		err = errors.Join(err, readErr) // Preserve both observations, even if one fails.
	}
	return out, err
}

func recordSkewMeta(rec *history.Recorder, f, round, phase string, value any) {
	encoded, _ := json.Marshal(value)
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: f, Phase: phase, Round: round, Value: encoded})
}

func skewResponseError(stage string, r rest.Response) error {
	// The redacted HTTP record carries details; do not copy raw error bodies into labels.
	return fmt.Errorf("%s: HTTP %d (%s)", stage, r.Status, r.Outcome())
}

func summarizeSkew(run *SkewRun, rec *history.Recorder) {
	// Record indexes order the calls; times can tie on a coarse clock.
	type interval struct{ start, end int }
	byRound := map[string][2]interval{}
	ids := map[string][2]int{}
	for _, op := range rec.Ops() {
		if op.F != "skew-txn" || op.Process < 0 || op.Process > 1 {
			continue
		}
		times := byRound[op.Round]
		pairIDs := ids[op.Round]
		if op.Type == history.Invoke {
			times[op.Process].start = op.Index
			pairIDs[op.Process] = op.ID
		} else {
			times[op.Process].end = op.Index
		}
		byRound[op.Round], ids[op.Round] = times, pairIDs
	}
	for i := range run.Rounds {
		round := &run.Rounds[i]
		times := byRound[round.ID]
		for p := range round.Attempts {
			round.Attempts[p].OperationID = ids[round.ID][p]
			if round.Attempts[p].Outcome == "info" {
				round.Unresolved = true
			}
		}
		round.Overlapped = round.Attempts[0].Invoked && round.Attempts[1].Invoked && times[0].start < times[1].end && times[1].start < times[0].end
		recordSkewMeta(rec, "skew-round-result", round.ID, "result", round)
	}
}
