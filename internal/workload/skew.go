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
	recordSkewMeta(rec, "run", "", "setup", map[string]any{
		"run_id": run.RunID, "started_at": time.Now().UTC(), "workload": "skew",
		"catalog":   history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		"warehouse": history.RedactText(c.Warehouse, "", c.Token),
		"namespace": cfg.Namespace, "rounds": cfg.Rounds, "controls": 2,
		"request_timeout_ns": c.HTTP.Timeout.Nanoseconds(),
	})
	setup := history.WithOperation(ctx, history.Op{Process: -1, Phase: "setup"})
	if _, err := c.Config(setup); err != nil {
		return run, err
	}
	if r := c.CreateNamespace(setup, cfg.Namespace); r.Outcome() != "ok" && r.Status != 409 {
		return run, skewResponseError("create namespace", r)
	}
	for i := 0; i < cfg.Rounds+2; i++ {
		if err := ctx.Err(); err != nil {
			return run, err
		}
		mode := SkewConcurrent
		if i == cfg.Rounds {
			mode = SkewAFirst
		} else if i == cfg.Rounds+1 {
			mode = SkewBFirst
		}
		round := SkewRound{ID: fmt.Sprintf("%s_r%d", run.RunID, i), Mode: mode}
		for p, suffix := range []string{"a", "b"} {
			round.Tables[p] = rest.Identifier{Namespace: strings.Split(cfg.Namespace, "."), Name: round.ID + "_" + suffix}
		}
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

func runSkewRound(ctx context.Context, c *rest.Client, runID string, round *SkewRound) error {
	baseCtx := func(phase string) context.Context {
		return history.WithOperation(ctx, history.Op{Process: -1, Phase: phase, Round: round.ID})
	}
	initial := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Required: true, Type: "long"}}}
	for _, table := range round.Tables {
		// Never drop an existing table: a collision must fail without overwriting it.
		r := c.CreateTable(baseCtx("setup"), strings.Join(table.Namespace, "."), table.Name, initial)
		if r.Outcome() != "ok" {
			return skewResponseError("create table "+table.Name, r)
		}
	}
	base, err := loadSkewPair(baseCtx("baseline"), c, round.Tables)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	var requests [2][]rest.TableChange
	var labels [2]SkewTxn
	for p := range requests {
		current, err := skewCurrentSchema(base[p])
		if err != nil || !reflect.DeepEqual(current.Fields, initial.Fields) {
			return fmt.Errorf("baseline %s is not the fresh one-column schema", round.Tables[p].Name)
		}
		if base[p].LastColumnID < 1 || base[p].LastColumnID >= 2147483647 {
			return fmt.Errorf("baseline %s has an unusable last-column-id", round.Tables[p].Name)
		}
		labels[p] = SkewTxn{
			RunID: runID,
			Mode:  round.Mode, Transaction: fmt.Sprintf("t%d", p+1), Target: round.Tables[p],
			Marker: rest.Field{ID: base[p].LastColumnID + 1, Name: round.ID + fmt.Sprintf("_t%d", p+1), Type: "long"},
		}
		requests[p] = skewChanges(round.Tables, base, p, labels[p].Marker)
	}
	call := func(p int) {
		value, _ := json.Marshal(labels[p])
		callCtx := history.WithOperation(ctx, history.Op{Process: p, F: "skew-txn", Phase: "workload", Round: round.ID, Value: value})
		r := c.CommitTransaction(callCtx, requests[p])
		round.Attempts[p] = SkewAttempt{Invoked: true, Outcome: r.Outcome(), Status: r.Status}
	}
	if round.Mode == SkewConcurrent {
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(2)
		done.Add(2)
		for p := range requests {
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
	} else {
		first := 0
		if round.Mode == SkewBFirst {
			first = 1
		}
		call(first)
		if round.Attempts[first].Outcome != "ok" {
			round.SkippedReason = "first transaction was not acknowledged"
		} else {
			observed, err := loadSkewTable(baseCtx("control-verify"), c, round.Tables[first])
			if err != nil {
				return fmt.Errorf("control verification: %w", err)
			}
			current, _ := skewCurrentSchema(observed) // loadSkewTable validated it.
			expected := append(append([]rest.Field(nil), initial.Fields...), labels[first].Marker)
			if observed.TableUUID != base[first].TableUUID {
				return errors.New("control verification: table UUID changed")
			}
			if observed.CurrentSchemaID == base[first].CurrentSchemaID || !reflect.DeepEqual(current.Fields, expected) {
				round.SkippedReason = "first acknowledged schema update was not established"
			} else {
				round.ControlEstablished = true
				call(1 - first) // The original request, with its now-stale guard.
			}
		}
	}
	final, err := loadSkewPair(baseCtx("final"), c, round.Tables)
	if err != nil {
		return fmt.Errorf("final observation: %w", err)
	}
	for p := range final {
		if final[p].TableUUID != base[p].TableUUID {
			return errors.New("final observation: table UUID changed")
		}
	}
	return nil
}

func skewChanges(tables [2]rest.Identifier, base [2]rest.TableMetadata, target int, marker rest.Field) []rest.TableChange {
	changes := make([]rest.TableChange, 0, 2)
	// Put the updated table first, as in the upstream reproduction.
	for _, p := range []int{target, 1 - target} {
		table := tables[p]
		change := rest.TableChange{Identifier: &table, Requirements: []map[string]any{
			{"type": "assert-table-uuid", "uuid": base[p].TableUUID},
			{"type": "assert-current-schema-id", "current-schema-id": base[p].CurrentSchemaID},
		}, Updates: []map[string]any{}}
		if p == target {
			current, _ := skewCurrentSchema(base[p])
			var schema map[string]json.RawMessage
			json.Unmarshal(current.Raw, &schema)
			delete(schema, "schema-id") // The catalog assigns the new schema ID.
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
	return changes
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
	type interval struct{ start, end int64 }
	byRound := map[string][2]interval{}
	ids := map[string][2]int{}
	for _, op := range rec.Ops() {
		if op.F != "skew-txn" || op.Process < 0 || op.Process > 1 {
			continue
		}
		times := byRound[op.Round]
		pairIDs := ids[op.Round]
		if op.Type == history.Invoke {
			times[op.Process].start = op.Time
			pairIDs[op.Process] = op.ID
		} else {
			times[op.Process].end = op.Time
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
