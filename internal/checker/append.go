// Package checker decides whether a recorded history is valid.
package checker

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aiskakov28/calve/internal/history"
)

// AppendValue is the value an append workload records: the unique key it
// tried to add to the table (a data file path, a property key, etc.).
type AppendValue struct {
	Key string `json:"key"`
}

// Anomaly is one problem a checker found.
type Anomaly struct {
	Kind   string `json:"kind"`
	Key    string `json:"key,omitempty"`
	Detail string `json:"detail"`
}

// AppendResult is the verdict of CheckAppend.
type AppendResult struct {
	Valid      bool              `json:"valid"`
	Verdict    Verdict           `json:"verdict"`
	Anomalies  []Anomaly         `json:"anomalies,omitempty"`
	Reasons    []string          `json:"reasons,omitempty"`
	Operations []AppendOperation `json:"operations,omitempty"`
	// Counts of append completions by outcome.
	OK, Failed, Unknown int
	ResolvedCommitted   int `json:"resolved_committed"`
	Unresolved          int `json:"unresolved"`
}

// AppendOperation keeps the source outcome separate from later evidence.
// Not committed is never inferred solely from an absent key.
type AppendOperation struct {
	ID          int          `json:"id,omitempty"`
	Key         string       `json:"key"`
	Outcome     history.Type `json:"outcome"`
	CommitState string       `json:"commit_state"`
	Resolution  string       `json:"resolution,omitempty"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type appendProof struct {
	resolution string
	ids        []int
}

// Presence contradicts rejection even if the stored value is wrong. Commitment
// proof requires the complete expected effect and is tracked independently.
type appendEvidence struct {
	committed map[string]appendProof
	present   map[string]bool
}

// CheckAppend compares an append history with the keys found in the final
// table state (one entry per occurrence, so duplicates show up).
//
// Rules:
//   - acknowledged (ok) append missing from the table   -> "lost"
//   - failed append present in the table                -> "phantom-failed"
//   - key present more than once                        -> "duplicate"
//   - key present that no client ever tried to append   -> "unexpected"
//   - unknown (info) appends present in the table are resolved as committed
//   - absent unknowns remain inconclusive, never rejected or anomalous alone
//
// The caller supplies a trustworthy final observation of the same table.
// CheckAppendHistory validates and selects recorded observations for replay.
func CheckAppend(ops []history.Op, finalKeys []string) (AppendResult, error) {
	return checkAppend(ops, finalKeys, appendEvidence{})
}

func checkAppend(ops []history.Op, finalKeys []string, evidence appendEvidence) (AppendResult, error) {
	outcome := map[string]history.Type{}
	invoked := map[string]bool{}
	ids := map[string]int{}

	for _, op := range ops {
		if op.F != "append" {
			continue
		}
		var v AppendValue
		if err := json.Unmarshal(op.Value, &v); err != nil {
			return AppendResult{}, fmt.Errorf("op %d: bad append value: %w", op.Index, err)
		}
		if v.Key == "" {
			return AppendResult{}, fmt.Errorf("op %d: empty append key", op.Index)
		}
		switch op.Type {
		case history.Invoke:
			if invoked[v.Key] {
				return AppendResult{}, fmt.Errorf("append key %q was invoked more than once", v.Key)
			}
			invoked[v.Key] = true
			ids[v.Key] = op.ID
		case history.OK, history.Fail, history.Info:
			if !invoked[v.Key] || outcome[v.Key] != "" {
				return AppendResult{}, fmt.Errorf("append key %q has no unique invocation/completion", v.Key)
			}
			outcome[v.Key] = op.Type
		}
	}

	seen := map[string]int{}
	for _, k := range finalKeys {
		seen[k]++
	}

	res := AppendResult{}
	var keys []string
	for k := range invoked {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		// An invoke with no completion is as uncertain as an info record.
		t, done := outcome[k]
		if !done {
			t = history.Info
		}
		operation := AppendOperation{ID: ids[k], Key: k, Outcome: t, CommitState: "unknown"}
		proof := evidence.committed[k]
		switch t {
		case history.OK:
			res.OK++
			operation.CommitState, operation.Resolution = "committed", "acknowledged"
		case history.Fail:
			res.Failed++
			operation.CommitState, operation.Resolution = "rejected", "rejected"
			if seen[k] > 0 || evidence.present[k] {
				res.Anomalies = append(res.Anomalies, Anomaly{Kind: "phantom-failed", Key: k,
					Detail: "catalog rejected the append but its key was observed in the table"})
			}
		default:
			res.Unknown++
			if seen[k] > 0 || proof.resolution != "" {
				operation.CommitState, operation.Resolution = "committed", "final-key"
				res.ResolvedCommitted++
			} else {
				res.Unresolved++
				res.Reasons = append(res.Reasons, fmt.Sprintf("append %q is unknown; absence does not prove noncommitment", k))
			}
		}
		if proof.resolution != "" && t != history.OK {
			operation.CommitState, operation.Resolution = "committed", proof.resolution
			operation.EvidenceIDs = proof.ids
		}
		res.Operations = append(res.Operations, operation)
	}
	for _, operation := range res.Operations {
		if operation.CommitState != "committed" || operation.Outcome == history.Fail || seen[operation.Key] > 0 {
			continue
		}
		if res.Unresolved > 0 {
			res.Reasons = append(res.Reasons, fmt.Sprintf("missing committed append %q cannot be checked without settled observations", operation.Key))
		} else {
			res.Anomalies = append(res.Anomalies, Anomaly{Kind: "lost", Key: operation.Key,
				Detail: "a committed append's expected effect is missing or incomplete in the observed table"})
		}
	}

	var present []string
	for k := range seen {
		present = append(present, k)
	}
	for k := range evidence.present {
		if !invoked[k] && seen[k] == 0 {
			present = append(present, k)
		}
	}
	sort.Strings(present)
	for _, k := range present {
		if seen[k] > 1 {
			res.Anomalies = append(res.Anomalies, Anomaly{Kind: "duplicate", Key: k,
				Detail: fmt.Sprintf("key appears %d times in the final table", seen[k])})
		}
		if !invoked[k] {
			res.Anomalies = append(res.Anomalies, Anomaly{Kind: "unexpected", Key: k,
				Detail: "key was observed in the table but no client appended it"})
		}
	}

	res.Verdict = Valid
	if len(res.Reasons) > 0 {
		res.Verdict = Inconclusive
	}
	if len(res.Anomalies) > 0 {
		res.Verdict = Invalid
	}
	res.Valid = res.Verdict == Valid
	return res, nil
}
