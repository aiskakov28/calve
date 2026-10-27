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
	Valid     bool      `json:"valid"`
	Anomalies []Anomaly `json:"anomalies,omitempty"`
	// Counts of append completions by outcome.
	OK, Failed, Unknown int
}

// CheckAppend compares an append history with the keys found in the final
// table state (one entry per occurrence, so duplicates show up).
//
// Rules:
//   - acknowledged (ok) append missing from the table   -> "lost"
//   - failed append present in the table                -> "phantom-failed"
//   - key present more than once                        -> "duplicate"
//   - key present that no client ever tried to append   -> "unexpected"
//   - unknown (info) appends may be present 0 or 1 times; never an anomaly alone
func CheckAppend(ops []history.Op, finalKeys []string) (AppendResult, error) {
	outcome := map[string]history.Type{}
	invoked := map[string]bool{}

	for _, op := range ops {
		if op.F != "append" {
			continue
		}
		var v AppendValue
		if err := json.Unmarshal(op.Value, &v); err != nil {
			return AppendResult{}, fmt.Errorf("op %d: bad append value: %w", op.Index, err)
		}
		switch op.Type {
		case history.Invoke:
			invoked[v.Key] = true
		case history.OK, history.Fail, history.Info:
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
		switch t {
		case history.OK:
			res.OK++
			if seen[k] == 0 {
				res.Anomalies = append(res.Anomalies, Anomaly{Kind: "lost", Key: k,
					Detail: "catalog acknowledged the append but the key is not in the final table"})
			}
		case history.Fail:
			res.Failed++
			if seen[k] > 0 {
				res.Anomalies = append(res.Anomalies, Anomaly{Kind: "phantom-failed", Key: k,
					Detail: "catalog rejected the append but the key is in the final table"})
			}
		default:
			res.Unknown++
		}
	}

	var present []string
	for k := range seen {
		present = append(present, k)
	}
	sort.Strings(present)
	for _, k := range present {
		if seen[k] > 1 {
			res.Anomalies = append(res.Anomalies, Anomaly{Kind: "duplicate", Key: k,
				Detail: fmt.Sprintf("key appears %d times in the final table", seen[k])})
		}
		if !invoked[k] {
			res.Anomalies = append(res.Anomalies, Anomaly{Kind: "unexpected", Key: k,
				Detail: "key is in the final table but no client appended it"})
		}
	}

	res.Valid = len(res.Anomalies) == 0
	return res, nil
}
