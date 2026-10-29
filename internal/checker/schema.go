package checker

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
)

type SchemaAnomaly struct {
	Kind        string   `json:"kind"`
	Basis       string   `json:"basis"` // catalog-contract
	Round       int      `json:"round"`
	Columns     []string `json:"columns"`
	EvidenceIDs []int    `json:"evidence_ids"`
	Detail      string   `json:"detail"`
}

type SchemaCommit struct {
	ID          int          `json:"id"`
	Round       int          `json:"round"`
	Column      string       `json:"column"`
	Outcome     history.Type `json:"outcome"`
	CommitState string       `json:"commit_state"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type SchemaResult struct {
	Valid             bool            `json:"valid"`
	Verdict           Verdict         `json:"verdict"`
	Anomalies         []SchemaAnomaly `json:"anomalies,omitempty"`
	Reasons           []string        `json:"reasons,omitempty"`
	Commits           []SchemaCommit  `json:"commits"`
	Rounds            int             `json:"rounds"`
	RoundsWithWinner  int             `json:"rounds_with_winner"`
	OK                int             `json:"ok"`
	Failed            int             `json:"failed"`
	Unknown           int             `json:"unknown"`
	ResolvedCommitted int             `json:"resolved_committed"`
	Unresolved        int             `json:"unresolved"`
}

type schemaManifest struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
	Clients   int    `json:"clients"`
	Rounds    int    `json:"rounds"`
}

type schemaCall struct {
	invoke, done *history.Op
}

// schemaView is one successful table load: its identity, current schema
// columns, and every column in any retained schema.
type schemaView struct {
	call    *schemaCall
	uuid    string
	current int
	last    int
	schemas []json.RawMessage
	fields  []json.RawMessage // current schema fields
	columns map[string]bool   // names in any schema
	now     map[string]bool   // names in the current schema
}

// CheckSchema checks the guarded schema compare-and-set workload. Every
// contender in a round requires the same observed current schema ID and adds
// its own column, so at most one may take effect. Columns are carried into
// later schemas, so a contender's column in any load proves its commit took
// effect; absence alone never proves rejection.
func CheckSchema(ops []history.Op) (SchemaResult, error) {
	m, calls, err := parseSchemaHistory(ops)
	if err != nil {
		return SchemaResult{}, err
	}
	res := SchemaResult{Rounds: m.Rounds}
	// Loads are grouped by round for guards; every successful load is evidence.
	roundLoads := map[int]*schemaCall{}
	commits := map[int][]*schemaCall{}
	var views []schemaView
	uuid := ""
	cut := len(ops)
	for _, call := range calls {
		if call.invoke.F == "load-table" {
			if call.invoke.Phase == "baseline" || call.invoke.Phase == "workload" {
				r, _ := strconv.Atoi(call.invoke.Round)
				if roundLoads[r] != nil {
					return res, fmt.Errorf("schema history: duplicate load for round %d", r)
				}
				roundLoads[r] = call
			}
			if call.done == nil || call.done.Type != history.OK || call.done.Response == nil {
				continue
			}
			view, err := readSchemaView(call)
			if err != nil {
				return res, fmt.Errorf("schema observation %d: %w", call.invoke.ID, err)
			}
			if uuid == "" {
				uuid = view.uuid
			}
			if view.uuid != uuid {
				cut = min(cut, call.invoke.Index)
				continue
			}
			views = append(views, view)
			continue
		}
		r, _ := strconv.Atoi(call.invoke.Round)
		commits[r] = append(commits[r], call)
	}
	if cut < len(ops) {
		res.Reasons = append(res.Reasons, fmt.Sprintf("table UUID changed; only evidence before record %d is used", cut))
		kept := views[:0]
		for _, view := range views {
			if view.call.done.Index < cut {
				kept = append(kept, view)
			}
		}
		views = kept
	}

	planned := map[string]*schemaCall{}
	lastCommit := -1
	for r := 0; r < m.Rounds; r++ {
		load := roundLoads[r]
		if load == nil {
			res.Reasons = append(res.Reasons, fmt.Sprintf("round %d was not recorded", r))
			break
		}
		base, ok := schemaViewOf(views, load)
		if !ok {
			res.Reasons = append(res.Reasons, fmt.Sprintf("round %d guard observation is unavailable", r))
			break
		}
		if r == 0 && (len(base.schemas) != 1 || len(base.now) != 1 || !base.now["id"]) {
			return res, fmt.Errorf("schema history: baseline is not a fresh one-column table")
		}
		if len(commits[r]) != m.Clients {
			return res, fmt.Errorf("schema history: round %d has %d commits, want %d", r, len(commits[r]), m.Clients)
		}
		for _, call := range commits[r] {
			column, err := validateSchemaCommit(call, base, r, m.Clients)
			if err != nil {
				return res, fmt.Errorf("schema commit %d: %w", call.invoke.ID, err)
			}
			if call.invoke.Index <= load.done.Index {
				return res, fmt.Errorf("schema commit %d starts before its guard observation completes", call.invoke.ID)
			}
			if planned[column] != nil {
				return res, fmt.Errorf("schema history: duplicate column %s", column)
			}
			planned[column] = call
			if next := roundLoads[r+1]; next != nil && call.done != nil && next.invoke.Index < call.done.Index {
				return res, fmt.Errorf("schema history: round %d starts before commit %d completes", r+1, call.invoke.ID)
			}
			lastCommit = max(lastCommit, call.invoke.Index)
			if call.done != nil {
				lastCommit = max(lastCommit, call.done.Index)
			}
		}
	}
	final := false
	for _, view := range views {
		if view.call.invoke.Index > lastCommit && (view.call.invoke.Phase == "final" || view.call.invoke.Phase == "recovery") {
			final = true
		}
	}
	if !final {
		res.Reasons = append(res.Reasons, "no usable observation after the last commit")
	}

	// A run column that no planned commit adds, or that appears before its
	// commit was sent, cannot come from this workload.
	reported := map[string]bool{}
	for _, view := range views {
		var names []string
		for name := range view.columns {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			call := planned[name]
			if name == "id" || reported[name] || call != nil && view.call.done.Index > call.invoke.Index {
				continue
			}
			reported[name] = true
			res.Anomalies = append(res.Anomalies, SchemaAnomaly{Kind: "unexpected", Basis: "catalog-contract", Round: -1,
				Columns: []string{name}, EvidenceIDs: []int{view.call.invoke.ID},
				Detail: "a column was observed that no earlier commit in this run adds"})
		}
	}

	for r := 0; r < m.Rounds; r++ {
		var winners []string
		var winnerIDs []int
		for _, call := range commits[r] {
			column := schemaMarker(call)
			raw := schemaOutcome(call)
			commit := SchemaCommit{ID: call.invoke.ID, Round: r, Column: column, Outcome: raw, CommitState: "unknown"}
			seen := schemaProof(views, column)
			switch raw {
			case history.OK:
				res.OK++
				commit.CommitState = "committed"
			case history.Fail:
				res.Failed++
				commit.CommitState = "rejected"
			default:
				res.Unknown++
			}
			if seen != nil {
				commit.EvidenceIDs = []int{seen.call.invoke.ID}
			}
			switch {
			case raw == history.Fail && seen != nil:
				res.Anomalies = append(res.Anomalies, SchemaAnomaly{Kind: "phantom-failed", Basis: "catalog-contract", Round: r,
					Columns: []string{column}, EvidenceIDs: []int{call.invoke.ID, seen.call.invoke.ID},
					Detail: "a rejected commit's column is present in the table"})
			case raw == history.Info && seen != nil:
				commit.CommitState = "committed"
				res.ResolvedCommitted++
			case raw == history.Info:
				res.Unresolved++
			}
			if commit.CommitState == "committed" || seen != nil {
				winners = append(winners, column)
				winnerIDs = append(winnerIDs, call.invoke.ID)
				if seen != nil {
					winnerIDs = append(winnerIDs, seen.call.invoke.ID)
				}
			}
			res.Commits = append(res.Commits, commit)
		}
		if len(winners) > 0 {
			res.RoundsWithWinner++
		}
		if len(winners) > 1 {
			sort.Ints(winnerIDs)
			res.Anomalies = append(res.Anomalies, SchemaAnomaly{Kind: "multiple-winners", Basis: "catalog-contract", Round: r,
				Columns: winners, EvidenceIDs: winnerIDs,
				Detail: fmt.Sprintf("%d commits requiring the same current schema ID all took effect", len(winners))})
			continue
		}
		// With one winner, every load that starts after the commit is known to
		// have taken effect (its acknowledgment, or for an unknown, the first
		// load showing its column) carries the column in its current schema.
		for _, call := range commits[r] {
			column := schemaMarker(call)
			evidence := []int{call.invoke.ID}
			bound := -1
			switch schemaOutcome(call) {
			case history.OK:
				bound = call.done.Index
			case history.Info:
				proof := schemaProof(views, column)
				if proof == nil {
					continue
				}
				evidence = append(evidence, proof.call.invoke.ID)
				if !proof.now[column] {
					// The load that proves add-schema took effect also shows the
					// schema is not current: set-current-schema did not apply.
					res.Anomalies = append(res.Anomalies, SchemaAnomaly{Kind: "lost", Basis: "catalog-contract", Round: r,
						Columns: []string{column}, EvidenceIDs: evidence,
						Detail: "a proven schema change is retained but not current in the load that shows it"})
					continue
				}
				bound = proof.call.done.Index
			}
			if bound < 0 {
				continue
			}
			for _, view := range views {
				if view.call.invoke.Index > bound && !view.now[column] {
					res.Anomalies = append(res.Anomalies, SchemaAnomaly{Kind: "lost", Basis: "catalog-contract", Round: r,
						Columns: []string{column}, EvidenceIDs: append(evidence, view.call.invoke.ID),
						Detail: "a committed schema change is missing from a later load's current schema"})
					break
				}
			}
		}
	}
	if res.Unresolved > 0 {
		res.Reasons = append(res.Reasons, "unknown commits remain unresolved; absence does not prove rejection")
	}
	if res.RoundsWithWinner == 0 {
		res.Reasons = append(res.Reasons, "no round has a committed schema change")
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

func parseSchemaHistory(ops []history.Op) (m schemaManifest, calls []*schemaCall, err error) {
	bad := func(msg string) (schemaManifest, []*schemaCall, error) {
		return m, nil, fmt.Errorf("schema history: %s", msg)
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" || json.Unmarshal(ops[0].Value, &m) != nil ||
		m.Workload != "schema-cas" || !strings.HasPrefix(m.RunID, "schema_") || m.Table != m.RunID ||
		m.Clients < 2 || m.Rounds <= 0 || m.Clients > int(^uint(0)>>1)/m.Rounds {
		return bad("missing or invalid schema run manifest")
	}
	for _, level := range strings.Split(m.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return bad("empty namespace level")
		}
	}
	byID := map[int]*schemaCall{}
	route := ""
	for i := range ops[1:] {
		op := &ops[i+1]
		if op.Index != i+1 {
			return bad("records are out of order")
		}
		if op.Type == history.Meta {
			return bad("unexpected meta record")
		}
		if op.ID <= 0 {
			return bad("missing operation ID")
		}
		if op.Type == history.Invoke {
			if byID[op.ID] != nil || op.Request == nil {
				return bad("duplicate or incomplete invocation")
			}
			u, err := url.Parse(op.Request.URL)
			if err != nil || !strings.HasSuffix(u.EscapedPath(), "/tables/"+url.PathEscape(m.Table)) {
				return bad("operation targets another table")
			}
			if route == "" {
				route = u.EscapedPath()
			}
			if u.EscapedPath() != route {
				return bad("operations use different table routes")
			}
			switch {
			case op.F == "load-table" && op.Request.Method == "GET":
				phase := op.Phase
				if phase != "baseline" && phase != "workload" && phase != "final" && phase != "recovery" {
					return bad("unsupported load phase")
				}
				if (phase == "baseline" || phase == "workload") && !schemaRoundLabel(op.Round, m.Rounds, phase) {
					return bad("load has an invalid round")
				}
			case op.F == "commit-table" && op.Request.Method == "POST" && op.Phase == "workload":
				if !schemaRoundLabel(op.Round, m.Rounds, "commit") || op.Process < 0 || op.Process >= m.Clients {
					return bad("commit has an invalid round or process")
				}
			default:
				return bad("unsupported operation " + op.F)
			}
			call := &schemaCall{invoke: op}
			byID[op.ID] = call
			calls = append(calls, call)
			continue
		}
		call := byID[op.ID]
		if call == nil || call.done != nil || op.F != call.invoke.F || op.Process != call.invoke.Process ||
			op.Round != call.invoke.Round || op.Time < call.invoke.Time {
			return bad("unpaired or mismatched completion")
		}
		if op.Type != history.OK && op.Type != history.Fail && op.Type != history.Info {
			return bad("unknown completion type")
		}
		call.done = op
	}
	return m, calls, nil
}

func schemaRoundLabel(label string, rounds int, kind string) bool {
	r, err := strconv.Atoi(label)
	if err != nil || strconv.Itoa(r) != label || r < 0 || r >= rounds {
		return false
	}
	return kind != "baseline" || r == 0
}

func readSchemaView(call *schemaCall) (schemaView, error) {
	var body struct {
		Metadata struct {
			UUID    string            `json:"table-uuid"`
			Current *int              `json:"current-schema-id"`
			Last    *int              `json:"last-column-id"`
			Schemas []json.RawMessage `json:"schemas"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(call.done.Response.Body, &body); err != nil {
		return schemaView{}, fmt.Errorf("decode metadata: %w", err)
	}
	md := body.Metadata
	if md.UUID == "" || md.Current == nil || md.Last == nil || len(md.Schemas) == 0 {
		return schemaView{}, fmt.Errorf("metadata lacks UUID, current schema, last column ID or schemas")
	}
	view := schemaView{call: call, uuid: md.UUID, current: *md.Current, last: *md.Last, schemas: md.Schemas,
		columns: map[string]bool{}, now: map[string]bool{}}
	found := false
	for _, raw := range md.Schemas {
		var schema struct {
			ID     *int              `json:"schema-id"`
			Fields []json.RawMessage `json:"fields"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil || schema.ID == nil {
			return schemaView{}, fmt.Errorf("malformed schema")
		}
		for _, raw := range schema.Fields {
			var field struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(raw, &field) != nil || field.Name == "" {
				return schemaView{}, fmt.Errorf("malformed schema field")
			}
			view.columns[field.Name] = true
			if *schema.ID == view.current {
				view.now[field.Name] = true
			}
		}
		if *schema.ID == view.current {
			if found {
				return schemaView{}, fmt.Errorf("duplicate current schema ID")
			}
			found, view.fields = true, schema.Fields
		}
	}
	if !found {
		return schemaView{}, fmt.Errorf("current schema is missing")
	}
	return view, nil
}

func schemaViewOf(views []schemaView, call *schemaCall) (schemaView, bool) {
	for _, view := range views {
		if view.call == call {
			return view, true
		}
	}
	return schemaView{}, false
}

// schemaProof is the load that first finished showing column, or nil.
func schemaProof(views []schemaView, column string) *schemaView {
	var proof *schemaView
	for i := range views {
		if views[i].columns[column] && (proof == nil || views[i].call.done.Index < proof.call.done.Index) {
			proof = &views[i]
		}
	}
	return proof
}

func schemaMarker(call *schemaCall) string {
	var marker struct {
		Column string `json:"column"`
	}
	json.Unmarshal(call.invoke.Value, &marker)
	return marker.Column
}

func schemaOutcome(call *schemaCall) history.Type {
	if call.done == nil {
		return history.Info
	}
	return call.done.Type
}

// validateSchemaCommit requires the exact guarded request the workload sends:
// the observed UUID and current schema ID, and the observed current schema
// plus one new column made current.
func validateSchemaCommit(call *schemaCall, base schemaView, round, clients int) (string, error) {
	column := schemaMarker(call)
	if column != fmt.Sprintf("c_%d_%d", round, call.invoke.Process) {
		return "", fmt.Errorf("marker does not identify its round and process")
	}
	var body struct {
		Requirements []map[string]any `json:"requirements"`
		Updates      []struct {
			Action string `json:"action"`
			Schema struct {
				Type   string            `json:"type"`
				Fields []json.RawMessage `json:"fields"`
			} `json:"schema"`
			LastColumnID *float64 `json:"last-column-id"`
			SchemaID     *float64 `json:"schema-id"`
		} `json:"updates"`
	}
	if err := json.Unmarshal(call.invoke.Request.Body, &body); err != nil {
		return "", fmt.Errorf("decode request: %w", err)
	}
	want := []map[string]any{
		{"type": "assert-table-uuid", "uuid": base.uuid},
		{"type": "assert-current-schema-id", "current-schema-id": float64(base.current)},
	}
	if !reflect.DeepEqual(body.Requirements, want) {
		return "", fmt.Errorf("requirements are not the observed UUID and current schema ID")
	}
	if len(body.Updates) != 2 || body.Updates[0].Action != "add-schema" || body.Updates[1].Action != "set-current-schema" ||
		body.Updates[1].SchemaID == nil || *body.Updates[1].SchemaID != -1 {
		return "", fmt.Errorf("updates are not add-schema then set-current-schema -1")
	}
	added := body.Updates[0]
	if added.Schema.Type != "struct" || len(added.Schema.Fields) != len(base.fields)+1 {
		return "", fmt.Errorf("added schema is not the current schema plus one column")
	}
	for i, field := range base.fields {
		var have, want any
		if json.Unmarshal(added.Schema.Fields[i], &have) != nil || json.Unmarshal(field, &want) != nil || !reflect.DeepEqual(have, want) {
			return "", fmt.Errorf("added schema changes an existing column")
		}
	}
	var marker struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if json.Unmarshal(added.Schema.Fields[len(base.fields)], &marker) != nil || marker.Name != column ||
		marker.ID != base.last+1 || added.LastColumnID == nil || *added.LastColumnID != float64(marker.ID) {
		return "", fmt.Errorf("added column does not match the marker and next column ID")
	}
	return column, nil
}
