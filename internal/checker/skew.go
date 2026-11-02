package checker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// SkewVerdict applies to the workload's at-most-one model, not general isolation.
type SkewVerdict = Verdict

const (
	SkewValid        = Valid
	SkewInvalid      = Invalid
	SkewInconclusive = Inconclusive
)

// SkewAnomaly distinguishes an isolation lead from a direct contract violation.
type SkewAnomaly struct {
	Round        string `json:"round"`
	Kind         string `json:"kind"`
	Basis        string `json:"basis"` // serializable-model or catalog-contract
	OperationIDs []int  `json:"operation_ids"`
	Detail       string `json:"detail"`
}

// SkewTransaction preserves the original outcome when state resolves an unknown.
type SkewTransaction struct {
	ID          int          `json:"id,omitempty"`
	Outcome     history.Type `json:"outcome,omitempty"`
	CommitState string       `json:"commit_state"` // not-invoked, unknown, committed, rejected
	Resolution  string       `json:"resolution,omitempty"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type SkewRoundResult struct {
	Round        string             `json:"round"`
	Mode         string             `json:"mode"`
	Verdict      SkewVerdict        `json:"verdict"`
	Overlapped   bool               `json:"overlapped"`
	OverlapKnown bool               `json:"overlap_known"`
	Transactions [2]SkewTransaction `json:"transactions"`
	Reasons      []string           `json:"reasons,omitempty"`
	Anomalies    []SkewAnomaly      `json:"anomalies,omitempty"`
}

// SkewResult is invalid if any finding is proven, otherwise inconclusive if any
// required evidence is unavailable. Valid is true only for a complete pass.
type SkewResult struct {
	Valid              bool              `json:"valid"`
	Verdict            SkewVerdict       `json:"verdict"`
	Rounds             []SkewRoundResult `json:"rounds"`
	Anomalies          []SkewAnomaly     `json:"anomalies,omitempty"`
	Reasons            []string          `json:"reasons,omitempty"`
	OK                 int               `json:"ok"`
	Failed             int               `json:"failed"`
	Unknown            int               `json:"unknown"`
	ResolvedCommitted  int               `json:"resolved_committed"`
	InconclusiveRounds int               `json:"inconclusive_rounds"`
	RejectedRounds     int               `json:"rejected_rounds"`
	// NotStarted counts concurrent rounds a fault campaign set up but its
	// horizon cut off; they are not in Rounds.
	NotStarted int `json:"not_started,omitempty"`
}

type skewCall struct {
	invoke history.Op
	done   *history.Op
}
type skewHeader struct {
	ID     string            `json:"id"`
	Mode   string            `json:"mode"`
	Tables []rest.Identifier `json:"tables"`
}
type skewManifest struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Namespace string `json:"namespace"`
	Catalog   string `json:"catalog"`
	Rounds    int    `json:"rounds"`
	Controls  int    `json:"controls"`
	// FaultPlan is set when the run is part of a fault campaign: concurrent
	// rounds the plan's horizon cut off are not started.
	FaultPlan string `json:"fault_plan,omitempty"`
}
type skewEvidence struct {
	header skewHeader
	calls  []*skewCall
}

// CheckSkew replays one version-1 RunSkew history without a catalog. Under a
// fault campaign, concurrent rounds the plan's horizon cut off are counted as
// not started; they may only follow every round that ran.
// Summaries are informational: requests, paired outcomes, and reads determine
// the result. Missing completions are unknown, not rejected. A complete current
// schema proves application, including in later recovery reads. Absence never
// resolves an unknown commit without a validated external completion boundary.
func CheckSkew(ops []history.Op) (SkewResult, error) {
	manifest, rounds, calls, err := parseSkewHistory(ops)
	if err != nil {
		return SkewResult{}, err
	}
	res := SkewResult{}
	setupOK := false
	namespaceOK := false
	for _, call := range calls {
		if call.invoke.Round != "" {
			continue
		}
		switch call.invoke.F {
		case "config":
			setupOK = call.done != nil && call.done.Type == history.OK
		case "create-namespace":
			namespaceOK = call.done != nil && (call.done.Type == history.OK || call.done.Type == history.Fail && call.done.Status == 409)
		}
	}
	if !setupOK || !namespaceOK {
		res.Reasons = append(res.Reasons, "catalog configuration or namespace setup was not established")
	}
	if len(rounds) != manifest.Rounds+2 {
		res.Reasons = append(res.Reasons, "not all requested concurrent rounds and controls were recorded")
	}
	markers := map[string]bool{}
	campaign := manifest.FaultPlan != ""
	for _, round := range rounds {
		if campaign && round.header.Mode == "concurrent" && !skewRoundStarted(round) {
			res.NotStarted++
			continue
		}
		if res.NotStarted != 0 && round.header.Mode == "concurrent" {
			return SkewResult{}, fmt.Errorf("round %s started after a round that was not started", round.header.ID)
		}
		r, err := checkSkewRound(manifest, round, markers)
		if err != nil {
			return SkewResult{}, fmt.Errorf("round %s: %w", round.header.ID, err)
		}
		if !setupOK || !namespaceOK {
			r.Reasons = append(r.Reasons, "run setup is incomplete")
			setSkewVerdict(&r)
		}
		res.Rounds = append(res.Rounds, r)
		res.Anomalies = append(res.Anomalies, r.Anomalies...)
		if len(r.Reasons) != 0 {
			res.InconclusiveRounds++
		}
		rejected := 0
		for _, txn := range r.Transactions {
			switch txn.Outcome {
			case history.OK:
				res.OK++
			case history.Fail:
				res.Failed++
				rejected++
			case history.Info:
				res.Unknown++
				if txn.CommitState == "committed" {
					res.ResolvedCommitted++
				}
			}
		}
		if rejected == 2 {
			res.RejectedRounds++
		}
	}
	if campaign && res.NotStarted >= manifest.Rounds {
		res.Reasons = append(res.Reasons, "no concurrent round started before the plan's horizon")
	}
	res.Verdict = SkewValid
	if len(res.Reasons) != 0 || res.InconclusiveRounds != 0 {
		res.Verdict = SkewInconclusive
	}
	if len(res.Anomalies) != 0 {
		res.Verdict = SkewInvalid
	}
	res.Valid = res.Verdict == SkewValid
	return res, nil
}

func parseSkewHistory(ops []history.Op) (skewManifest, []*skewEvidence, []*skewCall, error) {
	var manifest skewManifest
	var rounds []*skewEvidence
	var calls []*skewCall
	byRound := map[string]*skewEvidence{}
	byID := map[int]*skewCall{}
	usedTables := map[string]bool{}
	setupCalls := map[string]bool{}
	var catalog *url.URL
	fail := func(i int, msg string) (skewManifest, []*skewEvidence, []*skewCall, error) {
		return manifest, nil, nil, fmt.Errorf("history record %d: %s", i, msg)
	}
	for i, op := range ops {
		if op.Version != history.Version {
			return fail(i, "skew checking requires version 1 HTTP evidence")
		}
		if op.Index < 0 || op.Time < 0 || i > 0 && (op.Index <= ops[i-1].Index || op.Time < ops[i-1].Time) {
			return fail(i, "record indexes/timestamps are not ordered")
		}
		if op.Type == history.Meta {
			switch op.F {
			case "run":
				if manifest.RunID != "" || json.Unmarshal(op.Value, &manifest) != nil || manifest.Workload != "skew" || manifest.RunID == "" || manifest.Namespace == "" || manifest.Rounds <= 0 || manifest.Rounds > int(^uint(0)>>1)-2 || manifest.Controls != 2 {
					return fail(i, "invalid or duplicate skew run metadata")
				}
				var err error
				catalog, err = url.Parse(manifest.Catalog)
				if err != nil || catalog.Host == "" || catalog.Scheme != "http" && catalog.Scheme != "https" {
					return fail(i, "invalid catalog origin in run metadata")
				}
			case "skew-round":
				var h skewHeader
				if manifest.RunID == "" || json.Unmarshal(op.Value, &h) != nil || h.ID == "" || h.ID != op.Round || len(h.Tables) != 2 || byRound[h.ID] != nil {
					return fail(i, "invalid or duplicate round header")
				}
				mode := "concurrent"
				if len(rounds) == manifest.Rounds {
					mode = "control-a-first"
				} else if len(rounds) == manifest.Rounds+1 {
					mode = "control-b-first"
				} else if len(rounds) > manifest.Rounds+1 {
					return fail(i, "too many rounds")
				}
				if h.Mode != mode {
					return fail(i, "round modes do not match the run plan")
				}
				for _, table := range h.Tables {
					key := skewTableKey(table)
					if table.Name == "" || !reflect.DeepEqual(table.Namespace, strings.Split(manifest.Namespace, ".")) || usedTables[key] {
						return fail(i, "invalid or reused round table")
					}
					usedTables[key] = true
				}
				round := &skewEvidence{header: h}
				rounds = append(rounds, round)
				byRound[h.ID] = round
			case "skew-round-result": // Derived execution summaries are not a proof.
			case "readback-config": // Observation budget is not a completion barrier.
				if manifest.RunID == "" {
					return fail(i, "read-back configuration precedes run metadata")
				}
			default:
				return fail(i, "unsupported metadata in skew history")
			}
			continue
		}
		if manifest.RunID == "" || op.ID <= 0 {
			return fail(i, "missing run metadata or operation ID")
		}
		if op.Type == history.Invoke {
			if byID[op.ID] != nil || op.Request == nil {
				return fail(i, "duplicate ID or missing request")
			}
			u, err := url.Parse(op.Request.URL)
			if err != nil || u.Scheme != catalog.Scheme || u.Host != catalog.Host || !strings.HasPrefix(u.Path, strings.TrimRight(catalog.Path, "/")+"/v1/") {
				return fail(i, "request does not target the run's catalog")
			}
			method := http.MethodPost
			if op.F == "config" || op.F == "load-table" {
				method = http.MethodGet
			}
			if op.Request.Method != method {
				return fail(i, "unexpected request method")
			}
			switch op.F {
			case "config", "create-namespace":
				if op.Round != "" || op.Phase != "setup" {
					return fail(i, "invalid run setup labels")
				}
				if op.Process != -1 || setupCalls[op.F] {
					return fail(i, "duplicate or invalid run setup operation")
				}
				setupCalls[op.F] = true
			case "create-table", "load-table", "skew-txn":
				if byRound[op.Round] == nil {
					return fail(i, "operation has no round header")
				}
			default:
				return fail(i, "operation is outside the closed skew workload")
			}
			call := &skewCall{invoke: op}
			byID[op.ID] = call
			calls = append(calls, call)
			if op.Round != "" {
				byRound[op.Round].calls = append(byRound[op.Round].calls, call)
			}
		} else {
			call := byID[op.ID]
			if call == nil || call.done != nil {
				return fail(i, "completion without a unique invocation")
			}
			inv := call.invoke
			if op.Type != history.OK && op.Type != history.Fail && op.Type != history.Info || op.F != inv.F || op.Round != inv.Round || op.Phase != inv.Phase || op.Process != inv.Process || !bytes.Equal(op.Value, inv.Value) {
				return fail(i, "completion labels/outcome do not match the invocation")
			}
			if !history.Supported(op) || op.Status != 0 && (op.Response == nil || op.Response.Status != op.Status) || op.Status == 0 && op.Response != nil {
				return fail(i, "completion status and HTTP evidence disagree")
			}
			copy := op
			call.done = &copy
		}
	}
	if manifest.RunID == "" {
		return fail(0, "missing skew run metadata")
	}
	return manifest, rounds, calls, nil
}

type skewSnapshot struct {
	uuid       string
	current    int
	lastColumn int
	schema     map[string]any
	schemas    []map[string]any
}
type skewLabel struct {
	RunID       string          `json:"run_id"`
	Mode        string          `json:"mode"`
	Transaction string          `json:"transaction"`
	Target      rest.Identifier `json:"target"`
	Marker      rest.Field      `json:"marker"`
}
type skewRequirement struct {
	Type     string `json:"type"`
	UUID     string `json:"uuid"`
	SchemaID *int   `json:"current-schema-id"`
}
type skewUpdate struct {
	Action       string          `json:"action"`
	Schema       json.RawMessage `json:"schema"`
	SchemaID     *int            `json:"schema-id"`
	LastColumnID *int            `json:"last-column-id"`
}
type skewChange struct {
	Identifier   *rest.Identifier  `json:"identifier"`
	Requirements []skewRequirement `json:"requirements"`
	Updates      []skewUpdate      `json:"updates"`
}

// skewRoundStarted reports whether anything beyond setup and baseline reads
// was sent for the round.
func skewRoundStarted(round *skewEvidence) bool {
	for _, call := range round.calls {
		if call.invoke.Phase != "setup" && call.invoke.Phase != "baseline" {
			return true
		}
	}
	return false
}

func checkSkewRound(manifest skewManifest, evidence *skewEvidence, markers map[string]bool) (SkewRoundResult, error) {
	h := evidence.header
	r := SkewRoundResult{Round: h.ID, Mode: h.Mode}
	var txns [2]*skewCall
	reads := map[string]map[string]*skewCall{"baseline": {}, "final": {}, "control-verify": {}}
	observations := map[string][]*skewCall{}
	created := map[string]*skewCall{}
	for p := range r.Transactions {
		r.Transactions[p].CommitState = "not-invoked"
	}
	for _, call := range evidence.calls {
		op := call.invoke
		if op.F == "skew-txn" {
			if op.Phase != "workload" || op.Process < 0 || op.Process > 1 || txns[op.Process] != nil {
				return r, fmt.Errorf("duplicate or incorrectly labelled transaction")
			}
			txns[op.Process] = call
			t := &r.Transactions[op.Process]
			t.ID = op.ID
			t.Outcome = history.Info
			t.CommitState = "unknown"
			if call.done != nil {
				t.Outcome = call.done.Type
			}
			if t.Outcome == history.OK {
				t.CommitState = "committed"
				t.Resolution = "acknowledged"
			}
			if t.Outcome == history.Fail {
				t.CommitState = "rejected"
			}
			continue
		}
		if op.Process != -1 {
			return r, fmt.Errorf("table setup/read must use coordinator process")
		}
		if op.F == "create-table" {
			var body struct {
				Name        string `json:"name"`
				StageCreate bool   `json:"stage-create"`
			}
			if op.Phase != "setup" || json.Unmarshal(op.Request.Body, &body) != nil || body.StageCreate {
				return r, fmt.Errorf("invalid table creation evidence")
			}
			key := skewTableKey(rest.Identifier{Namespace: strings.Split(manifest.Namespace, "."), Name: body.Name})
			endpointKey, err := skewReadKey(strings.TrimRight(op.Request.URL, "/") + "/" + url.PathEscape(body.Name))
			if err != nil || endpointKey != key {
				return r, fmt.Errorf("table creation URL does not match its identifier")
			}
			if !skewRoundHasTable(h, key) || created[key] != nil {
				return r, fmt.Errorf("unexpected or duplicate table creation")
			}
			created[key] = call
		} else if op.F == "load-table" {
			key, err := skewReadKey(op.Request.URL)
			if err != nil || !skewRoundHasTable(h, key) {
				return r, fmt.Errorf("unexpected or duplicate table observation")
			}
			if op.Phase == "recovery" {
				observations[key] = append(observations[key], call)
				continue
			}
			if reads[op.Phase] == nil || reads[op.Phase][key] != nil {
				return r, fmt.Errorf("unexpected or duplicate table observation")
			}
			reads[op.Phase][key] = call
			if op.Phase == "final" {
				observations[key] = append(observations[key], call)
			}
		}
	}
	var base [2]skewSnapshot
	for p, table := range h.Tables {
		key := skewTableKey(table)
		if created[key] == nil || created[key].done == nil || created[key].done.Type != history.OK {
			r.Reasons = append(r.Reasons, "table creation was not established")
		}
		var err error
		base[p], err = readSkewSnapshot(reads["baseline"][key])
		if err != nil {
			r.Reasons = append(r.Reasons, "baseline: "+err.Error())
			continue
		}
		if created[key] != nil && created[key].done != nil && created[key].done.Index >= reads["baseline"][key].invoke.Index {
			return r, fmt.Errorf("baseline precedes table creation")
		}
		fields, _ := base[p].schema["fields"].([]any)
		if len(fields) != 1 || !skewFieldMatches(fields[0], rest.Field{ID: 1, Name: "id", Required: true, Type: "long"}) {
			return r, fmt.Errorf("baseline is not the fresh workload schema")
		}
	}
	// Without a trustworthy baseline, do not invent a conflicting pair.
	if len(r.Reasons) != 0 {
		setSkewVerdict(&r)
		return r, nil
	}
	routingBase := skewTableRoute(reads["baseline"][skewTableKey(h.Tables[0])].invoke.Request.URL)
	for _, call := range evidence.calls {
		rawURL := call.invoke.Request.URL
		if call.invoke.F == "skew-txn" {
			u, _ := url.Parse(rawURL)
			if strings.TrimSuffix(u.EscapedPath(), "/transactions/commit") != routingBase {
				return r, fmt.Errorf("transaction routing prefix differs from the baseline")
			}
		} else {
			if call.invoke.F == "create-table" {
				rawURL = strings.TrimRight(rawURL, "/") + "/table"
			}
			if skewTableRoute(rawURL) != routingBase {
				return r, fmt.Errorf("table routing prefixes differ within the round")
			}
		}
	}
	var expected [2]map[string]any
	var labels [2]skewLabel
	for p, call := range txns {
		if call == nil {
			r.Reasons = append(r.Reasons, "transaction was not invoked")
			continue
		}
		for _, table := range h.Tables {
			read := reads["baseline"][skewTableKey(table)]
			if read.done.Index >= call.invoke.Index {
				return r, fmt.Errorf("transaction precedes its baseline")
			}
		}
		var err error
		labels[p], expected[p], err = validateSkewRequest(manifest, h, p, call, base)
		if err != nil {
			return r, err
		}
		if markers[labels[p].Marker.Name] {
			return r, fmt.Errorf("schema marker reused across transactions")
		}
		markers[labels[p].Marker.Name] = true
		if call.done != nil && call.done.Type == history.Fail && call.done.Status != 409 {
			r.Reasons = append(r.Reasons, "transaction rejection was not a requirement conflict")
		}
	}
	if txns[0] != nil && txns[1] != nil && txns[0].done != nil && txns[1].done != nil {
		r.OverlapKnown = true
		r.Overlapped = skewCallsOverlap(txns[0], txns[1])
	}
	var final [2]skewSnapshot
	var trusted [2]bool
	var rejectedMarkers [2]bool
	for p, table := range h.Tables {
		samples := observations[skewTableKey(table)]
		var read *skewCall
		for _, sample := range samples {
			ordered := true
			for _, call := range txns {
				if call != nil && (sample.invoke.Index <= call.invoke.Index ||
					call.done != nil && sample.invoke.Index <= call.done.Index) {
					ordered = false
				}
			}
			if !ordered {
				return r, fmt.Errorf("final/recovery observation precedes transaction attempts")
			}
			snapshot, err := readSkewSnapshot(sample)
			if err == nil && snapshot.uuid == base[p].uuid {
				txn := &r.Transactions[p]
				if txn.Outcome == history.Fail && skewHasMarker(snapshot, labels[p].Marker) {
					rejectedMarkers[p] = true
				}
				complete := snapshot.current != base[p].current && reflect.DeepEqual(snapshot.schema, expected[p])
				if txns[p] != nil && txn.Outcome != history.OK && complete {
					txn.CommitState = "committed"
					txn.Resolution = "complete-final-schema"
					txn.EvidenceIDs = []int{sample.invoke.ID}
				}
			}
			read = sample
		}
		var err error
		final[p], err = readSkewSnapshot(read)
		if err != nil {
			r.Reasons = append(r.Reasons, "final observation: "+err.Error())
			continue
		}
		if final[p].uuid != base[p].uuid {
			r.Reasons = append(r.Reasons, "table UUID changed after the baseline")
			continue
		}
		ordered := true
		for _, call := range txns {
			if call != nil && (read.invoke.Index <= call.invoke.Index || call.done != nil && read.invoke.Index <= call.done.Index) {
				ordered = false
			}
		}
		if !ordered {
			r.Reasons = append(r.Reasons, "final observation does not follow the transaction attempts")
			continue
		}
		trusted[p] = true
	}
	// Positive observations can settle an unknown as committed. They do not
	// erase that commitment if a later read no longer contains its effects.
	settled := true
	for p, call := range txns {
		if call != nil && r.Transactions[p].CommitState == "unknown" {
			settled = false
		}
	}
	for p, call := range txns {
		if call == nil {
			continue
		}
		txn := &r.Transactions[p]
		rejectedEffect := rejectedMarkers[p]
		if trusted[p] {
			complete := final[p].current != base[p].current && reflect.DeepEqual(final[p].schema, expected[p])
			if complete && txn.Outcome != history.OK {
				txn.CommitState = "committed"
				txn.Resolution = "complete-final-schema"
			}
			if txn.CommitState == "committed" && txn.Outcome != history.Fail && !complete {
				if settled {
					addSkewAnomaly(&r, "lost", "catalog-contract", []int{txn.ID}, "committed schema update is missing or incomplete in the settled current schema")
				} else {
					r.Reasons = append(r.Reasons, "missing committed schema update cannot be checked while another transaction remains unresolved")
				}
			}
			if txn.Outcome == history.Fail && skewHasMarker(final[p], labels[p].Marker) {
				rejectedEffect = true
			}
		}
		if rejectedEffect {
			addSkewAnomaly(&r, "phantom-failed", "catalog-contract", []int{txn.ID}, "rejected transaction left its unique schema field in catalog metadata")
		}
		if txn.CommitState == "unknown" {
			r.Reasons = append(r.Reasons, "unknown transaction outcome is unresolved; absence does not prove rejection")
		}
	}
	if h.Mode == "concurrent" {
		if r.Transactions[0].CommitState == "committed" && r.Transactions[1].CommitState == "committed" {
			addSkewAnomaly(&r, "write-skew", "serializable-model", []int{r.Transactions[0].ID, r.Transactions[1].ID}, "both transactions committed despite opposing guards from the same baseline")
		}
	} else {
		first := 0
		if h.Mode == "control-b-first" {
			first = 1
		}
		second := 1 - first
		verify := reads["control-verify"][skewTableKey(h.Tables[first])]
		observed, err := readSkewSnapshot(verify)
		established := txns[first] != nil && txns[first].done != nil && txns[first].done.Type == history.OK && err == nil && observed.uuid == base[first].uuid && observed.current != base[first].current && reflect.DeepEqual(observed.schema, expected[first]) && verify.invoke.Index > txns[first].done.Index
		if txns[second] != nil {
			if !established || verify.done.Index >= txns[second].invoke.Index {
				return r, fmt.Errorf("stale control attempt lacks prior verification of the first update")
			}
			if r.Transactions[second].CommitState == "committed" {
				addSkewAnomaly(&r, "stale-requirement", "catalog-contract", []int{r.Transactions[second].ID}, "transaction committed after verification that its requirement on the unchanged table was false")
			}
		} else {
			r.Reasons = append(r.Reasons, "sequential stale-requirement control was not exercised")
		}
	}
	setSkewVerdict(&r)
	return r, nil
}

func validateSkewRequest(m skewManifest, h skewHeader, p int, call *skewCall, base [2]skewSnapshot) (skewLabel, map[string]any, error) {
	var label skewLabel
	bad := func(msg string) (skewLabel, map[string]any, error) {
		return label, nil, fmt.Errorf("transaction %d: %s", call.invoke.ID, msg)
	}
	if json.Unmarshal(call.invoke.Value, &label) != nil || label.RunID != m.RunID || label.Mode != h.Mode || label.Transaction != fmt.Sprintf("t%d", p+1) || !reflect.DeepEqual(label.Target, h.Tables[p]) || label.Marker.Name == "" || label.Marker.ID != base[p].lastColumn+1 || label.Marker.ID > 2147483647 || label.Marker.Required || label.Marker.Type != "long" {
		return bad("invalid transaction labels or marker")
	}
	if bytes.Contains(call.invoke.Request.Body, []byte("[REDACTED]")) {
		return bad("redaction removed model-relevant request evidence")
	}
	u, err := url.Parse(call.invoke.Request.URL)
	if err != nil || !strings.HasSuffix(u.Path, "/transactions/commit") {
		return bad("wrong commit endpoint")
	}
	var body struct {
		Changes []skewChange `json:"table-changes"`
	}
	if json.Unmarshal(call.invoke.Request.Body, &body) != nil || len(body.Changes) != 2 {
		return bad("missing complete table changes")
	}
	seen := map[int]bool{}
	var expected map[string]any
	for _, change := range body.Changes {
		if change.Identifier == nil {
			return bad("missing table identifier")
		}
		q := -1
		for i, table := range h.Tables {
			if reflect.DeepEqual(*change.Identifier, table) {
				q = i
			}
		}
		if q < 0 || seen[q] || len(change.Requirements) != 2 || change.Updates == nil {
			return bad("unexpected/duplicate table, guards, or null updates")
		}
		seen[q] = true
		uuid, schemaID := false, false
		for _, req := range change.Requirements {
			switch req.Type {
			case "assert-table-uuid":
				if uuid || req.UUID != base[q].uuid {
					return bad("UUID guard differs from baseline")
				}
				uuid = true
			case "assert-current-schema-id":
				if schemaID || req.SchemaID == nil || *req.SchemaID != base[q].current {
					return bad("schema guard differs from baseline")
				}
				schemaID = true
			default:
				return bad("unsupported requirement")
			}
		}
		if !uuid || !schemaID {
			return bad("missing UUID or schema guard")
		}
		if q != p {
			if len(change.Updates) != 0 {
				return bad("other table must have requirement-only updates")
			}
			continue
		}
		if len(change.Updates) != 2 || change.Updates[0].Action != "add-schema" || change.Updates[0].LastColumnID == nil || *change.Updates[0].LastColumnID != label.Marker.ID || change.Updates[1].Action != "set-current-schema" || change.Updates[1].SchemaID == nil || *change.Updates[1].SchemaID != -1 {
			return bad("updates are outside the schema-change model")
		}
		_, expected, err = normalizeSkewSchema(change.Updates[0].Schema)
		if err != nil {
			return bad("malformed added schema")
		}
		copy := cloneSkewSchema(base[p].schema)
		fields := append([]any(nil), copy["fields"].([]any)...)
		markerJSON, _ := json.Marshal(label.Marker)
		var marker any
		decodeSkewJSON(markerJSON, &marker)
		copy["fields"] = append(fields, marker)
		if !reflect.DeepEqual(copy, expected) {
			return bad("new schema must preserve baseline fields and add exactly its unique marker")
		}
	}
	return label, expected, nil
}

func readSkewSnapshot(call *skewCall) (skewSnapshot, error) {
	var out skewSnapshot
	if call == nil || call.done == nil || call.done.Type != history.OK || call.done.Response == nil {
		return out, fmt.Errorf("successful table read unavailable")
	}
	var body struct {
		Metadata struct {
			UUID    string            `json:"table-uuid"`
			Current *int              `json:"current-schema-id"`
			Last    *int              `json:"last-column-id"`
			Schemas []json.RawMessage `json:"schemas"`
		} `json:"metadata"`
	}
	if json.Unmarshal(call.done.Response.Body, &body) != nil || body.Metadata.UUID == "" || body.Metadata.Current == nil || body.Metadata.Last == nil || *body.Metadata.Last < 1 || *body.Metadata.Last > 2147483647 {
		return out, fmt.Errorf("identity/schema metadata is malformed")
	}
	out.uuid, out.current, out.lastColumn = body.Metadata.UUID, *body.Metadata.Current, *body.Metadata.Last
	if strings.Contains(out.uuid, "[REDACTED]") {
		return out, fmt.Errorf("table identity was redacted")
	}
	ids := map[int]bool{}
	for _, raw := range body.Metadata.Schemas {
		if bytes.Contains(raw, []byte("[REDACTED]")) {
			return out, fmt.Errorf("schema evidence was redacted")
		}
		id, schema, err := normalizeSkewSchema(raw)
		if err != nil || id == nil || *id < 0 || *id > 2147483647 || ids[*id] {
			return out, fmt.Errorf("schema history is malformed")
		}
		ids[*id] = true
		for _, field := range schema["fields"].([]any) {
			id, _ := field.(map[string]any)["id"].(json.Number).Int64() // normalizeSkewSchema validated the field.
			if id > int64(out.lastColumn) {
				return out, fmt.Errorf("last-column-id is inconsistent with schema fields")
			}
		}
		out.schemas = append(out.schemas, schema)
		if *id == out.current {
			out.schema = schema
		}
	}
	if out.schema == nil {
		return out, fmt.Errorf("current schema is absent from schema history")
	}
	return out, nil
}

func normalizeSkewSchema(raw []byte) (*int, map[string]any, error) {
	var header struct {
		ID *int `json:"schema-id"`
	}
	var schema map[string]any
	if json.Unmarshal(raw, &header) != nil || decodeSkewJSON(raw, &schema) != nil || schema["type"] != "struct" {
		return nil, nil, fmt.Errorf("invalid schema")
	}
	fields, ok := schema["fields"].([]any)
	if !ok || len(fields) == 0 {
		return nil, nil, fmt.Errorf("missing schema fields")
	}
	fieldIDs, names := map[int64]bool{}, map[string]bool{}
	for _, field := range fields {
		f, ok := field.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("invalid field")
		}
		number, ok := f["id"].(json.Number)
		if !ok {
			return nil, nil, fmt.Errorf("invalid field ID")
		}
		id, err := number.Int64()
		name, nameOK := f["name"].(string)
		_, requiredOK := f["required"].(bool)
		typ, typeOK := f["type"].(string)
		if err != nil || id < 1 || id > 2147483647 || fieldIDs[id] || !nameOK || name == "" || names[name] || !requiredOK || !typeOK || typ == "" {
			return nil, nil, fmt.Errorf("invalid or unsupported schema field")
		}
		fieldIDs[id], names[name] = true, true
	}
	delete(schema, "schema-id")
	if ids, ok := schema["identifier-field-ids"].([]any); ok && len(ids) == 0 {
		delete(schema, "identifier-field-ids")
	}
	return header.ID, schema, nil
}

func decodeSkewJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}
func cloneSkewSchema(schema map[string]any) map[string]any {
	raw, _ := json.Marshal(schema)
	var copy map[string]any
	decodeSkewJSON(raw, &copy)
	return copy
}
func skewFieldMatches(value any, field rest.Field) bool {
	raw, _ := json.Marshal(value)
	var actual rest.Field
	return json.Unmarshal(raw, &actual) == nil && actual == field
}
func skewHasMarker(snapshot skewSnapshot, marker rest.Field) bool {
	for _, schema := range snapshot.schemas {
		for _, field := range schema["fields"].([]any) {
			if skewFieldMatches(field, marker) {
				return true
			}
		}
	}
	return false
}
func skewTableKey(table rest.Identifier) string { raw, _ := json.Marshal(table); return string(raw) }
func skewRoundHasTable(h skewHeader, key string) bool {
	for _, table := range h.Tables {
		if skewTableKey(table) == key {
			return true
		}
	}
	return false
}
func skewReadKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	n := len(parts)
	if n < 5 || parts[n-4] != "namespaces" || parts[n-2] != "tables" {
		return "", fmt.Errorf("invalid table read URL")
	}
	ns, e1 := url.PathUnescape(parts[n-3])
	name, e2 := url.PathUnescape(parts[n-1])
	if e1 != nil || e2 != nil {
		return "", fmt.Errorf("invalid table identifier encoding")
	}
	return skewTableKey(rest.Identifier{Namespace: strings.Split(ns, "\x1f"), Name: name}), nil
}

func skewTableRoute(rawURL string) string {
	u, _ := url.Parse(rawURL) // Every request URL was parsed by parseSkewHistory.
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) < 5 {
		return ""
	}
	return "/" + strings.Join(parts[:len(parts)-4], "/")
}

// skewCallsOverlap compares record indexes, not times. The recorder numbers
// records in the order it makes them, which the times only follow: on a coarse
// clock, such as Windows', distinct records can share a time.
func skewCallsOverlap(a, b *skewCall) bool {
	return (a.done == nil || b.invoke.Index < a.done.Index) && (b.done == nil || a.invoke.Index < b.done.Index)
}
func addSkewAnomaly(r *SkewRoundResult, kind, basis string, ids []int, detail string) {
	r.Anomalies = append(r.Anomalies, SkewAnomaly{Round: r.Round, Kind: kind, Basis: basis, OperationIDs: ids, Detail: detail})
}
func setSkewVerdict(r *SkewRoundResult) {
	r.Verdict = SkewValid
	if len(r.Reasons) != 0 {
		r.Verdict = SkewInconclusive
	}
	if len(r.Anomalies) != 0 {
		r.Verdict = SkewInvalid
	}
}
