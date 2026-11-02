package checker

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
)

// MaintenanceModelVersion assumes the catalog returns every retained snapshot.
const MaintenanceModelVersion = "calve-maintenance-v1"

// MaintenanceCurrentModelVersion is for catalogs that return only the current
// snapshot. It checks main and outcomes, but makes no retention claim.
const MaintenanceCurrentModelVersion = "calve-maintenance-current-v1"

type MaintenanceAnomaly struct {
	Kind        string  `json:"kind"`
	Basis       string  `json:"basis"` // catalog-contract
	Snapshots   []int64 `json:"snapshots,omitempty"`
	EvidenceIDs []int   `json:"evidence_ids"`
	Detail      string  `json:"detail"`
}

type MaintenanceResult struct {
	Model             string               `json:"model"`
	Valid             bool                 `json:"valid"`
	Verdict           Verdict              `json:"verdict"`
	Anomalies         []MaintenanceAnomaly `json:"anomalies,omitempty"`
	Reasons           []string             `json:"reasons,omitempty"`
	Appends           int                  `json:"appends"`
	AppendsOK         int                  `json:"appends_ok"`
	AppendsFailed     int                  `json:"appends_failed"`
	AppendsUnknown    int                  `json:"appends_unknown"`
	ResolvedCommitted int                  `json:"resolved_committed"`
	Unresolved        int                  `json:"unresolved"`
	Expirations       int                  `json:"expirations"`
	ExpirationsOK     int                  `json:"expirations_ok"`
	RacingExpirations int                  `json:"racing_expirations"`
}

type maintenanceManifest struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Model     string `json:"model"`
	Table     string `json:"table"`
	Appends   int    `json:"appends"`
}

type maintenanceCall struct {
	invoke, done *history.Op
	label        maintenanceLabel
}

type maintenanceLabel struct {
	Kind      string  `json:"kind"`
	Snapshot  int64   `json:"snapshot"`
	Snapshots []int64 `json:"snapshots"`
}

// maintenanceView is one successful table load.
type maintenanceView struct {
	call      *maintenanceCall
	uuid      string
	main      *int64 // refs.main
	current   *int64 // current-snapshot-id, which the workload guards on
	snapshots map[int64]bool
}

// CheckMaintenance checks appends racing snapshot expiration on one table.
// Expiration may remove only the snapshots it lists, so an acknowledged or
// observed snapshot may disappear only after a non-rejected expiration that
// lists it was sent, and main must keep pointing at a retained snapshot no
// older than the last acknowledged append. Absence never proves rejection.
func CheckMaintenance(ops []history.Op) (MaintenanceResult, error) {
	m, calls, err := parseMaintenanceHistory(ops)
	if err != nil {
		return MaintenanceResult{}, err
	}
	res := MaintenanceResult{Model: m.Model}
	var views []maintenanceView
	var appends, expirations []*maintenanceCall
	byProcess := map[int][]*maintenanceCall{}
	uuid := ""
	cut := len(ops)
	firstCommit := len(ops)
	lastCommit := -1
	for _, call := range calls {
		byProcess[call.invoke.Process] = append(byProcess[call.invoke.Process], call)
		if call.invoke.F == "commit-table" {
			firstCommit = min(firstCommit, call.invoke.Index)
			lastCommit = max(lastCommit, call.invoke.Index)
			if call.done != nil {
				lastCommit = max(lastCommit, call.done.Index)
			}
			if call.label.Kind == "append" {
				appends = append(appends, call)
			} else {
				expirations = append(expirations, call)
			}
			continue
		}
		if call.done == nil || call.done.Type != history.OK || call.done.Response == nil {
			continue
		}
		view, err := readMaintenanceView(call)
		if err != nil {
			return res, fmt.Errorf("maintenance observation %d: %w", call.invoke.ID, err)
		}
		if uuid == "" {
			uuid = view.uuid
		}
		if view.uuid != uuid {
			cut = min(cut, call.invoke.Index)
			continue
		}
		if view.call.done.Index < cut {
			views = append(views, view)
		}
	}
	if cut < len(ops) {
		res.Reasons = append(res.Reasons, fmt.Sprintf("table UUID changed; only evidence before record %d is used", cut))
	}

	// The baseline is the first record after the run, of a fresh empty table.
	if len(calls) == 0 || calls[0].invoke.Phase != "baseline" || len(views) == 0 || views[0].call != calls[0] ||
		views[0].main != nil || len(views[0].snapshots) != 0 || views[0].call.done.Index > firstCommit {
		return res, fmt.Errorf("maintenance history: baseline is not a completed load of an empty table before any commit")
	}
	if err := validateMaintenanceRequests(m, uuid, byProcess); err != nil {
		return res, err
	}
	res.Appends, res.Expirations = len(appends), len(expirations)

	// Appends: outcomes, proof of effect, and rejected effects.
	committed := map[int64]int{} // snapshot -> record index after which it is known committed
	appended := map[int64]*maintenanceCall{}
	for _, call := range appends {
		id := call.label.Snapshot
		appended[id] = call
		proof := maintenanceProof(views, id)
		switch maintenanceOutcome(call) {
		case history.OK:
			res.AppendsOK++
			committed[id] = call.done.Index
		case history.Fail:
			res.AppendsFailed++
			if proof != nil {
				res.Anomalies = append(res.Anomalies, MaintenanceAnomaly{Kind: "phantom-failed", Basis: "catalog-contract",
					Snapshots: []int64{id}, EvidenceIDs: []int{call.invoke.ID, proof.call.invoke.ID},
					Detail: "a rejected append's snapshot is present in the table"})
			}
		default:
			res.AppendsUnknown++
			if proof != nil {
				res.ResolvedCommitted++
				committed[id] = proof.call.done.Index
			} else {
				res.Unresolved++
			}
		}
	}

	// Snapshots from no append sent before the load finished.
	reported := map[int64]bool{}
	for _, view := range views {
		for _, id := range sortedSnapshots(view.snapshots) {
			if call := appended[id]; reported[id] || call != nil && call.invoke.Index < view.call.done.Index {
				continue
			}
			reported[id] = true
			res.Anomalies = append(res.Anomalies, MaintenanceAnomaly{Kind: "unexpected", Basis: "catalog-contract",
				Snapshots: []int64{id}, EvidenceIDs: []int{view.call.invoke.ID}, Detail: "a snapshot no earlier append adds is present"})
		}
	}

	// A committed snapshot may vanish only after a non-rejected expiration
	// listing it was sent. If only rejected expirations list it, they applied.
	// The current-snapshot model makes no retention claim.
	retained := sortedKeys(committed)
	if m.Model == MaintenanceCurrentModelVersion {
		retained = nil
	}
	for _, id := range retained {
		for _, view := range views {
			if view.call.invoke.Index <= committed[id] || view.snapshots[id] {
				continue
			}
			explained, rejected := false, []int{}
			for _, e := range expirations {
				if e.invoke.Index > view.call.done.Index || !containsSnapshot(e.label.Snapshots, id) {
					continue
				}
				if maintenanceOutcome(e) == history.Fail {
					rejected = append(rejected, e.invoke.ID)
				} else {
					explained = true
				}
			}
			if explained {
				break
			}
			kind, detail := "lost-snapshot", "a committed snapshot is missing although no expiration listed it"
			evidence := []int{appended[id].invoke.ID, view.call.invoke.ID}
			if len(rejected) > 0 {
				kind, detail = "phantom-failed", "a committed snapshot is missing and only rejected expirations listed it"
				evidence = append(evidence, rejected...)
			}
			sort.Ints(evidence)
			res.Anomalies = append(res.Anomalies, MaintenanceAnomaly{Kind: kind, Basis: "catalog-contract",
				Snapshots: []int64{id}, EvidenceIDs: evidence, Detail: detail})
			break
		}
	}

	// main must stay on a retained snapshot no older than the last append
	// acknowledged before the load started. Append IDs grow along the chain.
	for _, view := range views {
		var last *maintenanceCall
		for _, call := range appends {
			if maintenanceOutcome(call) == history.OK && call.done.Index < view.call.invoke.Index &&
				(last == nil || call.label.Snapshot > last.label.Snapshot) {
				last = call
			}
		}
		if last == nil {
			continue
		}
		switch {
		case view.main == nil || !view.snapshots[*view.main]:
			res.Anomalies = append(res.Anomalies, MaintenanceAnomaly{Kind: "dangling-main", Basis: "catalog-contract",
				Snapshots: []int64{last.label.Snapshot}, EvidenceIDs: []int{last.invoke.ID, view.call.invoke.ID},
				Detail: "main is missing or points at a removed snapshot after an acknowledged append"})
		case *view.main < last.label.Snapshot:
			res.Anomalies = append(res.Anomalies, MaintenanceAnomaly{Kind: "main-regressed", Basis: "catalog-contract",
				Snapshots: []int64{*view.main, last.label.Snapshot}, EvidenceIDs: []int{last.invoke.ID, view.call.invoke.ID},
				Detail: "main points at a snapshot older than an acknowledged append"})
		default:
			continue
		}
		break
	}

	// Coverage: an acknowledged expiration that raced an append.
	for _, e := range expirations {
		if maintenanceOutcome(e) != history.OK {
			continue
		}
		res.ExpirationsOK++
		for _, a := range appends {
			if maintenanceOverlap(e, a, len(ops)) {
				res.RacingExpirations++
				break
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
	if len(committed) == 0 {
		res.Reasons = append(res.Reasons, "no append is known to have committed")
	}
	if res.ExpirationsOK == 0 {
		res.Reasons = append(res.Reasons, "no expiration was acknowledged")
	} else if res.RacingExpirations == 0 {
		res.Reasons = append(res.Reasons, "no acknowledged expiration overlapped an append")
	}
	if res.Unresolved > 0 {
		res.Reasons = append(res.Reasons, "unknown appends remain unresolved; absence does not prove rejection")
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

func parseMaintenanceHistory(ops []history.Op) (m maintenanceManifest, calls []*maintenanceCall, err error) {
	bad := func(msg string) (maintenanceManifest, []*maintenanceCall, error) {
		return m, nil, fmt.Errorf("maintenance history: %s", msg)
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" || json.Unmarshal(ops[0].Value, &m) != nil ||
		m.Workload != "maintenance-race" || !strings.HasPrefix(m.RunID, "maint_") || m.Table != m.RunID || m.Appends <= 0 ||
		m.Model != MaintenanceModelVersion && m.Model != MaintenanceCurrentModelVersion {
		return bad("missing or invalid maintenance run manifest")
	}
	for _, level := range strings.Split(m.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return bad("empty namespace level")
		}
	}
	byID := map[int]*maintenanceCall{}
	route := ""
	for i := range ops[1:] {
		op := &ops[i+1]
		if op.Index != i+1 || op.Type == history.Meta || op.ID <= 0 || op.Round != "" {
			return bad("records are out of order, unlabeled or carry a round")
		}
		if op.Type != history.Invoke {
			call := byID[op.ID]
			if call == nil || call.done != nil || op.F != call.invoke.F || op.Process != call.invoke.Process || op.Time < call.invoke.Time {
				return bad("unpaired or mismatched completion")
			}
			if !history.Supported(*op) {
				return bad("completion type not supported by its HTTP status")
			}
			call.done = op
			continue
		}
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
		call := &maintenanceCall{invoke: op}
		switch {
		case op.F == "load-table" && op.Request.Method == "GET":
			ok := op.Process == -1 && (op.Phase == "baseline" || op.Phase == "final" || op.Phase == "recovery") ||
				(op.Process == 0 || op.Process == 1) && op.Phase == "workload"
			if !ok {
				return bad("load has an unsupported process or phase")
			}
		case op.F == "commit-table" && op.Request.Method == "POST" && op.Phase == "workload":
			if json.Unmarshal(op.Value, &call.label) != nil ||
				!(op.Process == 0 && call.label.Kind == "append" && call.label.Snapshot > 0 && call.label.Snapshot <= int64(m.Appends)) &&
					!(op.Process == 1 && call.label.Kind == "expire" && len(call.label.Snapshots) > 0) {
				return bad("commit has an invalid label or process")
			}
		default:
			return bad("unsupported operation " + op.F)
		}
		byID[op.ID] = call
		calls = append(calls, call)
	}
	return m, calls, nil
}

// validateMaintenanceRequests requires the exact requests the workload sends:
// each append guarded on the head the appender last knew, and each expiration
// listing the snapshots its preceding load showed, except main's head.
func validateMaintenanceRequests(m maintenanceManifest, uuid string, byProcess map[int][]*maintenanceCall) error {
	var head *int64
	next := int64(1)
	for _, call := range byProcess[0] {
		if call.invoke.F == "load-table" {
			if call.done != nil && call.done.Type == history.OK {
				view, err := readMaintenanceView(call)
				if err != nil {
					return fmt.Errorf("maintenance observation %d: %w", call.invoke.ID, err)
				}
				head = view.current
			}
			continue
		}
		id := call.label.Snapshot
		if id != next {
			return fmt.Errorf("maintenance append %d is out of sequence", call.invoke.ID)
		}
		next++
		var body struct {
			Requirements []map[string]any `json:"requirements"`
			Updates      []struct {
				Action   string `json:"action"`
				Snapshot struct {
					ID     *int64 `json:"snapshot-id"`
					Parent *int64 `json:"parent-snapshot-id"`
				} `json:"snapshot"`
				Ref  string `json:"ref-name"`
				Type string `json:"type"`
				ID   *int64 `json:"snapshot-id"`
			} `json:"updates"`
		}
		if err := json.Unmarshal(call.invoke.Request.Body, &body); err != nil {
			return fmt.Errorf("maintenance append %d: decode request: %w", call.invoke.ID, err)
		}
		var guard any
		if head != nil {
			guard = float64(*head)
		}
		want := []map[string]any{
			{"type": "assert-table-uuid", "uuid": uuid},
			{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": guard},
		}
		if !reflect.DeepEqual(body.Requirements, want) || len(body.Updates) != 2 ||
			body.Updates[0].Action != "add-snapshot" || body.Updates[0].Snapshot.ID == nil || *body.Updates[0].Snapshot.ID != id ||
			!reflect.DeepEqual(body.Updates[0].Snapshot.Parent, head) ||
			body.Updates[1].Action != "set-snapshot-ref" || body.Updates[1].Ref != "main" || body.Updates[1].Type != "branch" ||
			body.Updates[1].ID == nil || *body.Updates[1].ID != id {
			return fmt.Errorf("maintenance append %d is not a main-guarded append of its snapshot", call.invoke.ID)
		}
		if maintenanceOutcome(call) == history.OK {
			head = &id
		}
	}
	var seen *maintenanceView
	for _, call := range byProcess[1] {
		if call.invoke.F == "load-table" {
			seen = nil
			if call.done != nil && call.done.Type == history.OK {
				view, err := readMaintenanceView(call)
				if err != nil {
					return fmt.Errorf("maintenance observation %d: %w", call.invoke.ID, err)
				}
				seen = &view
			}
			continue
		}
		if seen == nil || seen.current == nil {
			return fmt.Errorf("maintenance expiration %d has no preceding load showing main", call.invoke.ID)
		}
		var want []int64
		for _, id := range sortedSnapshots(seen.snapshots) {
			if id != *seen.current {
				want = append(want, id)
			}
		}
		listed := append([]int64(nil), call.label.Snapshots...)
		sort.Slice(listed, func(i, j int) bool { return listed[i] < listed[j] })
		var body struct {
			Requirements []map[string]any `json:"requirements"`
			Updates      []struct {
				Action string  `json:"action"`
				IDs    []int64 `json:"snapshot-ids"`
			} `json:"updates"`
		}
		if json.Unmarshal(call.invoke.Request.Body, &body) != nil ||
			!reflect.DeepEqual(body.Requirements, []map[string]any{{"type": "assert-table-uuid", "uuid": uuid}}) ||
			len(body.Updates) != 1 || body.Updates[0].Action != "remove-snapshots" ||
			!reflect.DeepEqual(body.Updates[0].IDs, call.label.Snapshots) || !reflect.DeepEqual(listed, want) {
			return fmt.Errorf("maintenance expiration %d does not expire exactly its load's non-head snapshots", call.invoke.ID)
		}
		seen = nil
	}
	return nil
}

func readMaintenanceView(call *maintenanceCall) (maintenanceView, error) {
	var body struct {
		Metadata struct {
			UUID      string `json:"table-uuid"`
			Current   *int64 `json:"current-snapshot-id"`
			Snapshots []struct {
				ID *int64 `json:"snapshot-id"`
			} `json:"snapshots"`
			Refs map[string]struct {
				ID *int64 `json:"snapshot-id"`
			} `json:"refs"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(call.done.Response.Body, &body); err != nil {
		return maintenanceView{}, fmt.Errorf("decode metadata: %w", err)
	}
	if body.Metadata.UUID == "" {
		return maintenanceView{}, fmt.Errorf("metadata lacks a table UUID")
	}
	view := maintenanceView{call: call, uuid: body.Metadata.UUID, snapshots: map[int64]bool{}}
	if body.Metadata.Current != nil && *body.Metadata.Current != -1 {
		view.current = body.Metadata.Current
	}
	for _, snapshot := range body.Metadata.Snapshots {
		if snapshot.ID == nil || view.snapshots[*snapshot.ID] {
			return maintenanceView{}, fmt.Errorf("missing or duplicate snapshot ID")
		}
		view.snapshots[*snapshot.ID] = true
	}
	if ref, ok := body.Metadata.Refs["main"]; ok {
		if ref.ID == nil {
			return maintenanceView{}, fmt.Errorf("main ref lacks a snapshot ID")
		}
		view.main = ref.ID
	}
	return view, nil
}

// maintenanceProof is the load that first finished showing the snapshot.
func maintenanceProof(views []maintenanceView, id int64) *maintenanceView {
	var proof *maintenanceView
	for i := range views {
		if (views[i].snapshots[id] || views[i].main != nil && *views[i].main == id) &&
			(proof == nil || views[i].call.done.Index < proof.call.done.Index) {
			proof = &views[i]
		}
	}
	return proof
}

func maintenanceOutcome(call *maintenanceCall) history.Type {
	if call.done == nil {
		return history.Info
	}
	return call.done.Type
}

// maintenanceOverlap reports whether two calls were in flight together. A
// missing completion stays in flight to the end of the history.
func maintenanceOverlap(a, b *maintenanceCall, end int) bool {
	doneA, doneB := end, end
	if a.done != nil {
		doneA = a.done.Index
	}
	if b.done != nil {
		doneB = b.done.Index
	}
	return a.invoke.Index < doneB && b.invoke.Index < doneA
}

func containsSnapshot(ids []int64, id int64) bool {
	for _, have := range ids {
		if have == id {
			return true
		}
	}
	return false
}

func sortedSnapshots(set map[int64]bool) []int64 {
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func sortedKeys(m map[int64]int) []int64 {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
