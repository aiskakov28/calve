package checker

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/anishathalye/porcupine"
)

// SnapshotResult describes linearizability relative to its recorded Model.
// Illegal is a model finding; malformed/unsupported evidence returns an error.
type SnapshotResult struct {
	Model             string                 `json:"model"`
	Verdict           Verdict                `json:"verdict"`
	Search            porcupine.CheckResult  `json:"search,omitempty"`
	Reasons           []string               `json:"reasons,omitempty"`
	Anomalies         []Anomaly              `json:"anomalies,omitempty"`
	OperationIDs      []int                  `json:"operation_ids"`
	Commits           []SnapshotCommitResult `json:"commits"`
	OK                int                    `json:"ok"`
	Failed            int                    `json:"failed"`
	Unknown           int                    `json:"unknown"`
	ResolvedCommitted int                    `json:"resolved_committed"`
	Unresolved        int                    `json:"unresolved"`
}

type SnapshotCommitResult struct {
	ID          int          `json:"id"`
	Outcome     history.Type `json:"outcome"`
	CommitState string       `json:"commit_state"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type snapshotCall struct {
	invoke history.Op
	done   *history.Op
	input  snapshotInput
	view   *snapshotState
}

// CheckSnapshotHistory checks one exclusively owned table from its baseline
// through the recorded observations. Unknown commits remain pending through the
// entire history, even when their client has already received a timeout/5xx.
// A positive timeout is required; search exhaustion is always inconclusive.
func CheckSnapshotHistory(ops []history.Op, timeout time.Duration) (SnapshotResult, error) {
	return checkSnapshotHistory(ops, timeout, porcupine.CheckEventsTimeout)
}

type snapshotSearch func(porcupine.Model, []porcupine.Event, time.Duration) porcupine.CheckResult

func checkSnapshotHistory(ops []history.Op, timeout time.Duration, search snapshotSearch) (SnapshotResult, error) {
	res := SnapshotResult{Model: SnapshotModelVersion, Verdict: Inconclusive}
	if timeout <= 0 {
		return res, fmt.Errorf("snapshot checker needs a positive search timeout")
	}
	manifest, calls, err := parseSnapshotHistory(ops)
	if err != nil {
		return res, err
	}
	res.Model = manifest.Model
	res.OperationIDs = snapshotIDs(calls)
	var baseline *snapshotCall
	for _, call := range calls {
		if call.invoke.Phase == "baseline" {
			if baseline != nil || call.invoke.F != "load-table" {
				return res, fmt.Errorf("snapshot history: duplicate/invalid baseline")
			}
			baseline = call
		}
	}
	if baseline == nil || baseline.done == nil || baseline.done.Type != history.OK {
		res.Reasons = append(res.Reasons, "successful baseline is unavailable")
		return res, nil
	}
	initial, err := snapshotMetadata(baseline.done.Response.Body)
	if err != nil {
		return res, fmt.Errorf("snapshot history: unsupported or incomplete baseline: %w", err)
	}
	if !snapshotBaselineValid(initial) {
		return res, fmt.Errorf("snapshot history: baseline is not an internally consistent supported snapshot state")
	}
	baseline.view = &initial
	seen := map[int64]bool{}
	var commits []*snapshotCall
	lastCommitRecord := baseline.done.Index
	lastFinal := -1
	var finalUsable bool
	searchEnd, identityID := len(ops), 0
	for _, call := range calls {
		if call == baseline {
			continue
		}
		if call.invoke.Index <= baseline.done.Index {
			return res, fmt.Errorf("snapshot history: operation starts before baseline completed")
		}
		if call.invoke.F == "commit-table" {
			call.input, err = snapshotRequest(call.invoke.Request.Body, manifest)
			if err != nil {
				return res, fmt.Errorf("snapshot history commit %d: %w", call.invoke.ID, err)
			}
			id := *call.input.added.ID
			if seen[id] || initial.Snapshots[id] != "" {
				return res, fmt.Errorf("snapshot history: proposed snapshot IDs must be unique and absent from baseline")
			}
			seen[id] = true
			commits = append(commits, call)
			last := call.invoke.Index
			if call.done != nil {
				last = call.done.Index
			}
			if last > lastCommitRecord {
				lastCommitRecord = last
			}
		} else {
			call.input.read = true
		}
		if call.done != nil && call.done.Type == history.OK {
			view, err := snapshotMetadata(call.done.Response.Body)
			if view.UUID != "" && view.UUID != initial.UUID {
				if call.invoke.Index < searchEnd {
					searchEnd, identityID = call.invoke.Index, call.invoke.ID
				}
			} else if err != nil {
				return res, fmt.Errorf("snapshot history operation %d: unusable successful metadata: %w", call.invoke.ID, err)
			} else {
				call.view = &view
			}
		}
		if call.invoke.Phase == "final" || call.invoke.Phase == "recovery" {
			lastFinal, finalUsable = call.invoke.Index, call.view != nil
		}
	}
	if len(commits) > manifest.Commits {
		return res, fmt.Errorf("snapshot history: more commits than planned")
	}
	if len(commits) != manifest.Commits {
		res.Reasons = append(res.Reasons, "not all planned commits were recorded")
	}
	if lastFinal <= lastCommitRecord || !finalUsable {
		res.Reasons = append(res.Reasons, "successful final/recovery read after the attempts is unavailable")
	}
	if identityID != 0 {
		res.Reasons = append(res.Reasons, fmt.Sprintf("operation %d observed a changed table UUID; search is limited to records before its invocation at index %d", identityID, searchEnd))
	}

	// Record indexes provide a strict call/return order even when clock values
	// tie. Info/missing commit returns are moved to the end, without changing the
	// source history or forcing a pending write to precede later reads.
	// At an identity boundary only the earlier incarnation is modeled. Calls
	// crossing that boundary remain pending, regardless of their later outcome.
	byID := map[int]*snapshotCall{}
	outputs := map[int]snapshotOutput{}
	checked := []*snapshotCall{baseline}
	for _, call := range calls {
		if call == baseline || call.invoke.Index >= searchEnd {
			continue
		}
		complete := call.done != nil && call.done.Index < searchEnd
		if call.input.read && (!complete || call.view == nil) {
			continue // a failed/pending read has no observable state constraint
		}
		out := snapshotOutput{outcome: history.Info}
		if complete {
			out.outcome, out.view = call.done.Type, call.view
		}
		byID[call.invoke.ID] = call
		outputs[call.invoke.ID] = out
		checked = append(checked, call)
	}
	res.OperationIDs = snapshotIDs(checked)
	var events, pending []porcupine.Event
	for _, op := range ops {
		if op.Index >= searchEnd {
			break
		}
		call := byID[op.ID]
		if call == nil {
			continue
		}
		out := outputs[op.ID]
		if op.Type == history.Invoke {
			events = append(events, porcupine.Event{Kind: porcupine.CallEvent, Id: op.ID, ClientId: op.Process, Value: call.input})
			if out.outcome == history.Info {
				pending = append(pending, porcupine.Event{Kind: porcupine.ReturnEvent, Id: op.ID, ClientId: op.Process, Value: out})
			}
		} else if out.outcome != history.Info {
			events = append(events, porcupine.Event{Kind: porcupine.ReturnEvent, Id: op.ID, ClientId: op.Process, Value: out})
		}
	}
	events = append(events, pending...)
	res.Search = search(snapshotModelFor(initial, manifest.Model), events, timeout)
	for _, call := range commits {
		item := SnapshotCommitResult{ID: call.invoke.ID, Outcome: history.Info, CommitState: "unknown"}
		if call.done != nil {
			item.Outcome = call.done.Type
		}
		switch item.Outcome {
		case history.OK:
			res.OK++
			item.CommitState = "committed"
		case history.Fail:
			res.Failed++
			item.CommitState = "rejected"
		default:
			res.Unknown++
			// Resolve only from an exact complete observed effect. Historical
			// descriptors alone do not prove the main-ref update was applied.
			for _, read := range calls {
				if read.view != nil && read.done != nil && read.done.Index < searchEnd && read.done.Index > call.invoke.Index &&
					sameSnapshotID(read.view.Current, call.input.added.ID) &&
					read.view.Main != nil && read.view.Main.Type == "branch" &&
					sameSnapshotID(read.view.Main.ID, call.input.added.ID) &&
					snapshotDescriptorEqual(read.view.Snapshots[*call.input.added.ID], call.input.encoded, manifest.Model == SnapshotCurrentModelVersion) {
					item.EvidenceIDs = append(item.EvidenceIDs, read.invoke.ID)
				}
			}
			if len(item.EvidenceIDs) > 0 {
				item.CommitState = "committed"
				res.ResolvedCommitted++
			} else {
				res.Unresolved++
			}
		}
		res.Commits = append(res.Commits, item)
	}
	if res.Unresolved > 0 {
		res.Reasons = append(res.Reasons, "unknown commits remain unresolved; absence does not prove rejection")
	}
	switch res.Search {
	case porcupine.Illegal:
		res.Verdict = Invalid
		res.Anomalies = []Anomaly{{Kind: "non-linearizable", Detail: "no atomic snapshot CAS/read ordering respects the recorded real-time precedence"}}
	case porcupine.Unknown:
		res.Reasons = append(res.Reasons, "Porcupine search timed out")
	case porcupine.Ok:
		if len(res.Reasons) == 0 {
			res.Verdict = Valid
		}
	}
	return res, nil
}

func splitSnapshotNamespace(namespace string) []string { return strings.Split(namespace, ".") }

func parseSnapshotHistory(ops []history.Op) (SnapshotManifest, []*snapshotCall, error) {
	var manifest SnapshotManifest
	bad := func(msg string) (SnapshotManifest, []*snapshotCall, error) {
		return manifest, nil, fmt.Errorf("snapshot history: %s", msg)
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" ||
		snapshotJSON(ops[0].Value, &manifest) != nil || manifest.RunID == "" ||
		manifest.Model != SnapshotModelVersion && manifest.Model != SnapshotCurrentModelVersion ||
		manifest.Workload != "snapshot-cas" || manifest.Namespace == "" || manifest.Table == "" || manifest.Commits <= 0 {
		return bad("requires a snapshot-cas run manifest with the supported model and positive commit count")
	}
	origin, err := url.Parse(manifest.Catalog)
	if err != nil || origin.Host == "" || origin.Scheme != "http" && origin.Scheme != "https" ||
		origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" {
		return bad("invalid catalog URL")
	}
	var ordered []*snapshotCall
	byID := map[int]*snapshotCall{}
	targetURL := ""
	for i, op := range ops {
		if op.Version != history.Version || op.Index != i || op.Time < 0 || i > 0 && op.Time < ops[i-1].Time {
			return bad("invalid version, index or clock order")
		}
		if i == 0 {
			continue
		}
		if op.ID <= 0 || op.Round != "" || op.Value != nil {
			return bad("unexpected ID, round or derived value labels")
		}
		if op.Type == history.Invoke {
			if byID[op.ID] != nil || op.Request == nil || op.Response != nil {
				return bad("duplicate invocation ID or missing/unexpected HTTP evidence")
			}
			if bytes.Contains(op.Request.Body, []byte("[REDACTED]")) {
				return bad("redaction removed model-relevant request evidence")
			}
			u, err := url.Parse(op.Request.URL)
			key, keyErr := skewReadKey(op.Request.URL)
			if err != nil || keyErr != nil || u.Scheme != origin.Scheme || u.Host != origin.Host || u.User != nil ||
				u.RawQuery != "" || u.Fragment != "" ||
				!strings.HasPrefix(u.EscapedPath(), strings.TrimRight(origin.EscapedPath(), "/")+"/v1/") ||
				key != snapshotTableKey(manifest) {
				return bad("request does not target exactly the run's single table")
			}
			if targetURL == "" {
				targetURL = op.Request.URL
			}
			if targetURL != op.Request.URL {
				return bad("table routing changed")
			}
			switch op.F {
			case "load-table":
				if op.Request.Method != http.MethodGet || len(op.Request.Body) != 0 ||
					op.Phase != "baseline" && op.Phase != "workload" && op.Phase != "final" && op.Phase != "recovery" {
					return bad("unsupported table read")
				}
			case "commit-table":
				if op.Request.Method != http.MethodPost || op.Phase != "workload" {
					return bad("unsupported commit")
				}
			default:
				return bad("operation is outside the single-table snapshot model")
			}
			call := &snapshotCall{invoke: op}
			ordered = append(ordered, call)
			byID[op.ID] = call
			continue
		}
		call := byID[op.ID]
		if call == nil || call.done != nil {
			return bad("completion without a unique invocation")
		}
		inv := call.invoke
		if op.F != inv.F || op.Phase != inv.Phase || op.Process != inv.Process || !bytes.Equal(op.Value, inv.Value) ||
			op.Request != nil || op.Status == 0 && op.Response != nil ||
			op.Status != 0 && (op.Response == nil || op.Response.Status != op.Status) {
			return bad("completion labels or HTTP evidence disagree")
		}
		switch op.Type {
		case history.OK:
			if op.Status != http.StatusOK || op.Error != "" {
				return bad("successful snapshot operations require complete HTTP 200 metadata")
			}
		case history.Fail:
			if op.Status < 400 || op.Status >= 500 {
				return bad("rejection without a 4xx response")
			}
		case history.Info:
			if op.Status != 0 && op.Status < 500 && op.Error == "" {
				return bad("unknown outcome without an uncertain response")
			}
		default:
			return bad("unsupported completion type")
		}
		copy := op
		call.done = &copy
	}
	return manifest, ordered, nil
}

func snapshotTableKey(m SnapshotManifest) string {
	// Same namespace escaping as rest.Client; this is not a multi-table split.
	return skewTableKey(rest.Identifier{Namespace: splitSnapshotNamespace(m.Namespace), Name: m.Table})
}
