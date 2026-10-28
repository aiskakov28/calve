package checker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// CheckAppendHistory checks a closed, fault-free property-append run entirely
// from version-1 HTTP evidence. Successful later reads can resolve commitment;
// an absent key, failed read, or expired read-back budget cannot prove rejection.
func CheckAppendHistory(ops []history.Op) (AppendResult, error) {
	var manifest struct {
		RunID     string `json:"run_id"`
		Workload  string `json:"workload"`
		Catalog   string `json:"catalog"`
		Namespace string `json:"namespace"`
		Table     string `json:"table"`
		Clients   int    `json:"clients"`
		PerClient int    `json:"per_client"`
	}
	calls := map[int]*skewCall{}
	var ordered []*skewCall
	setupCalls := map[string]bool{}
	var origin *url.URL
	for i, op := range ops {
		if op.Version != history.Version || op.Index < 0 || op.Time < 0 ||
			i > 0 && (op.Index <= ops[i-1].Index || op.Time < ops[i-1].Time) {
			return AppendResult{}, fmt.Errorf("append history record %d: invalid version/order", i)
		}
		if op.Type == history.Meta {
			if op.F != "run" || manifest.RunID != "" || json.Unmarshal(op.Value, &manifest) != nil ||
				manifest.RunID == "" || manifest.Workload != "append" || manifest.Namespace == "" || manifest.Table == "" {
				return AppendResult{}, fmt.Errorf("append history record %d: invalid run metadata", i)
			}
			var err error
			origin, err = url.Parse(manifest.Catalog)
			if err != nil || origin.Host == "" || origin.Scheme != "http" && origin.Scheme != "https" {
				return AppendResult{}, fmt.Errorf("append history: invalid catalog origin")
			}
			continue
		}
		if origin == nil || op.ID <= 0 || op.Round != "" {
			return AppendResult{}, fmt.Errorf("append history record %d: missing run/ID or unexpected round", i)
		}
		if op.Type == history.Invoke {
			if calls[op.ID] != nil || op.Request == nil {
				return AppendResult{}, fmt.Errorf("append history record %d: duplicate ID or missing request", i)
			}
			u, err := url.Parse(op.Request.URL)
			if err != nil || u.Scheme != origin.Scheme || u.Host != origin.Host ||
				!strings.HasPrefix(u.Path, strings.TrimRight(origin.Path, "/")+"/v1/") {
				return AppendResult{}, fmt.Errorf("append history: request targets a different catalog")
			}
			switch op.F {
			case "config", "create-namespace", "drop-table", "create-table":
				if op.Phase != "setup" || op.Process != -1 || setupCalls[op.F] {
					return AppendResult{}, fmt.Errorf("append history: invalid setup labels")
				}
				setupCalls[op.F] = true
			case "load-table":
				if op.Request.Method != http.MethodGet || op.Process != -1 ||
					op.Phase != "baseline" && op.Phase != "final" && op.Phase != "recovery" {
					return AppendResult{}, fmt.Errorf("append history: invalid observation labels")
				}
			case "append":
				if op.Request.Method != http.MethodPost || op.Phase != "workload" || op.Process < 0 {
					return AppendResult{}, fmt.Errorf("append history: invalid append labels")
				}
			default:
				return AppendResult{}, fmt.Errorf("append history: operation is outside the closed append workload")
			}
			call := &skewCall{invoke: op}
			calls[op.ID] = call
			ordered = append(ordered, call)
			continue
		}
		call := calls[op.ID]
		if call == nil || call.done != nil {
			return AppendResult{}, fmt.Errorf("append history: completion without a unique invocation")
		}
		inv := call.invoke
		if op.Type != history.OK && op.Type != history.Fail && op.Type != history.Info ||
			op.F != inv.F || op.Phase != inv.Phase || op.Process != inv.Process || !bytes.Equal(op.Value, inv.Value) ||
			op.Type == history.OK && (op.Status < 200 || op.Status >= 300 || op.Error != "") ||
			op.Type == history.Fail && (op.Status < 400 || op.Status >= 500) ||
			op.Status != 0 && (op.Response == nil || op.Response.Status != op.Status) ||
			op.Status == 0 && op.Response != nil {
			return AppendResult{}, fmt.Errorf("append history: completion labels/status/evidence disagree")
		}
		copy := op
		call.done = &copy
	}
	if manifest.RunID == "" || manifest.Clients <= 0 || manifest.PerClient <= 0 ||
		manifest.Clients > int(^uint(0)>>1)/manifest.PerClient {
		return AppendResult{}, fmt.Errorf("append history: missing run metadata")
	}
	var baseline *skewCall
	for _, call := range ordered {
		if call.invoke.F == "load-table" && call.invoke.Phase == "baseline" {
			if baseline != nil {
				return AppendResult{}, fmt.Errorf("append history: duplicate baseline")
			}
			baseline = call
		}
	}
	base, err := appendMetadata(baseline)
	if err != nil {
		return appendUnavailable(ops, "baseline observation is unavailable")
	}
	key, err := skewReadKey(baseline.invoke.Request.URL)
	if err != nil || key != skewTableKey(rest.Identifier{Namespace: strings.Split(manifest.Namespace, "."), Name: manifest.Table}) {
		return AppendResult{}, fmt.Errorf("append history: baseline does not identify the run's table")
	}
	for _, call := range ordered {
		if call.invoke.Phase != "setup" {
			continue
		}
		if call.done == nil || call.done.Type == history.Info || call.done.Index >= baseline.invoke.Index {
			// In particular, an ignored drop-table timeout can still remove
			// the new table later. It cannot establish a clean experiment.
			return appendUnavailable(ops, "setup operation is unresolved or follows the baseline")
		}
		if call.invoke.F == "drop-table" && (call.invoke.Request.Method != http.MethodDelete ||
			call.invoke.Request.URL != baseline.invoke.Request.URL) {
			return AppendResult{}, fmt.Errorf("append history: setup drop targets a different table")
		}
	}
	for k := range base.Properties {
		if strings.HasPrefix(k, "calve.append.") {
			return appendUnavailable(ops, "baseline contains append markers from another run")
		}
	}
	created := false
	for _, call := range ordered {
		if call.invoke.F != "create-table" {
			continue
		}
		var body struct {
			Name  string `json:"name"`
			Stage bool   `json:"stage-create"`
		}
		if json.Unmarshal(call.invoke.Request.Body, &body) == nil && body.Name == manifest.Table && !body.Stage &&
			call.invoke.Request.Method == http.MethodPost && call.done != nil && call.done.Type == history.OK &&
			call.done.Index < baseline.invoke.Index &&
			strings.TrimRight(call.invoke.Request.URL, "/")+"/"+url.PathEscape(body.Name) == baseline.invoke.Request.URL {
			created = true
		}
	}
	if !created {
		return appendUnavailable(ops, "fresh table creation was not established")
	}
	var appends []*skewCall
	for _, call := range ordered {
		if call.invoke.F != "append" {
			continue
		}
		var label AppendValue
		var request rest.TableChange
		if call.invoke.Request.URL != baseline.invoke.Request.URL || baseline.done.Index >= call.invoke.Index ||
			json.Unmarshal(call.invoke.Value, &label) != nil || !strings.HasPrefix(label.Key, "calve.append.") ||
			bytes.Contains(call.invoke.Request.Body, []byte("[REDACTED]")) ||
			json.Unmarshal(call.invoke.Request.Body, &request) != nil || request.Requirements == nil ||
			len(request.Requirements) != 0 || len(request.Updates) != 1 {
			return AppendResult{}, fmt.Errorf("append history: invalid append request/identity")
		}
		update := request.Updates[0]
		properties, ok := update["updates"].(map[string]any)
		if update["action"] != "set-properties" || len(update) != 2 || !ok ||
			len(properties) != 1 || properties[label.Key] != "1" {
			return AppendResult{}, fmt.Errorf("append history: request does not set its unique marker to 1")
		}
		appends = append(appends, call)
	}
	if len(appends) == 0 {
		return appendUnavailable(ops, "no append operations were recorded")
	}
	evidence := appendEvidence{committed: map[string]appendProof{}, present: map[string]bool{}}
	var finalKeys []string
	var observations int
	latestUsable := false
	for _, read := range ordered {
		if read.invoke.F != "load-table" || read.invoke.Phase == "baseline" {
			continue
		}
		if read.invoke.Request.URL != baseline.invoke.Request.URL {
			return AppendResult{}, fmt.Errorf("append history: observation targets a different table")
		}
		for _, call := range appends {
			if read.invoke.Index <= call.invoke.Index || call.done != nil && read.invoke.Index <= call.done.Index {
				return AppendResult{}, fmt.Errorf("append history: observation precedes workload completion")
			}
		}
		observations++
		meta, err := appendMetadata(read)
		latestUsable = err == nil && meta.TableUUID == base.TableUUID
		if !latestUsable {
			continue
		}
		finalKeys = nil
		for k, value := range meta.Properties {
			if !strings.HasPrefix(k, "calve.append.") {
				continue
			}
			// Any value supplies presence evidence for rejected/unexpected
			// keys. Only the exact value supplies commitment evidence.
			evidence.present[k] = true
			if value == "1" {
				finalKeys = append(finalKeys, k)
				evidence.committed[k] = appendProof{resolution: "observed-property", ids: []int{read.invoke.ID}}
			}
		}
	}
	if observations == 0 {
		return appendUnavailable(ops, "final observation is unavailable")
	}
	res, err := checkAppend(ops, finalKeys, evidence)
	if err != nil {
		return res, err
	}
	if !latestUsable {
		// Positive evidence remains useful; absence in an earlier read cannot
		// stand in for a failed or identity-mismatched final observation.
		kept := res.Anomalies[:0]
		for _, anomaly := range res.Anomalies {
			if anomaly.Kind != "lost" {
				kept = append(kept, anomaly)
			}
		}
		res.Anomalies = kept
		res.Reasons = append(res.Reasons, "latest observation is unavailable or table UUID changed")
		res.Valid, res.Verdict = false, Inconclusive
		if len(res.Anomalies) > 0 {
			res.Verdict = Invalid
		}
	}
	if len(appends) != manifest.Clients*manifest.PerClient {
		res.Reasons = append(res.Reasons, "not all configured append operations were recorded")
		res.Valid = false
		if res.Verdict != Invalid {
			res.Verdict = Inconclusive
		}
	}
	return res, nil
}

func appendMetadata(call *skewCall) (rest.TableMetadata, error) {
	if call == nil || call.done == nil || call.done.Type != history.OK || call.done.Response == nil {
		return rest.TableMetadata{}, fmt.Errorf("successful table read unavailable")
	}
	meta, err := rest.ParseMetadata(rest.Response{Status: call.done.Status, Body: call.done.Response.Body})
	if err != nil || meta.TableUUID == "" || strings.Contains(meta.TableUUID, "[REDACTED]") {
		return rest.TableMetadata{}, fmt.Errorf("usable table identity unavailable")
	}
	return meta, nil
}

func appendUnavailable(ops []history.Op, reason string) (AppendResult, error) {
	res, err := checkAppend(ops, nil, appendEvidence{})
	res.Anomalies = nil
	res.Reasons = append(res.Reasons, reason)
	res.Valid, res.Verdict = false, Inconclusive
	return res, err
}
