package checker

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type AtomicAnomaly struct {
	Kind          string            `json:"kind"`
	Basis         string            `json:"basis"` // catalog-contract, atomic-visibility-model, or fresh-read-model
	TransactionID int               `json:"transaction_id,omitempty"`
	Key           string            `json:"key"`
	Tables        []rest.Identifier `json:"tables"`
	EvidenceIDs   []int             `json:"evidence_ids"`
	Detail        string            `json:"detail"`
}

type AtomicTransaction struct {
	ID          int          `json:"id"`
	Key         string       `json:"key"`
	Outcome     history.Type `json:"outcome"`
	CommitState string       `json:"commit_state"`
	Resolution  string       `json:"resolution,omitempty"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type AtomicResult struct {
	Valid               bool                `json:"valid"`
	Verdict             Verdict             `json:"verdict"`
	Anomalies           []AtomicAnomaly     `json:"anomalies,omitempty"`
	Reasons             []string            `json:"reasons,omitempty"`
	Transactions        []AtomicTransaction `json:"transactions"`
	OK                  int                 `json:"ok"`
	Failed              int                 `json:"failed"`
	Unknown             int                 `json:"unknown"`
	ResolvedCommitted   int                 `json:"resolved_committed"`
	Unresolved          int                 `json:"unresolved"`
	WorkloadReads       int                 `json:"workload_reads"`
	ConcurrentReads     int                 `json:"concurrent_reads"`
	ReadCoverageMissing bool                `json:"read_coverage_missing,omitempty"`
}

// CheckAtomic checks the all-table unique-property workload, not serializability.
// Separate table loads are never assumed to be one consistent snapshot. Positive
// effects must precede a negative observation to establish a visibility lead.
func CheckAtomic(ops []history.Op) (AtomicResult, error) {
	m, calls, err := parseAtomicHistory(ops)
	if err != nil {
		return AtomicResult{}, err
	}
	res := AtomicResult{}
	var txns []*atomicCall
	keys := map[string]*atomicCall{}
	lastAttempt := -1
	for _, call := range calls {
		if call.invoke.F == "load-table" && call.invoke.Phase == "workload" {
			res.WorkloadReads++
		}
		if call.key == "" {
			continue
		}
		if keys[call.key] != nil {
			return res, fmt.Errorf("atomic history: duplicate transaction marker")
		}
		keys[call.key] = call
		txns = append(txns, call)
		last := call.invoke.Index
		if call.done != nil {
			last = call.done.Index
		}
		if last > lastAttempt {
			lastAttempt = last
		}
		switch atomicOutcome(call, len(ops)) {
		case history.OK:
			res.OK++
		case history.Fail:
			res.Failed++
		default:
			res.Unknown++
		}
	}
	switch {
	case m.FaultPlan == "" && len(txns) != m.Clients*m.PerClient:
		res.Reasons = append(res.Reasons, "not all planned transaction attempts were recorded")
	case m.FaultPlan != "" && len(txns) == 0:
		// A campaign ends at the plan's horizon, before its attempt cap.
		res.Reasons = append(res.Reasons, "no transaction attempt was recorded")
	}
	uuids, route, setupEnd, setupErr := atomicBaselines(m, calls)
	if setupErr != nil {
		res.Reasons = append(res.Reasons, setupErr.Error())
		for _, call := range txns {
			res.Transactions = append(res.Transactions, AtomicTransaction{ID: call.invoke.ID, Key: call.key,
				Outcome: atomicOutcome(call, len(ops)), CommitState: "unknown"})
			res.Unresolved++
		}
		finishAtomic(&res)
		return res, nil
	}
	for _, call := range txns {
		if call.invoke.Index <= setupEnd {
			return res, fmt.Errorf("atomic transaction starts before setup/baselines complete")
		}
		if err := validateAtomicRequest(call, m, uuids, route); err != nil {
			return res, err
		}
	}
	prefix := atomicPrefix + m.RunID + "."
	views := map[string][]atomicView{}
	latest := map[string]*atomicCall{}
	cut := len(ops)
	for _, call := range calls {
		if call.invoke.F != "load-table" || call.invoke.Phase == "baseline" {
			continue
		}
		if call.invoke.Index <= setupEnd || skewTableRoute(call.invoke.Request.URL) != route {
			return res, fmt.Errorf("atomic history: observation precedes setup or changes routing")
		}
		latest[call.table] = call
		view, err := atomicMetadata(call, prefix)
		if call.done == nil || call.done.Type != history.OK {
			continue
		}
		if view.uuid != "" && view.uuid != uuids[call.table] {
			if call.invoke.Index < cut {
				cut = call.invoke.Index
			}
			continue
		}
		if err != nil {
			return res, fmt.Errorf("atomic observation %d: %w", call.invoke.ID, err)
		}
		views[call.table] = append(views[call.table], view)
	}
	if cut < len(ops) {
		res.Reasons = append(res.Reasons, fmt.Sprintf("table UUID changed; only evidence before record %d belongs to the original experiment", cut))
	}
	for _, table := range m.Tables {
		key := skewTableKey(table)
		last := latest[key]
		if last == nil || last.done == nil || last.done.Type != history.OK ||
			last.invoke.Index <= lastAttempt || last.done.Index >= cut ||
			last.invoke.Phase != "final" && last.invoke.Phase != "recovery" {
			res.Reasons = append(res.Reasons, "usable final/recovery observation unavailable for "+table.Name)
		}
		kept := views[key][:0]
		for _, view := range views[key] {
			if view.call.done.Index < cut {
				kept = append(kept, view)
				if view.call.invoke.Phase == "workload" {
					for _, txn := range txns {
						if view.call.done.Index > txn.invoke.Index && (txn.done == nil || view.call.invoke.Index < txn.done.Index) {
							res.ConcurrentReads++
							break
						}
					}
				}
			}
		}
		views[key] = kept
	}
	if m.Readers > 0 && res.ConcurrentReads == 0 {
		res.ReadCoverageMissing = true
		res.Reasons = append(res.Reasons, "readers were enabled but no usable workload read overlapped a transaction call")
	}
	// Unexpected markers are attributable only within this run's fresh marker
	// space. Unrelated table properties (including redacted credentials) are not
	// projected into this model.
	unexpected := map[string]bool{}
	for _, table := range m.Tables {
		for _, view := range views[skewTableKey(table)] {
			var observedKeys []string
			for key := range view.props {
				observedKeys = append(observedKeys, key)
			}
			sort.Strings(observedKeys)
			for _, key := range observedKeys {
				call := keys[key]
				if call == nil || view.call.done.Index < call.invoke.Index {
					dedupe := key + " " + skewTableKey(table)
					if !unexpected[dedupe] {
						res.Anomalies = append(res.Anomalies, AtomicAnomaly{Kind: "unexpected", Basis: "catalog-contract", Key: key,
							Tables: []rest.Identifier{table}, EvidenceIDs: []int{view.call.invoke.ID},
							Detail: "run marker was observed without a preceding or overlapping attempt"})
						unexpected[dedupe] = true
					}
				}
			}
		}
	}
	for _, call := range txns {
		raw, outcome := atomicOutcome(call, len(ops)), atomicOutcome(call, cut)
		txn := AtomicTransaction{ID: call.invoke.ID, Key: call.key, Outcome: raw, CommitState: "unknown"}
		var positives []atomicView
		all := true
		bound := -1
		var postRejection []atomicView
		for _, table := range m.Tables {
			found := false
			var first atomicView
			for _, view := range views[skewTableKey(table)] {
				if view.call.done.Index <= call.invoke.Index {
					continue
				}
				value, exists := view.props[call.key]
				if exists && outcome == history.Fail && view.call.invoke.Index > call.done.Index {
					postRejection = append(postRejection, view)
				}
				if value == "1" && (!found || view.call.done.Index < first.call.done.Index) {
					first = view
					found = true
				}
			}
			if found {
				positives = append(positives, first)
				if first.call.done.Index > bound {
					bound = first.call.done.Index
				}
			}
			all = all && found
		}
		switch outcome {
		case history.OK:
			txn.CommitState, txn.Resolution = "committed", "acknowledged"
			bound = call.done.Index
		case history.Fail:
			txn.CommitState, txn.Resolution = "rejected", "rejected"
		}
		if all && outcome == history.Info {
			txn.CommitState, txn.Resolution = "committed", "complete-table-effects"
			for _, view := range positives {
				txn.EvidenceIDs = append(txn.EvidenceIDs, view.call.invoke.ID)
			}
			if raw == history.Info {
				res.ResolvedCommitted++
			}
		}
		if txn.CommitState == "unknown" {
			res.Unresolved++
		}
		add := func(kind, basis, detail string, evidence []atomicView) {
			ids := []int{call.invoke.ID}
			var tables []rest.Identifier
			used := map[string]bool{}
			for _, view := range evidence {
				ids = append(ids, view.call.invoke.ID)
				if !used[view.call.table] {
					for _, table := range m.Tables {
						if skewTableKey(table) == view.call.table {
							tables = append(tables, table)
						}
					}
					used[view.call.table] = true
				}
			}
			sort.Ints(ids)
			unique := ids[:0]
			for _, id := range ids {
				if len(unique) == 0 || unique[len(unique)-1] != id {
					unique = append(unique, id)
				}
			}
			res.Anomalies = append(res.Anomalies, AtomicAnomaly{Kind: kind, Basis: basis, TransactionID: call.invoke.ID,
				Key: call.key, Tables: tables, EvidenceIDs: unique, Detail: detail})
		}
		if len(postRejection) > 0 {
			add("phantom-failed", "catalog-contract", "a table load started after rejection and observed the attempt's unique marker", postRejection)
		} else {
			var missing []atomicView
			if txn.CommitState == "committed" {
				for _, table := range m.Tables {
					for _, view := range views[skewTableKey(table)] {
						if view.call.invoke.Index > bound && view.props[call.key] != "1" {
							missing = append(missing, view)
							break
						}
					}
				}
			}
			if len(missing) > 0 {
				evidence := missing
				if outcome != history.OK {
					evidence = append(append([]atomicView(nil), positives...), missing...)
				}
				add("lost", "catalog-contract", "a proven committed transaction's complete marker is missing from a later table load", evidence)
			} else {
				// Old A then new B is legal if the commit falls between reads.
				// New A completed BEFORE an old B starts cannot be explained by
				// that ordering under atomic publication and fresh table loads.
				witness := func(sameTable bool) []atomicView {
					for _, positive := range positives {
						for _, table := range m.Tables {
							key := skewTableKey(table)
							if (key == positive.call.table) != sameTable {
								continue
							}
							for _, negative := range views[key] {
								if negative.call.invoke.Index > positive.call.done.Index && negative.props[call.key] != "1" {
									return []atomicView{positive, negative}
								}
							}
						}
					}
					return nil
				}
				if regression := witness(true); len(regression) > 0 {
					add("single-table-regression", "fresh-read-model",
						"a previously observed marker disappeared from the same table; this lead assumes fresh table loads", regression)
				} else if crossTable := witness(false); len(crossTable) > 0 {
					add("non-atomic-visibility", "atomic-visibility-model",
						"a marker was observed before a later read of a different table lacked its transaction effect; this lead assumes fresh table loads", crossTable)
				}
			}
		}
		res.Transactions = append(res.Transactions, txn)
	}
	if res.Unresolved > 0 {
		res.Reasons = append(res.Reasons, "unknown transaction effects remain unresolved; absence does not prove rejection")
	}
	if res.OK+res.ResolvedCommitted == 0 {
		res.Reasons = append(res.Reasons, "no proven committed multi-table transaction coverage")
	}
	finishAtomic(&res)
	return res, nil
}

func atomicOutcome(call *atomicCall, end int) history.Type {
	if call.done == nil || call.done.Index >= end {
		return history.Info
	}
	return call.done.Type
}

func finishAtomic(res *AtomicResult) {
	res.Verdict = Valid
	if len(res.Reasons) > 0 || res.Unresolved > 0 {
		res.Verdict = Inconclusive
	}
	if len(res.Anomalies) > 0 {
		res.Verdict = Invalid
	}
	res.Valid = res.Verdict == Valid
}

func atomicBaselines(m atomicManifest, calls []*atomicCall) (map[string]string, string, int, error) {
	bad := func(msg string) (map[string]string, string, int, error) {
		return nil, "", 0, fmt.Errorf("atomic setup/baseline: %s", msg)
	}
	uuids := map[string]string{}
	baselines := map[string]*atomicCall{}
	created := map[string]*atomicCall{}
	var config, namespace *atomicCall
	route := ""
	end := -1
	for _, call := range calls {
		switch call.invoke.F {
		case "config":
			if config != nil {
				return bad("duplicate config")
			}
			config = call
		case "create-namespace":
			if namespace != nil {
				return bad("duplicate namespace creation")
			}
			namespace = call
		case "create-table":
			var body struct {
				Name   string          `json:"name"`
				Schema json.RawMessage `json:"schema"`
				Stage  bool            `json:"stage-create"`
			}
			if atomicJSON(call.invoke.Request.Body, &body) != nil || body.Stage || len(body.Schema) == 0 {
				return bad("unrecognized table creation")
			}
			key := skewTableKey(rest.Identifier{Namespace: strings.Split(m.Namespace, "."), Name: body.Name})
			if created[key] != nil {
				return bad("duplicate table creation")
			}
			created[key] = call
		case "load-table":
			if call.invoke.Phase == "baseline" {
				if baselines[call.table] != nil {
					return bad("duplicate baseline")
				}
				baselines[call.table] = call
				r := skewTableRoute(call.invoke.Request.URL)
				if route != "" && route != r {
					return bad("baseline routing differs")
				}
				route = r
			}
		}
	}
	if config == nil || config.done == nil || config.done.Type != history.OK ||
		namespace == nil || namespace.done == nil ||
		namespace.done.Type != history.OK && !(namespace.done.Type == history.Fail && namespace.done.Status == 409) ||
		config.done.Index >= namespace.invoke.Index {
		return bad("config/namespace setup was not established")
	}
	u, _ := url.Parse(namespace.invoke.Request.URL)
	var ns struct {
		Namespace []string `json:"namespace"`
	}
	if u.EscapedPath() != route+"/namespaces" || atomicJSON(namespace.invoke.Request.Body, &ns) != nil ||
		!reflect.DeepEqual(ns.Namespace, strings.Split(m.Namespace, ".")) || len(created) != len(m.Tables) || len(baselines) != len(m.Tables) {
		return bad("setup does not identify every run table")
	}
	usedUUID := map[string]bool{}
	for _, table := range m.Tables {
		key := skewTableKey(table)
		base, creation := baselines[key], created[key]
		if base == nil || creation == nil || creation.done == nil || creation.done.Type != history.OK ||
			namespace.done.Index >= creation.invoke.Index || creation.done.Index >= base.invoke.Index {
			return bad("fresh creation before baseline was not established")
		}
		if strings.TrimRight(creation.invoke.Request.URL, "/")+"/"+url.PathEscape(table.Name) != base.invoke.Request.URL {
			return bad("creation targets a different table")
		}
		view, e := atomicMetadata(base, atomicPrefix+m.RunID+".")
		made, ce := atomicMetadata(creation, atomicPrefix+m.RunID+".")
		if e != nil || ce != nil || view.uuid != made.uuid || usedUUID[view.uuid] || len(view.props) != 0 {
			return bad("baseline is unavailable, contaminated, or has untrusted identity")
		}
		uuids[key], usedUUID[view.uuid] = view.uuid, true
		if base.done.Index > end {
			end = base.done.Index
		}
	}
	return uuids, route, end, nil
}
