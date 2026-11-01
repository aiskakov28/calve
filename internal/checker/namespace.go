package checker

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aiskakov28/calve/internal/history"
)

// namespaceOwnerKey is the property each contender sets on its namespace.
const namespaceOwnerKey = "calve.namespace.owner"

var namespaceRunID = regexp.MustCompile(`^nscreate_[0-9a-f]{24}$`)

type NamespaceAnomaly struct {
	Kind        string   `json:"kind"`
	Basis       string   `json:"basis"` // catalog-contract
	Round       int      `json:"round"`
	Owners      []string `json:"owners,omitempty"`
	EvidenceIDs []int    `json:"evidence_ids"`
	Detail      string   `json:"detail"`
}

type NamespaceCreate struct {
	ID          int          `json:"id"`
	Round       int          `json:"round"`
	Owner       string       `json:"owner"`
	Outcome     history.Type `json:"outcome"`
	Status      int          `json:"status,omitempty"`
	CommitState string       `json:"commit_state"`
	EvidenceIDs []int        `json:"evidence_ids,omitempty"`
}

type NamespaceResult struct {
	Valid             bool               `json:"valid"`
	Verdict           Verdict            `json:"verdict"`
	Anomalies         []NamespaceAnomaly `json:"anomalies,omitempty"`
	Reasons           []string           `json:"reasons,omitempty"`
	Creates           []NamespaceCreate  `json:"creates"`
	Rounds            int                `json:"rounds"`
	RoundsWithWinner  int                `json:"rounds_with_winner"`
	OK                int                `json:"ok"`
	Failed            int                `json:"failed"`
	Unknown           int                `json:"unknown"`
	ResolvedCommitted int                `json:"resolved_committed"`
	Unresolved        int                `json:"unresolved"`
	// UnknownBesideWinner counts unresolved creates in rounds whose load shows
	// another contender made the namespace.
	UnknownBesideWinner int `json:"unknown_beside_winner"`
}

type namespaceManifest struct {
	RunID    string `json:"run_id"`
	Workload string `json:"workload"`
	Catalog  string `json:"catalog"`
	Parent   string `json:"parent"`
	Clients  int    `json:"clients"`
	Rounds   int    `json:"rounds"`
}

type namespaceCall struct {
	invoke, done *history.Op
}

// namespaceView is one round's final load: whether the namespace exists and,
// if it does, the owner property it holds.
type namespaceView struct {
	known, exists, hasOwner bool
	owner                   string
}

// CheckNamespace checks the namespace-create race. Every contender in a round
// creates the same fresh namespace with its own owner property, so at most one
// create may take effect, and the namespace must then hold that owner. The
// stored owner proves which create took effect; an unknown create whose owner
// is absent stays unresolved, since absence alone never proves rejection.
func CheckNamespace(ops []history.Op) (NamespaceResult, error) {
	m, calls, err := parseNamespaceHistory(ops)
	if err != nil {
		return NamespaceResult{}, err
	}
	res := NamespaceResult{}
	creates := map[int][]*namespaceCall{}
	loads := map[int][]*namespaceCall{}
	for _, call := range calls {
		r, _ := strconv.Atoi(call.invoke.Round)
		if call.invoke.F == "create-namespace" {
			creates[r] = append(creates[r], call)
		} else {
			loads[r] = append(loads[r], call)
		}
	}
	for r := 0; r < m.Rounds; r++ {
		if len(creates[r]) == 0 && len(loads[r]) == 0 {
			res.Reasons = append(res.Reasons, fmt.Sprintf("round %d was not recorded", r))
			if len(creates[r+1]) != 0 || len(loads[r+1]) != 0 {
				return res, fmt.Errorf("namespace history: round %d is missing but a later round follows", r)
			}
			break
		}
		if len(creates[r]) != m.Clients || len(loads[r]) != 1 {
			return res, fmt.Errorf("namespace history: round %d has %d creates and %d loads, want %d and 1", r, len(creates[r]), len(loads[r]), m.Clients)
		}
		load := loads[r][0]
		for _, call := range creates[r] {
			if call.done == nil || load.invoke.Index < call.done.Index {
				return res, fmt.Errorf("namespace history: round %d load starts before create %d completes", r, call.invoke.ID)
			}
		}
		for _, call := range creates[r+1] {
			if load.done == nil || call.invoke.Index < load.done.Index {
				return res, fmt.Errorf("namespace history: round %d starts before round %d's load completes", r+1, r)
			}
		}
		view, err := readNamespaceView(load, namespaceLevels(m, r))
		if err != nil {
			return res, fmt.Errorf("namespace observation %d: %w", load.invoke.ID, err)
		}
		if !view.known {
			res.Reasons = append(res.Reasons, fmt.Sprintf("round %d final observation is unavailable", r))
		}
		res.Rounds++
		checkNamespaceRound(&res, r, creates[r], load, view)
	}
	if res.Unresolved > 0 {
		res.Reasons = append(res.Reasons, "unknown creates remain unresolved; absence does not prove rejection")
	}
	if res.UnknownBesideWinner > 0 {
		res.Reasons = append(res.Reasons, fmt.Sprintf("%d of them got no definite answer in rounds where another create made the namespace; the REST spec answers a create of an existing namespace with 409",
			res.UnknownBesideWinner))
	}
	if res.RoundsWithWinner == 0 {
		res.Reasons = append(res.Reasons, "no round has a committed create")
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

func checkNamespaceRound(res *NamespaceResult, r int, creates []*namespaceCall, load *namespaceCall, view namespaceView) {
	stored := func(owner string) bool { return view.known && view.exists && view.hasOwner && view.owner == owner }
	var winners []string
	var winnerIDs, acked []int
	// conflicts counts 409s, the answer that claims the namespace exists; other
	// 4xx rejections (a 403, say) claim nothing about it.
	matched, conflicts, unknown, unresolved := false, 0, 0, 0
	for _, call := range creates {
		owner := namespaceMarker(call)
		create := NamespaceCreate{ID: call.invoke.ID, Round: r, Owner: owner, Outcome: call.done.Type, Status: call.done.Status, CommitState: "unknown"}
		seen := stored(owner)
		matched = matched || seen
		if seen {
			create.EvidenceIDs = []int{load.invoke.ID}
		}
		switch call.done.Type {
		case history.OK:
			res.OK++
			create.CommitState = "committed"
			acked = append(acked, call.invoke.ID)
		case history.Fail:
			res.Failed++
			if call.done.Status == 409 {
				conflicts++
			}
			create.CommitState = "rejected"
			if seen {
				res.Anomalies = append(res.Anomalies, NamespaceAnomaly{Kind: "phantom-failed", Basis: "catalog-contract", Round: r,
					Owners: []string{owner}, EvidenceIDs: []int{call.invoke.ID, load.invoke.ID},
					Detail: "a rejected create's owner property is stored on the namespace"})
			}
		default:
			res.Unknown++
			unknown++
			if seen {
				create.CommitState = "committed"
				res.ResolvedCommitted++
			} else {
				res.Unresolved++
				unresolved++
			}
		}
		if create.CommitState == "committed" || seen {
			winners = append(winners, owner)
			winnerIDs = append(winnerIDs, call.invoke.ID)
		}
		res.Creates = append(res.Creates, create)
	}
	if len(winners) > 0 {
		res.RoundsWithWinner++
	}
	if matched {
		res.UnknownBesideWinner += unresolved
	}
	if len(winners) > 1 {
		if matched {
			winnerIDs = append(winnerIDs, load.invoke.ID)
		}
		sort.Ints(winnerIDs)
		res.Anomalies = append(res.Anomalies, NamespaceAnomaly{Kind: "multiple-winners", Basis: "catalog-contract", Round: r,
			Owners: winners, EvidenceIDs: winnerIDs,
			Detail: fmt.Sprintf("%d creates of the same new namespace all took effect", len(winners))})
		return
	}
	if !view.known {
		return
	}
	if view.exists && !matched {
		res.Anomalies = append(res.Anomalies, NamespaceAnomaly{Kind: "unexpected", Basis: "catalog-contract", Round: r,
			EvidenceIDs: []int{load.invoke.ID},
			Detail:      "the namespace exists without the owner property of any create in its round"})
	}
	for _, id := range acked {
		if !matched {
			res.Anomalies = append(res.Anomalies, NamespaceAnomaly{Kind: "lost", Basis: "catalog-contract", Round: r,
				Owners: winners, EvidenceIDs: []int{id, load.invoke.ID},
				Detail: "an acknowledged create's namespace or owner property is missing from the load after the round"})
		}
	}
	if !view.exists && conflicts == len(creates) && unknown == 0 {
		res.Anomalies = append(res.Anomalies, NamespaceAnomaly{Kind: "wrongly-rejected", Basis: "catalog-contract", Round: r,
			EvidenceIDs: []int{load.invoke.ID},
			Detail:      "every create of a new namespace was rejected with 409 as already existing, and the namespace does not exist"})
	}
}

func parseNamespaceHistory(ops []history.Op) (m namespaceManifest, calls []*namespaceCall, err error) {
	bad := func(msg string) (namespaceManifest, []*namespaceCall, error) {
		return m, nil, fmt.Errorf("namespace history: %s", msg)
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" || json.Unmarshal(ops[0].Value, &m) != nil ||
		m.Workload != "namespace-create" || !namespaceRunID.MatchString(m.RunID) ||
		m.Clients < 2 || m.Rounds <= 0 || m.Clients > int(^uint(0)>>1)/m.Rounds {
		return bad("missing or invalid namespace run manifest")
	}
	for _, level := range strings.Split(m.Parent, ".") {
		if strings.TrimSpace(level) == "" {
			return bad("empty parent level")
		}
	}
	byID := map[int]*namespaceCall{}
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
			r, err := strconv.Atoi(op.Round)
			if err != nil || strconv.Itoa(r) != op.Round || r < 0 || r >= m.Rounds {
				return bad("operation has an invalid round")
			}
			u, err := url.Parse(op.Request.URL)
			if err != nil {
				return bad("unparseable request URL")
			}
			path := u.EscapedPath()
			switch {
			case op.F == "create-namespace" && op.Request.Method == "POST" && op.Phase == "workload":
				if op.Process < 0 || op.Process >= m.Clients || !strings.HasSuffix(path, "/namespaces") {
					return bad("create has an invalid process or route")
				}
				if route == "" {
					route = path
				}
				if err := validateNamespaceCreate(op, m, r); err != nil {
					return bad(fmt.Sprintf("create %d: %v", op.ID, err))
				}
			case op.F == "load-namespace" && op.Request.Method == "GET" && op.Phase == "final":
				if op.Process != -1 {
					return bad("load has an invalid process")
				}
			default:
				return bad("unsupported operation " + op.F)
			}
			call := &namespaceCall{invoke: op}
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
	// Every create of the run goes to one route, and each load reads its
	// round's namespace under that route.
	for _, call := range calls {
		u, _ := url.Parse(call.invoke.Request.URL)
		r, _ := strconv.Atoi(call.invoke.Round)
		want := route
		if call.invoke.F == "load-namespace" {
			want += "/" + url.PathEscape(strings.Join(namespaceLevels(m, r), "\x1f"))
		}
		if route == "" || u.EscapedPath() != want {
			return bad("operations use different routes or another namespace")
		}
	}
	return m, calls, nil
}

func namespaceLevels(m namespaceManifest, round int) []string {
	return append(strings.Split(m.Parent, "."), fmt.Sprintf("%s_%d", m.RunID, round))
}

func namespaceOwner(round, process int) string { return fmt.Sprintf("%d-%d", round, process) }

// validateNamespaceCreate requires the exact request the workload sends: its
// round's namespace with only its own owner property.
func validateNamespaceCreate(op *history.Op, m namespaceManifest, round int) error {
	owner := namespaceOwner(round, op.Process)
	var marker struct {
		Owner string `json:"owner"`
	}
	if json.Unmarshal(op.Value, &marker) != nil || marker.Owner != owner {
		return fmt.Errorf("marker does not identify its round and process")
	}
	var body struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(op.Request.Body, &body); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if !reflect.DeepEqual(body.Namespace, namespaceLevels(m, round)) {
		return fmt.Errorf("request names another namespace")
	}
	if !reflect.DeepEqual(body.Properties, map[string]string{namespaceOwnerKey: owner}) {
		return fmt.Errorf("request properties are not exactly its owner")
	}
	return nil
}

// readNamespaceView decodes a round's load. A 404 shows the namespace absent;
// any other failure, or no response, leaves it unknown.
func readNamespaceView(call *namespaceCall, levels []string) (namespaceView, error) {
	if call.done == nil {
		return namespaceView{}, nil
	}
	switch {
	case call.done.Type == history.Fail && call.done.Status == 404:
		return namespaceView{known: true}, nil
	case call.done.Type != history.OK || call.done.Response == nil:
		return namespaceView{}, nil
	}
	var body struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(call.done.Response.Body, &body); err != nil {
		return namespaceView{}, fmt.Errorf("decode namespace: %w", err)
	}
	if !reflect.DeepEqual(body.Namespace, levels) {
		return namespaceView{}, fmt.Errorf("load returned another namespace")
	}
	owner, ok := body.Properties[namespaceOwnerKey]
	return namespaceView{known: true, exists: true, hasOwner: ok, owner: owner}, nil
}

func namespaceMarker(call *namespaceCall) string {
	var marker struct {
		Owner string `json:"owner"`
	}
	json.Unmarshal(call.invoke.Value, &marker)
	return marker.Owner
}
