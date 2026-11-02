package checker

import (
	"encoding/json"
	"testing"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
)

// Construct synthetic schedules with reads during the request. The normal
// runner reads after attempts; the checker explicitly also accepts workload
// observations and must apply its visibility rule to those intervals.
func atomicPendingViews(t *testing.T, rejected bool) []history.Op {
	t.Helper()
	original := atomicHistory(t, fakecatalog.Bugs{}, 1, 1)
	var prefix, final []history.Op
	var invoke, done history.Op
	for _, op := range original {
		switch {
		case op.F == "atomic-txn" && op.Type == history.Invoke:
			invoke = op
		case op.F == "atomic-txn":
			done = op
		case op.Phase == "final":
			final = append(final, op)
		default:
			prefix = append(prefix, op)
		}
	}
	empty := func(op history.Op) history.Op {
		var body map[string]any
		if err := json.Unmarshal(op.Response.Body, &body); err != nil {
			t.Fatal(err)
		}
		body["metadata"].(map[string]any)["properties"] = map[string]string{}
		response := *op.Response
		response.Body = snapshotRaw(body)
		op.Response = &response
		return op
	}
	prefix = append(prefix, invoke)
	// A positive table-0 observation completes while the commit is pending.
	for _, op := range final[:2] {
		op.ID, op.Phase = 1000, "workload"
		prefix = append(prefix, op)
	}
	if rejected {
		for i := 2; i < len(final); i += 2 {
			for _, op := range final[i : i+2] {
				op.ID, op.Phase = 1000+i, "workload"
				prefix = append(prefix, op)
			}
		}
		done.Type, done.Status, done.Error = history.Fail, 409, "rejected"
		done.Response = &history.Response{Status: 409, Body: []byte("{}")}
		for i := range final {
			if final[i].Type == history.OK {
				final[i] = empty(final[i])
			}
		}
	} else {
		// A later table-1 read still sees no effect, before the acknowledgment.
		for _, op := range final[2:4] {
			op.ID, op.Phase = 1001, "workload"
			if op.Type == history.OK {
				op = empty(op)
			}
			prefix = append(prefix, op)
		}
	}
	prefix = append(prefix, done)
	prefix = append(prefix, final...)
	return stampAtomic(prefix)
}

func TestAtomicPendingVisibilityBeforeAcknowledgment(t *testing.T) {
	ops := atomicPendingViews(t, false)
	res := requireAtomic(t, ops, Invalid)
	if len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "non-atomic-visibility" ||
		res.Anomalies[0].Basis != "atomic-visibility-model" || res.OK != 1 {
		t.Fatal(res)
	}
}

func TestAtomicPreRejectionVisibilityIsModelEvidence(t *testing.T) {
	ops := atomicPendingViews(t, true)
	res := requireAtomic(t, ops, Invalid)
	if len(res.Anomalies) != 1 || res.Anomalies[0].Basis != "fresh-read-model" ||
		res.Anomalies[0].Kind != "single-table-regression" {
		t.Fatalf("pre-rejection exposure was presented as a post-rejection contract finding: %+v", res)
	}
	if res.Transactions[0].Outcome != history.Fail || res.Transactions[0].CommitState != "rejected" {
		t.Fatal(res)
	}
}
