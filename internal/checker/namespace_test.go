package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func namespaceHistory(t *testing.T, bugs fakecatalog.Bugs, clients, rounds int) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	client := rest.NewClient(fake.Server.URL, "", "namespace-private-token", time.Second)
	if _, err := workload.RunNamespace(context.Background(), client, workload.NamespaceConfig{Parent: "test", Clients: clients, Rounds: rounds}, rec); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err := history.WriteJSONL(&data, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(data.String(), "namespace-private-token") {
		t.Fatal("credential leaked")
	}
	ops, err := history.ReadJSONL(&data)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func requireNamespace(t *testing.T, ops []history.Op, want Verdict) NamespaceResult {
	t.Helper()
	result, err := CheckNamespace(ops)
	if err != nil || result.Verdict != want {
		t.Fatalf("want %s; got %+v; error %v", want, result, err)
	}
	return result
}

func namespaceKinds(res NamespaceResult) map[string]int {
	kinds := map[string]int{}
	for _, a := range res.Anomalies {
		kinds[a.Kind]++
	}
	return kinds
}

func TestNamespaceCheckerCorrectCatalog(t *testing.T) {
	res := requireNamespace(t, namespaceHistory(t, fakecatalog.Bugs{}, 4, 5), Valid)
	if res.Rounds != 5 || res.RoundsWithWinner != 5 || res.OK != 5 || res.Failed != 15 || res.Unknown != 0 || len(res.Anomalies) != 0 {
		t.Fatalf("want one acknowledged create per round and the rest rejected: %+v", res)
	}
}

func TestNamespaceCheckerPlantedBugs(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		kind string
	}{
		// Every contender creates the namespace again.
		{"ignored existence check", fakecatalog.Bugs{IgnoreNamespaceExists: true}, "multiple-winners"},
		// Acknowledged without creating, so the next contender also succeeds.
		{"acknowledged but not created", fakecatalog.Bugs{LoseNamespaces: true}, "multiple-winners"},
		// The creating request is answered with 409.
		{"rejected but created", fakecatalog.Bugs{RejectAppliedNamespaces: true}, "phantom-failed"},
		// Every create is answered with 409 and nothing is created.
		{"new namespace rejected", fakecatalog.Bugs{RejectNewNamespaces: true}, "wrongly-rejected"},
		// The namespace is created with properties no contender sent.
		{"unexpected properties", fakecatalog.Bugs{UnexpectedNamespaceProperties: true}, "unexpected"},
		{"acknowledged create's owner missing", fakecatalog.Bugs{UnexpectedNamespaceProperties: true}, "lost"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := requireNamespace(t, namespaceHistory(t, tt.bugs, 3, 3), Invalid)
			if namespaceKinds(res)[tt.kind] != 3 {
				t.Fatalf("want a %s finding in each of 3 rounds: %+v", tt.kind, res.Anomalies)
			}
		})
	}
}

func TestNamespaceCheckerResolvesUnknownsOnlyFromEffects(t *testing.T) {
	// The creating request gets 500; its stored owner proves it took effect.
	res := requireNamespace(t, namespaceHistory(t, fakecatalog.Bugs{UnknownAppliedNamespaces: true}, 3, 4), Valid)
	if res.Unknown != 4 || res.ResolvedCommitted != 4 || res.Unresolved != 0 || res.RoundsWithWinner != 4 {
		t.Fatalf("want each round's unknown creator resolved from its stored owner: %+v", res)
	}

	// Losers get 500 instead of 409, as Iceberg's JDBC catalog answers: their
	// owners are absent, which never proves rejection, so the run cannot be
	// valid; nor is it a finding.
	res = requireNamespace(t, namespaceHistory(t, fakecatalog.Bugs{ServerErrorForExistingNamespace: true}, 3, 4), Inconclusive)
	if len(res.Anomalies) != 0 || res.OK != 4 || res.Unknown != 8 || res.Unresolved != 8 || res.UnknownBesideWinner != 8 {
		t.Fatalf("want 8 unresolved creates beside 4 proven winners and no findings: %+v", res)
	}
	if !strings.Contains(strings.Join(res.Reasons, "\n"), "409") {
		t.Fatalf("want the reasons to name the spec's 409: %v", res.Reasons)
	}
}

func TestNamespaceCheckerUnavailableLoadIsInconclusive(t *testing.T) {
	ops := namespaceHistory(t, fakecatalog.Bugs{}, 3, 2)
	for i := range ops {
		if ops[i].F == "load-namespace" && ops[i].Type == history.OK && ops[i].Round == "1" {
			ops[i].Type, ops[i].Status, ops[i].Response, ops[i].Error = history.Info, 0, nil, "no response: timeout"
		}
	}
	res := requireNamespace(t, ops, Inconclusive)
	if len(res.Anomalies) != 0 || !strings.Contains(strings.Join(res.Reasons, "\n"), "round 1 final observation is unavailable") {
		t.Fatalf("want round 1 undecided without findings: %+v", res)
	}
}

func TestNamespaceCheckerOnlyConflictsProveWrongRejection(t *testing.T) {
	// Every create is rejected and the namespace is absent. With 409s that is
	// a finding; with 403s, which claim nothing about existence, it is not.
	ops := namespaceHistory(t, fakecatalog.Bugs{RejectNewNamespaces: true}, 3, 2)
	for i := range ops {
		if ops[i].F == "create-namespace" && ops[i].Type == history.Fail {
			ops[i].Status, ops[i].Response.Status = 403, 403
		}
	}
	res := requireNamespace(t, ops, Inconclusive)
	if len(res.Anomalies) != 0 || res.Failed != 6 || res.RoundsWithWinner != 0 {
		t.Fatalf("want 403 rejections left undecided without findings: %+v", res)
	}
}

func TestNamespaceCheckerRejectsForeignEvidence(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(op *history.Op)
	}{
		{"create for another namespace", func(op *history.Op) {
			if op.F == "create-namespace" && op.Type == history.Invoke && op.Process == 1 {
				op.Request.Body = bytes.Replace(op.Request.Body, []byte(`_0"`), []byte(`_9"`), 1)
			}
		}},
		{"create with extra properties", func(op *history.Op) {
			if op.F == "create-namespace" && op.Type == history.Invoke && op.Process == 1 {
				var body map[string]any
				json.Unmarshal(op.Request.Body, &body)
				body["properties"].(map[string]any)["extra"] = "1"
				op.Request.Body, _ = json.Marshal(body)
			}
		}},
		{"marker for another process", func(op *history.Op) {
			if op.F == "create-namespace" && op.Process == 1 {
				op.Value = json.RawMessage(`{"owner":"0-2"}`)
			}
		}},
		{"load of another namespace", func(op *history.Op) {
			if op.F == "load-namespace" && op.Type == history.Invoke {
				op.Request.URL += "x"
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := namespaceHistory(t, fakecatalog.Bugs{}, 3, 1)
			for i := range ops {
				tt.mutate(&ops[i])
			}
			if _, err := CheckNamespace(ops); err == nil {
				t.Fatal("want the history rejected")
			}
		})
	}
}
