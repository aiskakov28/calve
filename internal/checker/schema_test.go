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

func schemaHistory(t *testing.T, bugs fakecatalog.Bugs, clients, rounds int) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	client := rest.NewClient(fake.Server.URL, "", "schema-private-token", time.Second)
	if _, err := workload.RunSchema(context.Background(), client, workload.SchemaConfig{Namespace: "test", Clients: clients, Rounds: rounds}, rec); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err := history.WriteJSONL(&data, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(data.String(), "schema-private-token") {
		t.Fatal("credential leaked")
	}
	ops, err := history.ReadJSONL(&data)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func requireSchema(t *testing.T, ops []history.Op, want Verdict) SchemaResult {
	t.Helper()
	result, err := CheckSchema(ops)
	if err != nil || result.Verdict != want {
		t.Fatalf("want %s; got %+v; error %v", want, result, err)
	}
	return result
}

func schemaKinds(res SchemaResult) map[string]int {
	kinds := map[string]int{}
	for _, a := range res.Anomalies {
		kinds[a.Kind]++
	}
	return kinds
}

func TestSchemaCheckerCorrectCatalog(t *testing.T) {
	res := requireSchema(t, schemaHistory(t, fakecatalog.Bugs{}, 4, 5), Valid)
	if res.RoundsWithWinner != 5 || res.OK != 5 || res.Failed != 15 || res.Unknown != 0 || len(res.Anomalies) != 0 {
		t.Fatalf("want one acknowledged winner per round and the rest rejected: %+v", res)
	}
}

func TestSchemaCheckerPlantedBugs(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		kind string
	}{
		// Every contender passes the identity check and commits.
		{"ignored schema guard", fakecatalog.Bugs{IgnoreSchemaRequirements: true}, "multiple-winners"},
		// The winner's change is applied but answered with 409.
		{"rejected but applied", fakecatalog.Bugs{RejectAppliedSchemas: true}, "phantom-failed"},
		// Acknowledged without applying, so the next contender's guard still holds.
		{"acknowledged but not applied", fakecatalog.Bugs{LoseEvery: 1}, "multiple-winners"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := requireSchema(t, schemaHistory(t, tt.bugs, 3, 3), Invalid)
			if schemaKinds(res)[tt.kind] != 3 {
				t.Fatalf("want a %s finding in each of 3 rounds: %+v", tt.kind, res.Anomalies)
			}
		})
	}
}

func TestSchemaCheckerResolvesUnknownsOnlyFromEffects(t *testing.T) {
	// 503 after applying: the column proves each winner committed.
	res := requireSchema(t, schemaHistory(t, fakecatalog.Bugs{UnknownAppliedSchemas: true}, 3, 3), Valid)
	if res.Unknown != 3 || res.ResolvedCommitted != 3 || res.Unresolved != 0 {
		t.Fatalf("want three unknown winners resolved from their columns: %+v", res)
	}

	// A rejected loser whose response was lost stays unknown: absence is not rejection.
	ops := schemaHistory(t, fakecatalog.Bugs{}, 3, 2)
	for i := range ops {
		if ops[i].F == "commit-table" && ops[i].Type == history.Fail {
			ops[i].Type, ops[i].Status, ops[i].Response = history.Info, 0, nil
			break
		}
	}
	res = requireSchema(t, ops, Inconclusive)
	if res.Unresolved != 1 || len(res.Anomalies) != 0 {
		t.Fatalf("want one unresolved commit and no findings: %+v", res)
	}
}

func TestSchemaCheckerLostChange(t *testing.T) {
	ops := schemaHistory(t, fakecatalog.Bugs{}, 2, 3)
	// The final load shows the table rolled back to its first schema.
	final := &ops[len(ops)-1]
	if final.F != "load-table" || final.Phase != "final" || final.Type != history.OK {
		t.Fatalf("last record is not the final load: %+v", final)
	}
	var body map[string]json.RawMessage
	var metadata map[string]any
	if err := json.Unmarshal(final.Response.Body, &body); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body["metadata"], &metadata); err != nil {
		t.Fatal(err)
	}
	metadata["current-schema-id"] = 0
	body["metadata"], _ = json.Marshal(metadata)
	final.Response.Body, _ = json.Marshal(body)
	res := requireSchema(t, ops, Invalid)
	if schemaKinds(res)["lost"] != 3 {
		t.Fatalf("want every round's acknowledged change reported lost: %+v", res.Anomalies)
	}
}

func TestSchemaCheckerRejectsMalformedHistories(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func([]history.Op) []history.Op
	}{
		{"missing manifest", func(ops []history.Op) []history.Op { ops[0].Value = json.RawMessage(`{}`); return ops }},
		{"stale guard", func(ops []history.Op) []history.Op {
			for i := range ops {
				if ops[i].F == "commit-table" && ops[i].Type == history.Invoke {
					ops[i].Request.Body = bytes.Replace(ops[i].Request.Body, []byte(`"current-schema-id":0`), []byte(`"current-schema-id":7`), 1)
					break
				}
			}
			return ops
		}},
		{"other table", func(ops []history.Op) []history.Op {
			for i := range ops {
				if ops[i].Type == history.Invoke {
					ops[i].Request.URL += "_other"
					break
				}
			}
			return ops
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := CheckSchema(tt.mutate(schemaHistory(t, fakecatalog.Bugs{}, 2, 2))); err == nil {
				t.Fatal("want a history error")
			}
		})
	}
}
