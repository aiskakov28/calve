package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

type controlTransport func(*http.Request) (*http.Response, error)

func (f controlTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func replayControlHistory(t *testing.T, ops []history.Op) []history.Op {
	t.Helper()
	var data bytes.Buffer
	if err := history.WriteJSONL(&data, ops); err != nil {
		t.Fatal(err)
	}
	copy, err := history.ReadJSONL(&data)
	if err != nil {
		t.Fatal(err)
	}
	return copy
}

func propertyControlHistory(t *testing.T, bugs fakecatalog.Bugs) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	manifest, _ := json.Marshal(map[string]any{"run_id": "append-control", "workload": "append", "catalog": fake.Server.URL,
		"namespace": "test", "table": "properties", "clients": 2, "per_client": 3})
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: manifest})
	client := rest.NewClient(fake.Server.URL, "", "", time.Second).WithRecorder(rec)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	setup := history.WithOperation(ctx, history.Op{Process: -1, Phase: "setup"})
	if _, err := client.Config(setup); err != nil {
		t.Fatal(err)
	}
	cfg := workload.AppendConfig{Namespace: "test", Table: "properties", Clients: 2, PerClient: 3}
	if err := workload.SetupTable(setup, client, cfg.Namespace, cfg.Table); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"baseline", "final"} {
		if phase == "final" {
			workload.RunAppend(ctx, client, cfg, rec)
		}
		if _, err := rest.ParseMetadata(client.LoadTable(history.WithOperation(ctx, history.Op{Process: -1, Phase: phase}), cfg.Namespace, cfg.Table)); err != nil {
			t.Fatal(err)
		}
	}
	fake.Close()
	return replayControlHistory(t, rec.Ops())
}

func TestPlantedPropertyAppendControls(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		bugs                     fakecatalog.Bugs
		want                     Verdict
		kind                     string
		count, unknown, resolved int
	}{
		{name: "correct", want: Valid},
		{name: "lost", bugs: fakecatalog.Bugs{LoseEvery: 2}, want: Invalid, kind: "lost", count: 3},
		{name: "rejected effect", bugs: fakecatalog.Bugs{RejectAppliedProperties: true}, want: Invalid, kind: "phantom-failed", count: 6},
		{name: "unrequested effect", bugs: fakecatalog.Bugs{UnexpectedProperties: true}, want: Invalid, kind: "unexpected", count: 1},
		{name: "unknown applied", bugs: fakecatalog.Bugs{UnknownAppliedProperties: true}, want: Valid, unknown: 6, resolved: 6},
		{name: "unknown absent", bugs: fakecatalog.Bugs{UnknownUnappliedProperties: true}, want: Inconclusive, unknown: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := propertyControlHistory(t, tt.bugs)
			res, err := CheckAppendHistory(ops)
			if err != nil || res.Verdict != tt.want || res.Unknown != tt.unknown || res.ResolvedCommitted != tt.resolved || res.Unresolved != tt.unknown-tt.resolved {
				t.Fatal(res, err)
			}
			if len(res.Anomalies) != tt.count || tt.kind != "" && kinds(res)[tt.kind] != tt.count {
				t.Fatal(res.Anomalies)
			}
			if tt.unknown > 0 {
				for _, op := range ops {
					if op.F == "append" && op.Type != history.Invoke && op.Type != history.Info {
						t.Fatal("unknown outcome was rewritten", op)
					}
				}
			}
		})
	}
}

func TestPlantedAtomicUnexpectedProperties(t *testing.T) {
	res := requireAtomic(t, atomicHistory(t, fakecatalog.Bugs{UnexpectedProperties: true}, 1, 1), Invalid)
	if len(res.Anomalies) != 3 {
		t.Fatal(res)
	}
	for _, finding := range res.Anomalies {
		if finding.Kind != "unexpected" || finding.Basis != "catalog-contract" || len(finding.EvidenceIDs) != 1 {
			t.Fatal(finding)
		}
	}
}

func TestPlantedAtomicPropertyRegression(t *testing.T) {
	for _, buggy := range []bool{false, true} {
		t.Run(fmt.Sprint(buggy), func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{ForgetTransactionProperties: buggy, UnknownAppliedTransactions: true})
			defer fake.Close()
			rec := history.NewRecorder()
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			observer := client.WithRecorder(rec)
			var first sync.Once
			var observationErr error
			client.HTTP.Transport = controlTransport(func(req *http.Request) (*http.Response, error) {
				response, err := http.DefaultTransport.RoundTrip(req)
				if err == nil && history.Operation(req.Context()).F == "atomic-txn" {
					first.Do(func() {
						// Record a real GET after publication and before the next
						// transaction. Responses and source outcomes are untouched.
						body, e := req.GetBody()
						if e != nil {
							observationErr = e
							return
						}
						defer body.Close()
						var request struct {
							Changes []rest.TableChange `json:"table-changes"`
						}
						if e = json.NewDecoder(body).Decode(&request); e != nil {
							observationErr = e
							return
						}
						id := request.Changes[0].Identifier
						call := history.WithOperation(req.Context(), history.Op{Process: -1, Phase: "workload"})
						_, observationErr = rest.ParseMetadata(observer.LoadTable(call, strings.Join(id.Namespace, "."), id.Name))
					})
				}
				return response, err
			})
			if _, err := workload.RunAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 2}, rec); err != nil {
				t.Fatal(err)
			}
			if observationErr != nil {
				t.Fatal(observationErr)
			}
			fake.Close()
			want := Valid
			if buggy {
				want = Invalid
			}
			res := requireAtomic(t, replayControlHistory(t, rec.Ops()), want)
			if !buggy && len(res.Anomalies) != 0 {
				t.Fatal(res.Anomalies)
			}
			if buggy && (len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "single-table-regression" ||
				res.Anomalies[0].Basis != "fresh-read-model" || len(res.Anomalies[0].Tables) != 1 || len(res.Anomalies[0].EvidenceIDs) != 3) {
				t.Fatal(res.Anomalies)
			}
		})
	}
}
