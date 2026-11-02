package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

type acceptManifests struct{}

func (acceptManifests) Put(context.Context, string, []byte) error { return nil }

func maintenanceHistory(t *testing.T, bugs fakecatalog.Bugs, appends int) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	cfg := workload.MaintenanceConfig{Namespace: "test", Appends: appends, ExpireInterval: time.Millisecond, ManifestRoot: t.TempDir()}
	if _, err := workload.RunMaintenance(context.Background(), client, cfg, acceptManifests{}, rec); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err := history.WriteJSONL(&data, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	ops, err := history.ReadJSONL(&data)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func maintenanceKinds(res MaintenanceResult) map[string]int {
	kinds := map[string]int{}
	for _, a := range res.Anomalies {
		kinds[a.Kind]++
	}
	return kinds
}

func TestMaintenanceCheckerUnknownAppends(t *testing.T) {
	// 503 after applying: the snapshot appears, so the appends resolve.
	res, err := CheckMaintenance(maintenanceHistory(t, fakecatalog.Bugs{UnknownAppliedSnapshots: true}, 10))
	if err != nil || len(res.Anomalies) != 0 || res.AppendsUnknown != 10 || res.ResolvedCommitted != 10 || res.Unresolved != 0 {
		t.Fatalf("want ten unknown appends resolved from their snapshots: %+v, %v", res, err)
	}
	// 503 without applying: absence never proves rejection.
	res, err = CheckMaintenance(maintenanceHistory(t, fakecatalog.Bugs{UnknownUnappliedSnapshots: true}, 5))
	if err != nil || res.Verdict != Inconclusive || len(res.Anomalies) != 0 || res.Unresolved != 5 {
		t.Fatalf("want five unresolved appends and no findings: %+v, %v", res, err)
	}
}

func TestMaintenanceCheckerRejectsMalformedHistories(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func([]history.Op)
	}{
		{"missing manifest", func(ops []history.Op) { ops[0].Value = json.RawMessage(`{}`) }},
		{"unguarded append", func(ops []history.Op) {
			for i := range ops {
				if ops[i].F == "commit-table" && ops[i].Type == history.Invoke && ops[i].Process == 0 {
					ops[i].Request.Body = bytes.Replace(ops[i].Request.Body, []byte(`,{"ref":"main","snapshot-id":null,"type":"assert-ref-snapshot-id"}`), nil, 1)
					return
				}
			}
		}},
		{"expiration lists an unobserved snapshot", func(ops []history.Op) {
			for i := range ops {
				if ops[i].F == "commit-table" && ops[i].Type == history.Invoke && ops[i].Process == 1 {
					// Label and request agree; only the listed set is wrong.
					var label workload.MaintenanceOp
					json.Unmarshal(ops[i].Value, &label)
					label.Snapshots = append(label.Snapshots, 999)
					ops[i].Value, _ = json.Marshal(label)
					ops[i].Request.Body = bytes.Replace(ops[i].Request.Body, []byte(`]}]}`), []byte(`,999]}]}`), 1)
					return
				}
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := maintenanceHistory(t, fakecatalog.Bugs{}, 40)
			tt.mutate(ops)
			if _, err := CheckMaintenance(ops); err == nil {
				t.Fatal("want a history error")
			}
		})
	}
}

func TestMaintenanceCurrentSnapshotModel(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	rec := history.NewRecorder()
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	cfg := workload.MaintenanceConfig{Namespace: "test", Appends: 5, ExpireInterval: time.Hour, ManifestRoot: t.TempDir()}
	if _, err := workload.RunMaintenance(context.Background(), client, cfg, acceptManifests{}, rec); err != nil {
		t.Fatal(err)
	}
	// The expirer's first pass races the first appends and may expire real
	// snapshots, which no single-snapshot view could show. Drop its requests so
	// the appends alone are checked, however the goroutines were scheduled, and
	// renumber the records as a recorder would have.
	var ops []history.Op
	for _, op := range rec.Ops() {
		if op.Process != 1 {
			op.Index = len(ops)
			ops = append(ops, op)
		}
	}
	// Show only the current snapshot, as a single-snapshot REST view does.
	for i := range ops {
		if ops[i].F != "load-table" || ops[i].Type != history.OK {
			continue
		}
		var body map[string]json.RawMessage
		var metadata map[string]any
		json.Unmarshal(ops[i].Response.Body, &body)
		json.Unmarshal(body["metadata"], &metadata)
		var current []any
		for _, s := range metadata["snapshots"].([]any) {
			if s.(map[string]any)["snapshot-id"] == metadata["current-snapshot-id"] {
				current = append(current, s)
			}
		}
		metadata["snapshots"] = current
		body["metadata"], _ = json.Marshal(metadata)
		ops[i].Response.Body, _ = json.Marshal(body)
	}
	res, err := CheckMaintenance(ops)
	if err != nil || maintenanceKinds(res)["lost-snapshot"] == 0 {
		t.Fatalf("the retained-history model should not accept a single-snapshot view: %+v, %v", res, err)
	}
	var run map[string]any
	json.Unmarshal(ops[0].Value, &run)
	run["model"] = MaintenanceCurrentModelVersion
	ops[0].Value, _ = json.Marshal(run)
	res, err = CheckMaintenance(ops)
	if err != nil || res.Verdict != Inconclusive || len(res.Anomalies) != 0 || res.AppendsOK != 5 {
		t.Fatalf("want no findings and no expiration coverage under the current model: %+v, %v", res, err)
	}
}
