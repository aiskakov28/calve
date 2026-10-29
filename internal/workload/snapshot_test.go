package workload_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
	"github.com/aiskakov28/calve/internal/workload"
)

func runSnapshotFixture(t *testing.T, bugs fakecatalog.Bugs, clients int) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	cfg := workload.SnapshotConfig{Namespace: "test.nested", Clients: clients, Rounds: 2, ManifestRoot: t.TempDir()}
	run, err := workload.RunSnapshots(context.Background(), rest.NewClient(fake.Server.URL, "", "snapshot-private-token", time.Second), cfg, storage.ManifestStore{}, rec)
	if err != nil {
		t.Fatal(err)
	}
	var saved bytes.Buffer
	if err = history.WriteJSONL(&saved, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(saved.Bytes(), []byte("snapshot-private-token")) {
		t.Fatal("token in history")
	}
	ops, err := history.ReadJSONL(&saved)
	if err != nil {
		t.Fatal(err)
	}
	commits := 0
	for _, op := range ops {
		if op.F == "config" || op.F == "create-table" || op.F == "create-namespace" || op.Request != nil && op.Request.Method == "DELETE" {
			t.Fatal("setup/destructive request in checked history", op)
		}
		if op.F == "commit-table" && op.Type == history.Invoke {
			commits++
			var body struct {
				Updates []struct {
					Snapshot struct {
						Manifest string `json:"manifest-list"`
					} `json:"snapshot"`
				} `json:"updates"`
			}
			if json.Unmarshal(op.Request.Body, &body) != nil || len(body.Updates) != 2 {
				t.Fatal("bad snapshot request")
			}
			u, _ := url.Parse(body.Updates[0].Snapshot.Manifest)
			data, e := os.ReadFile(u.Path)
			if e != nil || !bytes.HasPrefix(data, []byte("Obj\x01")) {
				t.Fatal("snapshot does not reference a real manifest list", e)
			}
		}
	}
	if commits != clients*2 || run.Commits != commits || ops[1].Phase != "baseline" {
		t.Fatal(run, commits)
	}
	return ops
}

func TestSnapshotWorkloadAndPlantedControls(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		want checker.Verdict
	}{
		{"correct", fakecatalog.Bugs{}, checker.Valid},
		{"lost", fakecatalog.Bugs{LoseEvery: 1}, checker.Invalid},
		{"partial", fakecatalog.Bugs{PartialSnapshots: true}, checker.Invalid},
		{"rejected effect", fakecatalog.Bugs{RejectAppliedSnapshots: true}, checker.Invalid},
		{"stale reads", fakecatalog.Bugs{StaleSnapshotReads: true}, checker.Invalid},
		{"unknown applied", fakecatalog.Bugs{UnknownAppliedSnapshots: true}, checker.Valid},
		{"unknown absent", fakecatalog.Bugs{UnknownUnappliedSnapshots: true}, checker.Inconclusive},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := runSnapshotFixture(t, tt.bugs, 1)
			for _, model := range []string{checker.SnapshotModelVersion, checker.SnapshotCurrentModelVersion} {
				var manifest checker.SnapshotManifest
				json.Unmarshal(ops[0].Value, &manifest)
				manifest.Model = model
				ops[0].Value, _ = json.Marshal(manifest)
				res, err := checker.CheckSnapshotHistory(ops, time.Second)
				if err != nil || res.Verdict != tt.want || res.Model != model {
					t.Fatal(model, res, err)
				}
				if tt.bugs.UnknownAppliedSnapshots && (res.Unknown != 2 || res.ResolvedCommitted != 2) {
					t.Fatal(res)
				}
			}
		})
	}
}

func TestSnapshotWorkloadConcurrentRounds(t *testing.T) {
	ops := runSnapshotFixture(t, fakecatalog.Bugs{}, 4)
	res, err := checker.CheckSnapshotHistory(ops, time.Second)
	if err != nil || res.Verdict != checker.Valid || res.OK != 2 || res.Failed != 6 {
		t.Fatal(res, err)
	}
}

func TestCurrentSnapshotProjectionIsExplicit(t *testing.T) {
	ops := runSnapshotFixture(t, fakecatalog.Bugs{UnknownAppliedSnapshots: true}, 1)
	// Synthetic response projection of Nessie's documented single-snapshot
	// policy. This fixture is not a claimed live-catalog finding.
	for i, op := range ops {
		if op.Response == nil || op.Type != history.OK {
			continue
		}
		var body map[string]json.RawMessage
		json.Unmarshal(op.Response.Body, &body)
		var meta map[string]json.RawMessage
		json.Unmarshal(body["metadata"], &meta)
		var current *int64
		json.Unmarshal(meta["current-snapshot-id"], &current)
		var snapshots []json.RawMessage
		json.Unmarshal(meta["snapshots"], &snapshots)
		selected := []json.RawMessage{}
		for _, raw := range snapshots {
			var snapshot map[string]json.RawMessage
			json.Unmarshal(raw, &snapshot)
			var id int64
			json.Unmarshal(snapshot["snapshot-id"], &id)
			if current != nil && id == *current {
				delete(snapshot, "parent-snapshot-id")
				projected, _ := json.Marshal(snapshot)
				selected = append(selected, projected)
			}
		}
		meta["snapshots"], _ = json.Marshal(selected)
		body["metadata"], _ = json.Marshal(meta)
		response := *op.Response
		response.Body, _ = json.Marshal(body)
		ops[i].Response = &response
	}
	full, err := checker.CheckSnapshotHistory(ops, time.Second)
	if err != nil || full.Verdict != checker.Invalid {
		t.Fatal(full, err)
	}
	var manifest checker.SnapshotManifest
	json.Unmarshal(ops[0].Value, &manifest)
	manifest.Model = checker.SnapshotCurrentModelVersion
	ops[0].Value, _ = json.Marshal(manifest)
	var before bytes.Buffer
	history.WriteJSONL(&before, ops)
	current, err := checker.CheckSnapshotHistory(ops, time.Second)
	if err != nil || current.Verdict != checker.Valid || current.ResolvedCommitted != 2 || current.Unknown != 2 {
		t.Fatal(current, err)
	}
	var after bytes.Buffer
	history.WriteJSONL(&after, ops)
	if !bytes.Equal(before.Bytes(), after.Bytes()) {
		t.Fatal("checker rewrote source history")
	}
}

type failedManifestStore struct{}

func (failedManifestStore) Put(context.Context, string, []byte) error {
	return errors.New("upload rejected")
}

func TestSnapshotPreparationFailureDoesNotCommit(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	rec := history.NewRecorder()
	_, err := workload.RunSnapshots(context.Background(), rest.NewClient(fake.Server.URL, "", "", time.Second),
		workload.SnapshotConfig{Namespace: "test", Clients: 2, Rounds: 2, ManifestRoot: t.TempDir()}, failedManifestStore{}, rec)
	if err == nil || !strings.Contains(err.Error(), "upload rejected") {
		t.Fatal(err)
	}
	for _, op := range rec.Ops() {
		if op.F == "commit-table" {
			t.Fatal("committed missing manifest")
		}
	}
	res, err := checker.CheckSnapshotHistory(rec.Ops(), time.Second)
	if err != nil || res.Verdict != checker.Inconclusive {
		t.Fatal(res, err)
	}
}
