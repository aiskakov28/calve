package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/workload"
)

func runSnapshotCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32) (checker.SnapshotResult, *campaign, string, error) {
	t.Helper()
	camp, client, _, dir := testCampaign(t, bugs, mode, nil)
	cfg := workload.SnapshotConfig{Namespace: "test", Clients: 2, Rounds: int(camp.plan.Config.RunDuration / time.Millisecond),
		ManifestRoot: t.TempDir()}
	accept := snapshotWriterFunc(func(context.Context, string, []byte) error { return nil })
	res, err := executeSnapshotCampaign(context.Background(), client, cfg, accept, readbackConfig{}, camp, dir, "reference", "run_test")
	return res, camp, dir, err
}

func TestSnapshotCampaignCorrectCatalogUnderLatency(t *testing.T) {
	res, camp, dir, err := runSnapshotCampaign(t, fakecatalog.Bugs{}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Valid || res.OK < 2 {
		t.Fatalf("want a valid run with acknowledged rounds: %s %+v", camp.verdict, res)
	}
	for _, c := range camp.coverage {
		if c.Commits == 0 {
			t.Fatalf("fault %s met no commit although rounds ran to the horizon", c.Event)
		}
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 0 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

func TestSnapshotCampaignRetriesGuardReadsUnderFaults(t *testing.T) {
	_, camp, dir, err := runSnapshotCampaign(t, fakecatalog.Bugs{}, down)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict == checker.Invalid {
		t.Fatal("a correct catalog behind failing requests was reported invalid")
	}
	ops, err := readOps(filepath.Join(dir, historyFile))
	if err != nil {
		t.Fatal(err)
	}
	failedGuards, okAfter := 0, 0
	for _, op := range ops {
		if op.F == "load-table" && op.Phase == "workload" && op.Type == history.Info {
			failedGuards++
		}
		if failedGuards > 0 && op.F == "commit-table" && op.Type == history.OK {
			okAfter++
		}
	}
	if failedGuards == 0 || okAfter == 0 {
		t.Fatalf("want failed guard reads retried and commits acknowledged after them: %d failed, %d ok after", failedGuards, okAfter)
	}
	code, err := checkRun(dir, &bytes.Buffer{})
	if err != nil || code != map[checker.Verdict]int{checker.Valid: 0, checker.Inconclusive: 4}[camp.verdict] {
		t.Fatalf("offline check: exit %d, %v; live verdict %s", code, err, camp.verdict)
	}
}

func TestSnapshotCampaignStillCatchesPlantedBugs(t *testing.T) {
	// Partial publication stores the snapshot without moving main. (An ignored
	// guard cannot fire through this workload: contenders share the next
	// sequence number, so the fake rejects the second one regardless.)
	_, camp, dir, err := runSnapshotCampaign(t, fakecatalog.Bugs{PartialSnapshots: true}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Invalid {
		t.Fatalf("partial snapshot publication was not caught under faults: %s", camp.verdict)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}
