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

func runMaintenanceCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32) (checker.MaintenanceResult, *campaign, string, error) {
	t.Helper()
	camp, client, _, dir := testCampaign(t, bugs, mode, nil)
	cfg := workload.MaintenanceConfig{Namespace: "test", Appends: int(camp.plan.Config.RunDuration / time.Millisecond),
		ExpireInterval: time.Millisecond, ManifestRoot: t.TempDir()}
	accept := snapshotWriterFunc(func(context.Context, string, []byte) error { return nil })
	res, err := executeMaintenanceCampaign(context.Background(), client, cfg, accept, readbackConfig{}, camp, dir, "reference", "run_test")
	return res, camp, dir, err
}

func TestMaintenanceCampaignCorrectCatalogUnderLatency(t *testing.T) {
	res, camp, dir, err := runMaintenanceCampaign(t, fakecatalog.Bugs{}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Valid || len(res.Anomalies) != 0 || res.AppendsOK < 2 || res.RacingExpirations == 0 {
		t.Fatalf("want a valid run with acknowledged appends and racing expirations: %s %+v", camp.verdict, res)
	}
	for _, c := range camp.coverage {
		if c.Commits == 0 {
			t.Fatalf("fault %s met no commit although the load ran to the horizon", c.Event)
		}
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 0 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

func TestMaintenanceCampaignRetriesReloadsUnderFaults(t *testing.T) {
	res, camp, dir, err := runMaintenanceCampaign(t, fakecatalog.Bugs{}, down)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Anomalies) != 0 || camp.verdict == checker.Invalid {
		t.Fatalf("a correct catalog behind failing requests produced findings: %+v", res.Anomalies)
	}
	ops, err := readOps(filepath.Join(dir, historyFile))
	if err != nil {
		t.Fatal(err)
	}
	failedReloads, okAfter := 0, 0
	for _, op := range ops {
		if op.F == "load-table" && op.Process == 0 && op.Type == history.Info {
			failedReloads++
		}
		if failedReloads > 0 && op.F == "commit-table" && op.Process == 0 && op.Type == history.OK {
			okAfter++
		}
	}
	if failedReloads == 0 || okAfter == 0 {
		t.Fatalf("want failed reloads retried and appends acknowledged after them: %d failed, %d ok after", failedReloads, okAfter)
	}
	code, err := checkRun(dir, &bytes.Buffer{})
	if err != nil || code != map[checker.Verdict]int{checker.Valid: 0, checker.Inconclusive: 4}[camp.verdict] {
		t.Fatalf("offline check: exit %d, %v; live verdict %s", code, err, camp.verdict)
	}
}

func TestMaintenanceCampaignStillCatchesPlantedBugs(t *testing.T) {
	res, camp, dir, err := runMaintenanceCampaign(t, fakecatalog.Bugs{ExpireRemovesHead: true}, slow)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, a := range res.Anomalies {
		kinds[a.Kind] = true
	}
	if camp.verdict != checker.Invalid || !kinds["dangling-main"] || !kinds["lost-snapshot"] {
		t.Fatalf("an expiration that removes the head was not caught under faults: %s %+v", camp.verdict, res.Anomalies)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}
