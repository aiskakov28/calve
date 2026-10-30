package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/workload"
)

func runAtomicCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32) (checker.AtomicResult, *campaign, string, error) {
	t.Helper()
	camp, client, _, dir := testCampaign(t, bugs, mode, nil)
	cfg := workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 3, PerClient: int(camp.plan.Config.RunDuration / time.Microsecond),
		Readers: 1, ReadInterval: time.Millisecond}
	res, err := executeAtomicCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference", "run_test")
	return res, camp, dir, err
}

func TestAtomicCampaignCorrectCatalogUnderLatency(t *testing.T) {
	res, camp, dir, err := runAtomicCampaign(t, fakecatalog.Bugs{}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Valid || res.OK < 2 || res.ConcurrentReads == 0 {
		t.Fatalf("want a valid run with acknowledged transactions and concurrent reads: %s %+v", camp.verdict, res)
	}
	for _, c := range camp.coverage {
		if c.Commits == 0 {
			t.Fatalf("fault %s met no transaction although clients ran to the horizon", c.Event)
		}
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 0 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

func TestAtomicCampaignCorrectCatalogUnderFailures(t *testing.T) {
	res, camp, dir, err := runAtomicCampaign(t, fakecatalog.Bugs{}, down)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Anomalies) != 0 || camp.verdict == checker.Invalid {
		t.Fatalf("a correct catalog behind failing requests produced findings: %+v", res.Anomalies)
	}
	code, err := checkRun(dir, &bytes.Buffer{})
	if err != nil || code != map[checker.Verdict]int{checker.Valid: 0, checker.Inconclusive: 4}[camp.verdict] {
		t.Fatalf("offline check: exit %d, %v; live verdict %s", code, err, camp.verdict)
	}
}

func TestAtomicCampaignStillCatchesPlantedBugs(t *testing.T) {
	_, camp, dir, err := runAtomicCampaign(t, fakecatalog.Bugs{PartialTransactions: true}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Invalid {
		t.Fatalf("partial transactions were not caught under faults: %s", camp.verdict)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}
