package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/workload"
)

func runSkewCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32, spare int) (checker.SkewResult, *campaign, string, error) {
	t.Helper()
	camp, client, _, dir := testCampaign(t, bugs, mode, nil)
	interval := 20 * time.Millisecond
	cfg := workload.SkewConfig{Namespace: "test", Rounds: int(camp.plan.Config.RunDuration/interval) + spare, Interval: interval}
	res, err := executeSkewCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference", "run_test")
	return res, camp, dir, err
}

func TestSkewCampaignCorrectCatalogUnderLatency(t *testing.T) {
	res, camp, dir, err := runSkewCampaign(t, fakecatalog.Bugs{}, slow, 0)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Valid || res.OK < 2 {
		t.Fatalf("want a valid run with acknowledged rounds: %s, %d ok", camp.verdict, res.OK)
	}
	// Both controls ran after healing and their stale transactions were rejected.
	for _, r := range res.Rounds[len(res.Rounds)-2:] {
		if r.Mode == "concurrent" || r.Transactions[0].CommitState == r.Transactions[1].CommitState {
			t.Fatalf("control %s was not exercised: %+v", r.Round, r.Transactions)
		}
	}
	for _, c := range camp.coverage {
		if c.Commits == 0 {
			t.Fatalf("fault %s met no transaction although rounds ran to the horizon", c.Event)
		}
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 0 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

func TestSkewCampaignSetsUpEveryRoundBeforeThePlan(t *testing.T) {
	// More rounds than fit before the horizon.
	res, camp, dir, err := runSkewCampaign(t, fakecatalog.Bugs{}, slow, 20)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := readOps(filepath.Join(dir, historyFile))
	if err != nil {
		t.Fatal(err)
	}
	// The plan starts after the last setup call: no table is created or
	// baselined during the load phase.
	lastSetup, firstTxn := int64(-1), int64(-1)
	for _, op := range ops {
		if op.Type == history.Invoke && (op.Phase == "setup" || op.Phase == "baseline") {
			lastSetup = op.Time
		}
		if op.Type == history.Invoke && op.F == "skew-txn" && firstTxn < 0 {
			firstTxn = op.Time
		}
	}
	if lastSetup < 0 || firstTxn <= lastSetup {
		t.Fatalf("a table was set up after the first transaction: setup ends %d, first transaction %d", lastSetup, firstTxn)
	}
	if res.NotStarted == 0 || len(res.Rounds)+res.NotStarted != int(camp.plan.Config.RunDuration/(20*time.Millisecond))+20+2 {
		t.Fatalf("want rounds left at the horizon counted as not started: %d checked, %d not started", len(res.Rounds), res.NotStarted)
	}
}

func TestSkewCampaignCorrectCatalogUnderFailures(t *testing.T) {
	res, camp, dir, err := runSkewCampaign(t, fakecatalog.Bugs{}, down, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Anomalies) != 0 || camp.verdict == checker.Invalid || res.Unknown == 0 {
		t.Fatalf("want unknown transactions and no findings behind failing requests: %d unknown, %v", res.Unknown, res.Anomalies)
	}
	code, err := checkRun(dir, &bytes.Buffer{})
	if err != nil || code != map[checker.Verdict]int{checker.Valid: 0, checker.Inconclusive: 4}[camp.verdict] {
		t.Fatalf("offline check: exit %d, %v; live verdict %s", code, err, camp.verdict)
	}
}

func TestSkewCampaignStillCatchesPlantedBugs(t *testing.T) {
	// Ignored read-only guards let both transactions of a concurrent pair commit,
	// and let a control's stale transaction commit after healing.
	res, camp, dir, err := runSkewCampaign(t, fakecatalog.Bugs{IgnoreReadOnlyRequirements: true}, slow, 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, a := range res.Anomalies {
		kinds[a.Kind]++
	}
	if camp.verdict != checker.Invalid || kinds["write-skew"] == 0 || kinds["stale-requirement"] != 2 {
		t.Fatalf("ignored guards were not caught under faults: %s %v", camp.verdict, kinds)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

// Only rounds the horizon cut off may be missing: a gap before a round that ran
// means the history is incomplete, not that the round was never started.
func TestSkewCampaignRejectsAGapBeforeAStartedRound(t *testing.T) {
	res, _, dir, err := runSkewCampaign(t, fakecatalog.Bugs{}, slow, 0)
	if err != nil || len(res.Rounds) < 4 {
		t.Fatalf("want a run with several rounds: %d, %v", len(res.Rounds), err)
	}
	ops, err := readOps(filepath.Join(dir, historyFile))
	if err != nil {
		t.Fatal(err)
	}
	var gapped []history.Op
	for _, op := range ops {
		if op.Round == res.Rounds[0].Round && op.Type != history.Meta && op.Phase != "setup" && op.Phase != "baseline" {
			continue
		}
		gapped = append(gapped, op)
	}
	if _, err := checker.CheckSkew(gapped); err == nil || !strings.Contains(err.Error(), "started after a round that was not started") {
		t.Fatalf("a history missing an early round's transactions was not rejected for the gap: %v", err)
	}
}
