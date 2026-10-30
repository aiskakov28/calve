package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/workload"
)

func runSchemaCampaign(t *testing.T, bugs fakecatalog.Bugs, mode int32) (checker.SchemaResult, *campaign, string, error) {
	t.Helper()
	camp, client, _, dir := testCampaign(t, bugs, mode, nil)
	cfg := workload.SchemaConfig{Namespace: "test", Clients: 3, Rounds: int(camp.plan.Config.RunDuration / time.Millisecond)}
	res, err := executeSchemaCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference", "run_test")
	return res, camp, dir, err
}

func TestSchemaCampaignCorrectCatalogUnderLatency(t *testing.T) {
	res, camp, dir, err := runSchemaCampaign(t, fakecatalog.Bugs{}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Valid || len(res.Anomalies) != 0 || res.Rounds < 2 || res.RoundsWithWinner != res.Rounds {
		t.Fatalf("want a valid run with a winner in every round: %s %+v", camp.verdict, res)
	}
	if len(camp.coverage) != len(camp.plan.Events) {
		t.Fatalf("want coverage for each of %d faults: %+v", len(camp.plan.Events), camp.coverage)
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

func TestSchemaCampaignRetriesGuardReadsUnderFaults(t *testing.T) {
	res, camp, dir, err := runSchemaCampaign(t, fakecatalog.Bugs{}, down)
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
	failedGuards, laterRounds := 0, 0
	for _, op := range ops {
		if op.F == "load-table" && op.Phase == "workload" && op.Type == history.Info {
			failedGuards++
		}
		if op.F == "commit-table" && op.Type == history.OK {
			laterRounds++
		}
	}
	if failedGuards == 0 || laterRounds < 2 {
		t.Fatalf("want failed guard reads retried and acknowledged rounds around the faults: %d failed, %d ok", failedGuards, laterRounds)
	}
	code, err := checkRun(dir, &bytes.Buffer{})
	if err != nil || code != map[checker.Verdict]int{checker.Valid: 0, checker.Inconclusive: 4}[camp.verdict] {
		t.Fatalf("offline check: exit %d, %v; live verdict %s", code, err, camp.verdict)
	}
}

func TestSchemaCampaignStillCatchesPlantedBugs(t *testing.T) {
	res, camp, dir, err := runSchemaCampaign(t, fakecatalog.Bugs{IgnoreSchemaRequirements: true}, slow)
	if err != nil {
		t.Fatal(err)
	}
	if camp.verdict != checker.Invalid || res.Anomalies[0].Kind != "multiple-winners" {
		t.Fatalf("an ignored schema guard was not caught under faults: %s %+v", camp.verdict, res.Anomalies)
	}
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 3 {
		t.Fatalf("offline check: exit %d, %v", code, err)
	}
}

func TestSchemaCampaignSetupFailureEndsWithoutHanging(t *testing.T) {
	camp, client, _, dir := testCampaign(t, fakecatalog.Bugs{}, down, nil)
	// The catalog is down before the plan starts, so setup fails in prepare.
	client.BaseURL = "http://127.0.0.1:1"
	cfg := workload.SchemaConfig{Namespace: "test", Clients: 2, Rounds: 1000}
	done := make(chan error, 1)
	go func() {
		_, err := executeSchemaCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference", "run_test")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a campaign whose setup failed returned no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a campaign whose setup failed did not return")
	}
	data, err := os.ReadFile(filepath.Join(dir, resultFile))
	if err != nil {
		t.Fatal(err)
	}
	var saved runResult
	if json.Unmarshal(data, &saved) != nil || saved.Verdict != "error" {
		t.Fatalf("result.json: %s", data)
	}
}

func TestLoadCoverageCountsCommitsDuringEachFault(t *testing.T) {
	action := func(event, kind, act string) json.RawMessage {
		data, _ := json.Marshal(fault.ActionRecord{Stage: "execution", Action: act,
			Injection: fault.Injection{Event: fault.Event{ID: event, Kind: fault.Kind(kind)}}})
		return data
	}
	// Fault f1's window is 90 to 200, f2's 290 to 400: from the start of the
	// apply to the start of the heal.
	faults := []history.Op{
		{ID: 1, Type: history.Invoke, F: "fault", Time: 90},
		{ID: 1, Type: history.OK, F: "fault", Time: 100, Value: action("f1", "pause", "apply")},
		{ID: 2, Type: history.Invoke, F: "fault", Time: 200},
		{ID: 2, Type: history.OK, F: "fault", Time: 210, Value: action("f1", "pause", "heal")},
		{ID: 3, Type: history.Invoke, F: "fault", Time: 290},
		{ID: 3, Type: history.OK, F: "fault", Time: 300, Value: action("f2", "pause", "apply")},
		{ID: 4, Type: history.Invoke, F: "fault", Time: 400},
		{ID: 4, Type: history.OK, F: "fault", Time: 410, Value: action("f2", "pause", "heal")},
	}
	ops := []history.Op{
		// A commit spanning f1, a read during f2 (not a commit), and a commit
		// that ends just before f2's apply starts.
		{ID: 1, Type: history.Invoke, Phase: "workload", F: "commit-table", Time: 50},
		{ID: 1, Type: history.OK, Phase: "workload", F: "commit-table", Time: 150},
		{ID: 2, Type: history.Invoke, Phase: "workload", F: "load-table", Time: 320},
		{ID: 2, Type: history.OK, Phase: "workload", F: "load-table", Time: 330},
		{ID: 3, Type: history.Invoke, Phase: "workload", F: "commit-table", Time: 250},
		{ID: 3, Type: history.OK, Phase: "workload", F: "commit-table", Time: 285},
	}
	got := loadCoverage(faults, ops)
	if len(got) != 2 || got[0] != (faultCoverage{"f1", "pause", 1}) || got[1] != (faultCoverage{"f2", "pause", 0}) {
		t.Fatalf("coverage: %+v", got)
	}
	if campaignVerdict(checker.Valid, got) != checker.Inconclusive {
		t.Fatal("a fault that met no commit left the run valid")
	}
	if campaignVerdict(checker.Invalid, got) != checker.Invalid {
		t.Fatal("missing coverage hid a finding")
	}
	// A commit with no completion stays in flight to the end.
	ops = append(ops, history.Op{ID: 4, Type: history.Invoke, Phase: "workload", F: "commit-table", Time: 260})
	if got := loadCoverage(faults, ops); got[1].Commits != 1 {
		t.Fatalf("an unfinished commit did not cover f2: %+v", got)
	}
}

// A commit in flight when a kill or stop starts counts, although it ends before
// the apply is confirmed.
func TestLoadCoverageCountsCommitsCaughtByTheApply(t *testing.T) {
	data, _ := json.Marshal(fault.ActionRecord{Stage: "execution", Action: "apply",
		Injection: fault.Injection{Event: fault.Event{ID: "f1", Kind: fault.Restart}}})
	faults := []history.Op{
		{ID: 1, Type: history.Invoke, F: "fault", Time: 100},
		{ID: 1, Type: history.OK, F: "fault", Time: 200, Value: data},
	}
	ops := []history.Op{
		{ID: 1, Type: history.Invoke, Phase: "workload", F: "commit-table", Time: 90},
		{ID: 1, Type: history.Info, Phase: "workload", F: "commit-table", Time: 150},
	}
	if got := loadCoverage(faults, ops); len(got) != 1 || got[0].Commits != 1 {
		t.Fatalf("a commit cut off by the stop was not counted: %+v", got)
	}
	// calve-run-v3 opened the window at the apply's completion and missed it.
	if got := coverageFrom(faults, ops, false); got[0].Commits != 0 {
		t.Fatalf("the calve-run-v3 window changed: %+v", got)
	}
}
