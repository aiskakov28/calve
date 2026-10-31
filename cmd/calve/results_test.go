package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestResultsRecheckEverySweepHistory(t *testing.T) {
	dir := t.TempDir()
	save := func(rel string, ops []history.Op) {
		t.Helper()
		path := filepath.Join(dir, rel)
		os.MkdirAll(filepath.Dir(path), 0o755)
		if err := history.SaveFile(path, ops); err != nil {
			t.Fatal(err)
		}
	}
	schemaRun := func(bugs fakecatalog.Bugs) []history.Op {
		fake := fakecatalog.New(bugs)
		defer fake.Close()
		rec := history.NewRecorder()
		c := rest.NewClient(fake.Server.URL, "", "", time.Second)
		if _, err := workload.RunSchema(context.Background(), c, workload.SchemaConfig{Namespace: "test", Clients: 3, Rounds: 3}, rec); err != nil {
			t.Fatal(err)
		}
		return rec.Ops()
	}
	maintenanceRun := func(bugs fakecatalog.Bugs) []history.Op {
		fake := fakecatalog.New(bugs)
		defer fake.Close()
		rec := history.NewRecorder()
		c := rest.NewClient(fake.Server.URL, "", "", time.Second)
		cfg := workload.MaintenanceConfig{Namespace: "test", Appends: 20, ExpireInterval: time.Millisecond, ManifestRoot: t.TempDir()}
		accept := snapshotWriterFunc(func(context.Context, string, []byte) error { return nil })
		if _, err := workload.RunMaintenance(context.Background(), c, cfg, accept, rec); err != nil {
			t.Fatal(err)
		}
		return rec.Ops()
	}
	schema := schemaRun(fakecatalog.Bugs{})
	save("fake/schema/1.jsonl", schema)
	save("fake/schema/2.jsonl", schema)
	save("fake/maintenance/1.jsonl", maintenanceRun(fakecatalog.Bugs{ExpireRemovesHead: true}))
	// A history filed under the wrong workload is an error, never a verdict.
	save("fake/atomic/1.jsonl", schemaRun(fakecatalog.Bugs{}))
	// A run that saved no history still counts.
	os.WriteFile(filepath.Join(dir, "fake/atomic/2.missing"), nil, 0o644)
	os.WriteFile(filepath.Join(dir, "catalogs.tsv"), []byte("fake\tfake/catalog:1.0\n"), 0o644)

	rows, err := sweepResults(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]resultRow{}
	for _, row := range rows {
		got[row.Workload] = row
	}
	// Each run's commit requests count; setup and reads do not.
	attempts := 0
	for _, op := range schema {
		if op.Type == history.Invoke && op.F == "commit-table" {
			attempts++
		}
	}
	if r := got["schema"]; r.Runs != 2 || r.Valid != 2 || r.Errors != 0 || r.Version != "fake/catalog:1.0" || attempts == 0 || r.Commits != 2*attempts {
		t.Fatalf("schema row: %+v", r)
	}
	if r := got["maintenance"]; r.Runs != 1 || r.Invalid != 1 || r.Anomalies["dangling-main"] == 0 || r.Anomalies["lost-snapshot"] == 0 {
		t.Fatalf("maintenance row: %+v", r)
	}
	// A history that could not be checked adds no commits to the rate.
	if r := got["atomic"]; r.Runs != 2 || r.Errors != 2 || r.Valid+r.Invalid+r.Inconclusive != 0 || r.Commits != 0 {
		t.Fatalf("atomic row: %+v", r)
	}
	var table bytes.Buffer
	writeResultsTable(&table, rows)
	if !strings.Contains(table.String(), fmt.Sprintf("| fake | fake/catalog:1.0 | maintenance | none | 1 | 0 | 1 | 0 | 0 | %d |", got["maintenance"].Commits)) {
		t.Fatalf("table:\n%s", table.String())
	}
}

// A fault sweep stores whole run directories, one level below their fault
// kind. Each is checked as calve check does, so coverage counts too.
func TestResultsCheckCampaignRunDirectories(t *testing.T) {
	dir := t.TempDir()
	run := func(rel string, bugs fakecatalog.Bugs, mode int32) {
		t.Helper()
		camp, client, cfg, _ := testCampaign(t, bugs, mode, nil)
		out := filepath.Join(dir, rel)
		if _, _, err := executeAppendCampaign(context.Background(), client, cfg, readbackConfig{}, camp, out, "reference"); err != nil {
			t.Fatal(err)
		}
	}
	run("fake/append/pause/1", fakecatalog.Bugs{}, slow)
	run("fake/append/pause/2", fakecatalog.Bugs{LoseEvery: 7}, slow)
	// The same history filed under a different workload is an error.
	run("fake/schema/pause/1", fakecatalog.Bugs{}, slow)
	os.WriteFile(filepath.Join(dir, "fake/append/pause/3.missing"), nil, 0o644)
	os.WriteFile(filepath.Join(dir, "fake/append/pause/1.log"), []byte("log"), 0o644)
	// Baseline histories sit beside campaign runs in the same table.
	os.MkdirAll(filepath.Join(dir, "fake/append"), 0o755)
	baseline, err := os.ReadFile(filepath.Join(dir, "fake/append/pause/1", historyFile))
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "fake/append/1.jsonl"), baseline, 0o644)

	rows, err := sweepResults(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]resultRow{}
	for _, row := range rows {
		got[row.Workload+"/"+row.Fault] = row
	}
	if r := got["append/pause"]; r.Runs != 3 || r.Valid != 1 || r.Invalid != 1 || r.Errors != 1 || r.Anomalies["lost"] == 0 || r.Commits == 0 {
		t.Fatalf("append under pause: %+v", r)
	}
	if r := got["schema/pause"]; r.Runs != 1 || r.Errors != 1 {
		t.Fatalf("a run filed under the wrong workload was not an error: %+v", r)
	}
	if r := got["append/none"]; r.Runs != 1 || r.Errors+r.Valid+r.Invalid+r.Inconclusive != 1 {
		t.Fatalf("baseline row beside campaign runs: %+v", r)
	}
	var table bytes.Buffer
	writeResultsTable(&table, rows)
	if !strings.Contains(table.String(), "| append | pause | 3 | 1 | 1 | 0 | 1 |") {
		t.Fatalf("table:\n%s", table.String())
	}
}

// After a search timeout the saved result may differ from this check's. The
// row must describe this check: its verdict and its anomalies, never the
// saved ones.
func TestResultsUseTheRecomputedAnomalies(t *testing.T) {
	_, camp, run, err := runSnapshotCampaign(t, fakecatalog.Bugs{}, slow)
	if err != nil || camp.verdict != checker.Valid {
		t.Fatalf("want a valid run: %s, %v", camp.verdict, err)
	}
	dir := t.TempDir()
	copied := filepath.Join(dir, "fake/snapshot/latency/1")
	if err := os.CopyFS(copied, os.DirFS(run)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(copied, resultFile))
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	json.Unmarshal(data, &result)
	inner := result["result"].(map[string]any)
	// As saved by a run whose search ran out of time, with a finding this
	// check does not establish.
	inner["search"], inner["verdict"] = "Unknown", "inconclusive"
	inner["reasons"] = []any{"Porcupine search timed out"}
	inner["anomalies"] = []any{map[string]any{"kind": "non-linearizable"}}
	result["verdict"] = "inconclusive"
	encoded, _ := json.Marshal(result)
	os.WriteFile(filepath.Join(copied, resultFile), encoded, 0o600)

	rows, err := sweepResults(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Valid != 1 || len(rows[0].Anomalies) != 0 {
		t.Fatalf("want this check's valid verdict with no saved anomalies: %+v", rows)
	}
}

// The rate is every anomaly per 1,000 commit attempts; a row with no checked
// commit has none rather than a division by zero.
func TestResultsTableRate(t *testing.T) {
	var table bytes.Buffer
	writeResultsTable(&table, []resultRow{
		{Catalog: "fake", Workload: "skew", Fault: "none", Runs: 1, Invalid: 1, Commits: 2000, Anomalies: map[string]int{"write-skew": 2, "stale-requirement": 1}},
		{Catalog: "fake", Workload: "atomic", Fault: "none", Runs: 1, Errors: 1, Anomalies: map[string]int{}},
	})
	for _, want := range []string{
		"| fake |  | skew | none | 1 | 0 | 1 | 0 | 0 | 2000 | 1.5 | stale-requirement ×1, write-skew ×2 |",
		"| fake |  | atomic | none | 1 | 0 | 0 | 0 | 1 | 0 |  |  |",
	} {
		if !strings.Contains(table.String(), want) {
			t.Fatalf("want %q in:\n%s", want, table.String())
		}
	}
}
