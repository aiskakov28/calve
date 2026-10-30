package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	save("fake/schema/1.jsonl", schemaRun(fakecatalog.Bugs{}))
	save("fake/schema/2.jsonl", schemaRun(fakecatalog.Bugs{}))
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
	if r := got["schema"]; r.Runs != 2 || r.Valid != 2 || r.Errors != 0 || r.Version != "fake/catalog:1.0" {
		t.Fatalf("schema row: %+v", r)
	}
	if r := got["maintenance"]; r.Runs != 1 || r.Invalid != 1 || r.Anomalies["dangling-main"] == 0 || r.Anomalies["lost-snapshot"] == 0 {
		t.Fatalf("maintenance row: %+v", r)
	}
	if r := got["atomic"]; r.Runs != 2 || r.Errors != 2 || r.Valid+r.Invalid+r.Inconclusive != 0 {
		t.Fatalf("atomic row: %+v", r)
	}
	var table bytes.Buffer
	writeResultsTable(&table, rows)
	if !strings.Contains(table.String(), "| fake | fake/catalog:1.0 | maintenance | none | 1 | 0 | 1 | 0 | 0 | dangling-main ×") {
		t.Fatalf("table:\n%s", table.String())
	}
}
