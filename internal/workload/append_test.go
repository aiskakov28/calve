package workload

import (
	"context"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

func runAgainst(t *testing.T, bugs fakecatalog.Bugs) checker.AppendResult {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()

	ctx := context.Background()
	c := rest.NewClient(fake.Server.URL, "", "", 5*time.Second)
	if _, err := c.Config(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := AppendConfig{Namespace: "calve", Table: "append_test", Clients: 8, PerClient: 25}
	if err := SetupTable(ctx, c, cfg.Namespace, cfg.Table); err != nil {
		t.Fatal(err)
	}
	rec := history.NewRecorder()
	RunAppend(ctx, c, cfg, rec)

	final, err := FinalAppendKeys(ctx, c, cfg.Namespace, cfg.Table)
	if err != nil {
		t.Fatal(err)
	}
	res, err := checker.CheckAppend(rec.Ops(), final)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestAppendCorrectCatalogPasses(t *testing.T) {
	res := runAgainst(t, fakecatalog.Bugs{})
	if !res.Valid {
		t.Fatalf("correct catalog flagged: %+v", res.Anomalies)
	}
	if res.OK != 200 {
		t.Fatalf("want 200 acknowledged appends, got %d", res.OK)
	}
}

func TestAppendCatchesLostCommits(t *testing.T) {
	res := runAgainst(t, fakecatalog.Bugs{LoseEvery: 7})
	if res.Valid {
		t.Fatal("planted lost commits were not caught")
	}
	lost := 0
	for _, a := range res.Anomalies {
		if a.Kind == "lost" {
			lost++
		}
	}
	// 200 commits, every 7th dropped.
	if lost != 200/7 {
		t.Fatalf("want %d lost, got %d", 200/7, lost)
	}
}
