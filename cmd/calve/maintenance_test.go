package main

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestMaintenanceReplayMatchesLiveVerdict(t *testing.T) {
	accept := snapshotWriterFunc(func(context.Context, string, []byte) error { return nil })
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
	}{
		{"correct", fakecatalog.Bugs{}},
		{"expire removes head", fakecatalog.Bugs{ExpireRemovesHead: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			out := filepath.Join(t.TempDir(), "history-maintenance.jsonl")
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			cfg := workload.MaintenanceConfig{Namespace: "test", Appends: 20, ExpireInterval: time.Millisecond, ManifestRoot: t.TempDir()}
			res, err := executeMaintenance(context.Background(), c, cfg, accept, out, readbackConfig{})
			if err != nil {
				t.Fatal(err)
			}
			live := reportMaintenance(io.Discard, res)
			if tt.bugs.ExpireRemovesHead && live != 3 {
				t.Fatalf("planted bug not reported: exit %d", live)
			}
			fake.Close()
			replayed, err := checkArtifact(out, io.Discard)
			if err != nil || replayed != live {
				t.Fatalf("replay exit %d, error %v; live exit %d", replayed, err, live)
			}
		})
	}
}
