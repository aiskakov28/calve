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

func TestSchemaReplayMatchesLiveVerdict(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		want int
	}{
		{"correct", fakecatalog.Bugs{}, 0},
		{"ignored schema guard", fakecatalog.Bugs{IgnoreSchemaRequirements: true}, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			out := filepath.Join(t.TempDir(), "history-schema.jsonl")
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			res, err := executeSchema(context.Background(), c, workload.SchemaConfig{Namespace: "test", Clients: 3, Rounds: 3}, out, readbackConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if live := reportSchema(io.Discard, res); live != tt.want {
				t.Fatalf("live exit %d, want %d", live, tt.want)
			}
			fake.Close()
			replayed, err := checkArtifact(out, io.Discard)
			if err != nil || replayed != tt.want {
				t.Fatalf("replay exit %d, error %v; want %d", replayed, err, tt.want)
			}
		})
	}
}
