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

func TestNamespaceReplayMatchesLiveVerdict(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		want int
	}{
		{"correct", fakecatalog.Bugs{}, 0},
		{"ignored existence check", fakecatalog.Bugs{IgnoreNamespaceExists: true}, 3},
		{"500 for an existing namespace", fakecatalog.Bugs{ServerErrorForExistingNamespace: true}, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			out := filepath.Join(t.TempDir(), "history-namespace.jsonl")
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			res, err := executeNamespace(context.Background(), c, workload.NamespaceConfig{Parent: "test", Clients: 3, Rounds: 3}, out)
			if err != nil {
				t.Fatal(err)
			}
			if live := reportNamespace(io.Discard, res); live != tt.want {
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
