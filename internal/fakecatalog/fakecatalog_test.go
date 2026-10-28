package fakecatalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/rest"
)

func TestTransactionStagesAllTables(t *testing.T) {
	for _, scenario := range []string{"commit", "stale-read-only", "invalid-update", "unknown-requirement"} {
		t.Run(scenario, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			ctx := context.Background()
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			c.CreateNamespace(ctx, "test")
			schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Type: "long"}}}
			var changes []rest.TableChange
			for _, name := range []string{"a", "b"} {
				if r := c.CreateTable(ctx, "test", name, schema); r.Outcome() != "ok" {
					t.Fatal(rest.Describe(r))
				}
				changes = append(changes, rest.TableChange{
					Identifier:   &rest.Identifier{Namespace: []string{"test"}, Name: name},
					Requirements: []map[string]any{{"type": "assert-current-schema-id", "current-schema-id": 0}},
					Updates:      []map[string]any{{"action": "set-properties", "updates": map[string]string{"test": "committed"}}},
				})
			}
			want := 204
			switch scenario {
			case "stale-read-only":
				want = 409
				changes[1].Requirements[0]["current-schema-id"] = 99
				changes[1].Updates = []map[string]any{}
			case "invalid-update":
				want = 400
				changes[1].Updates[0]["action"] = "not-supported"
			case "unknown-requirement":
				want = 400
				changes[1].Requirements[0]["type"] = "not-supported"
			}
			if r := c.CommitTransaction(ctx, changes); r.Status != want {
				t.Fatalf("want %d, got %s", want, rest.Describe(r))
			}
			for _, name := range []string{"a", "b"} {
				meta, err := rest.ParseMetadata(c.LoadTable(ctx, "test", name))
				if err != nil {
					t.Fatal(err)
				}
				if (meta.Properties["test"] == "committed") != (want == 204) {
					t.Fatal("transaction published only part of its changes")
				}
			}
		})
	}
}
