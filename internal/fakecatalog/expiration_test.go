package fakecatalog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/rest"
)

func TestExpirationRechecksTableAfterHook(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
	}{
		{"single-table commit", 200},
		{"multi-table transaction", 200},
		{"drop", 404},
		{"recreate", 409},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := rest.NewClient(fake.Server.URL, "", "", 2*time.Second)
			if r := client.CreateNamespace(ctx, "test"); r.Outcome() != "ok" {
				t.Fatal(rest.Describe(r))
			}
			create := func(name string) string {
				t.Helper()
				meta, err := rest.ParseMetadata(client.CreateTable(ctx, "test", name,
					rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Type: "long"}}}))
				if err != nil {
					t.Fatal(err)
				}
				return meta.TableUUID
			}
			guard := func(uuid string) []map[string]any {
				return []map[string]any{{"type": "assert-table-uuid", "uuid": uuid}}
			}
			seedSnapshots := func(uuid string) {
				t.Helper()
				for id := int64(1); id <= 2; id++ {
					var parent any
					if id > 1 {
						parent = id - 1
					}
					snapshot := map[string]any{"snapshot-id": id, "sequence-number": id, "timestamp-ms": 1,
						"manifest-list": fmt.Sprintf("file:/test/%d.avro", id), "schema-id": 0,
						"summary": map[string]string{"operation": "append"}}
					if parent != nil {
						snapshot["parent-snapshot-id"] = parent
					}
					r := client.CommitTable(ctx, "test", "a", rest.TableChange{
						Requirements: append(guard(uuid), map[string]any{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent}),
						Updates: []map[string]any{{"action": "add-snapshot", "snapshot": snapshot},
							{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id}},
					})
					if r.Outcome() != "ok" {
						t.Fatal(rest.Describe(r))
					}
				}
			}
			uuid, other := create("a"), create("b")
			seedSnapshots(uuid)
			entered, release := make(chan struct{}), make(chan struct{})
			fake.SetBeforeExpiration(func(call context.Context) {
				close(entered)
				select {
				case <-release:
				case <-call.Done():
				}
			})
			done := make(chan rest.Response, 1)
			go func() {
				done <- client.CommitTable(ctx, "test", "a", rest.TableChange{Requirements: guard(uuid),
					Updates: []map[string]any{{"action": "remove-snapshots", "snapshot-ids": []int64{1}}}})
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			properties := []map[string]any{{"action": "set-properties", "updates": map[string]string{"keep": "1"}}}
			switch tt.name {
			case "single-table commit":
				if r := client.CommitTable(ctx, "test", "a", rest.TableChange{Requirements: guard(uuid), Updates: properties}); r.Outcome() != "ok" {
					t.Fatal(rest.Describe(r))
				}
			case "multi-table transaction":
				r := client.CommitTransaction(ctx, []rest.TableChange{
					{Identifier: &rest.Identifier{Namespace: []string{"test"}, Name: "a"}, Requirements: guard(uuid), Updates: properties},
					{Identifier: &rest.Identifier{Namespace: []string{"test"}, Name: "b"}, Requirements: guard(other), Updates: properties},
				})
				if r.Outcome() != "ok" {
					t.Fatal(rest.Describe(r))
				}
			case "drop", "recreate":
				if r := client.DropTable(ctx, "test", "a"); r.Outcome() != "ok" {
					t.Fatal(rest.Describe(r))
				}
				if tt.name == "recreate" {
					seedSnapshots(create("a"))
				}
			}
			close(release)
			select {
			case r := <-done:
				if r.Err != nil || r.Status != tt.status {
					t.Fatalf("want expiration HTTP %d: %s", tt.status, rest.Describe(r))
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			response := client.LoadTable(ctx, "test", "a")
			if tt.name == "drop" {
				if response.Status != 404 {
					t.Fatal("expiration resurrected a dropped table", rest.Describe(response))
				}
				return
			}
			meta, err := rest.ParseMetadata(response)
			if err != nil {
				t.Fatal(err)
			}
			ids := map[float64]bool{}
			for _, snapshot := range meta.Snapshots {
				ids[snapshot["snapshot-id"].(float64)] = true
			}
			if !ids[2] || meta.CurrentSnapshotID == nil || *meta.CurrentSnapshotID != 2 {
				t.Fatal("expiration changed the current snapshot", meta)
			}
			if tt.name == "recreate" {
				if meta.TableUUID == uuid || !ids[1] || len(ids) != 2 {
					t.Fatal("expiration changed the replacement table", meta)
				}
			} else {
				if meta.TableUUID != uuid || ids[1] || len(ids) != 1 || meta.Properties["keep"] != "1" {
					t.Fatal("expiration must remove snapshot 1 and retain the concurrent property", meta)
				}
			}
		})
	}
}
