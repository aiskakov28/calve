// Package workload generates concurrent operations against a catalog and
// records them in a history.
package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// AppendKeyPrefix marks the table properties the append workload owns.
const AppendKeyPrefix = "calve.append."

// AppendConfig sizes an append run.
type AppendConfig struct {
	Namespace string
	Table     string
	Clients   int
	PerClient int
}

// SetupTable creates the namespace (ignoring "already exists") and a fresh
// table for the run, dropping any table left over from an earlier run.
func SetupTable(ctx context.Context, c *rest.Client, namespace, table string) error {
	if r := c.CreateNamespace(ctx, namespace); r.Outcome() != "ok" && r.Status != 409 {
		return fmt.Errorf("create namespace: %s", rest.Describe(r))
	}
	c.DropTable(ctx, namespace, table)
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Required: true, Type: "long"}}}
	if r := c.CreateTable(ctx, namespace, table, schema); r.Outcome() != "ok" {
		return fmt.Errorf("create table: %s", rest.Describe(r))
	}
	return nil
}

// RunAppend has every client commit unique keys concurrently.
//
// Each append is a single-table commit with a set-properties update that adds
// one unique key. The catalog applies set-properties on top of whatever the
// current metadata is, so a correct catalog never loses an acknowledged key,
// even when clients race. Real data-file appends come later (milestone M2+);
// properties keep this first workload free of Parquet and object storage.
func RunAppend(ctx context.Context, c *rest.Client, cfg AppendConfig, rec *history.Recorder) {
	c = c.WithRecorder(rec)
	var wg sync.WaitGroup
	for p := 0; p < cfg.Clients; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < cfg.PerClient; i++ {
				if ctx.Err() != nil {
					return
				}
				key := fmt.Sprintf("%s%d-%d", AppendKeyPrefix, p, i)
				value, _ := json.Marshal(map[string]string{"key": key})

				change := rest.TableChange{
					Requirements: []map[string]any{},
					Updates: []map[string]any{{
						"action":  "set-properties",
						"updates": map[string]string{key: "1"},
					}},
				}
				callCtx := history.WithOperation(ctx, history.Op{Process: p, F: "append", Phase: "workload", Value: value})
				c.CommitTable(callCtx, cfg.Namespace, cfg.Table, change)
			}
		}(p)
	}
	wg.Wait()
}

// FinalAppendKeys loads the table and returns every append key it holds.
func FinalAppendKeys(ctx context.Context, c *rest.Client, namespace, table string) ([]string, error) {
	meta, err := rest.ParseMetadata(c.LoadTable(ctx, namespace, table))
	if err != nil {
		return nil, fmt.Errorf("final read: %w", err)
	}
	return AppendKeys(meta), nil
}

// AppendKeys extracts the workload's keys from saved or freshly loaded metadata.
func AppendKeys(meta rest.TableMetadata) []string {
	var keys []string
	for k := range meta.Properties {
		if strings.HasPrefix(k, AppendKeyPrefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
