package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

const AtomicKeyPrefix = "calve.atomic."

type AtomicConfig struct {
	Namespace string
	Tables    int
	Clients   int
	PerClient int
}

// AtomicRun is execution provenance. Requests and observations, not a derived
// run summary, determine the checker verdict.
type AtomicRun struct {
	RunID     string            `json:"run_id"`
	Workload  string            `json:"workload"`
	Catalog   string            `json:"catalog"`
	Warehouse string            `json:"warehouse"`
	Namespace string            `json:"namespace"`
	Tables    []rest.Identifier `json:"tables"`
	Clients   int               `json:"clients"`
	PerClient int               `json:"per_client"`
}

func (cfg AtomicConfig) Validate() error {
	if cfg.Namespace == "" || cfg.Tables < 2 || cfg.Tables > 32 || cfg.Clients <= 0 ||
		cfg.PerClient <= 0 || cfg.Clients > int(^uint(0)>>1)/cfg.PerClient {
		return fmt.Errorf("atomic requires a namespace, 2..32 tables, and positive clients/per-client without overflow")
	}
	for _, level := range strings.Split(cfg.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("atomic namespace contains an empty level")
		}
	}
	return nil
}

// RunAtomic sets a unique marker on every table in each transaction. All
// requests use baseline UUID guards and run exactly once. No request removes or
// overwrites a marker. Tables are created without dropping anything and remain
// available for inspection. The caller must save rec, including on errors.
func RunAtomic(ctx context.Context, c *rest.Client, cfg AtomicConfig, rec *history.Recorder) (run AtomicRun, err error) {
	if c == nil || rec == nil {
		return run, fmt.Errorf("atomic requires a client and recorder")
	}
	if err := cfg.Validate(); err != nil {
		return run, err
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return run, err
	}
	run = AtomicRun{RunID: "atomic_" + hex.EncodeToString(nonce[:]), Workload: "atomic",
		Catalog:   history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		Warehouse: history.RedactText(c.Warehouse, "", c.Token), Namespace: cfg.Namespace,
		Clients: cfg.Clients, PerClient: cfg.PerClient}
	for i := 0; i < cfg.Tables; i++ {
		run.Tables = append(run.Tables, rest.Identifier{Namespace: strings.Split(cfg.Namespace, "."), Name: fmt.Sprintf("%s_t%d", run.RunID, i)})
	}
	value, _ := json.Marshal(run)
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Phase: "setup", Value: value})
	c = c.WithRecorder(rec)
	labeled := func(phase string) context.Context {
		return history.WithOperation(ctx, history.Op{Process: -1, Phase: phase})
	}
	if _, err := c.Config(labeled("setup")); err != nil {
		return run, err
	}
	if response := c.CreateNamespace(labeled("setup"), cfg.Namespace); response.Outcome() != "ok" && response.Status != 409 {
		return run, fmt.Errorf("create namespace: %s", rest.Describe(response))
	}
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Required: true, Type: "long"}}}
	for _, table := range run.Tables {
		if response := c.CreateTable(labeled("setup"), cfg.Namespace, table.Name, schema); response.Outcome() != "ok" {
			return run, fmt.Errorf("create table: %s", rest.Describe(response))
		}
	}
	uuids := make([]string, len(run.Tables))
	used := map[string]bool{}
	for i, table := range run.Tables {
		meta, err := rest.ParseMetadata(c.LoadTable(labeled("baseline"), cfg.Namespace, table.Name))
		if err != nil || meta.TableUUID == "" || used[meta.TableUUID] {
			return run, fmt.Errorf("atomic baseline: missing/duplicate table identity or failed load: %v", err)
		}
		for key := range meta.Properties {
			if strings.HasPrefix(key, AtomicKeyPrefix+run.RunID+".") {
				return run, fmt.Errorf("atomic baseline contains this run's markers")
			}
		}
		uuids[i], used[meta.TableUUID] = meta.TableUUID, true
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(cfg.Clients)
	done.Add(cfg.Clients)
	for p := 0; p < cfg.Clients; p++ {
		go func(p int) {
			defer done.Done()
			ready.Done()
			<-start
			for i := 0; i < cfg.PerClient; i++ {
				if ctx.Err() != nil {
					return
				}
				key := fmt.Sprintf("%s%s.%d-%d", AtomicKeyPrefix, run.RunID, p, i)
				changes := make([]rest.TableChange, 0, len(run.Tables))
				for t, table := range run.Tables {
					identifier := table
					changes = append(changes, rest.TableChange{Identifier: &identifier,
						Requirements: []map[string]any{{"type": "assert-table-uuid", "uuid": uuids[t]}},
						Updates:      []map[string]any{{"action": "set-properties", "updates": map[string]string{key: "1"}}}})
				}
				label, _ := json.Marshal(map[string]string{"key": key})
				callCtx := history.WithOperation(ctx, history.Op{Process: p, Phase: "workload", F: "atomic-txn", Value: label})
				c.CommitTransaction(callCtx, changes)
			}
		}(p)
	}
	ready.Wait()
	close(start)
	done.Wait()
	// Record every final load even if another table is unavailable. These GETs
	// need not be a consistent cross-table snapshot; their intervals matter.
	for _, table := range run.Tables {
		c.LoadTable(labeled("final"), cfg.Namespace, table.Name)
	}
	return run, ctx.Err()
}
