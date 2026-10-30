package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type SchemaConfig struct {
	Namespace       string
	Clients, Rounds int
	// Hooks drive the run under a fault campaign; nil runs without faults.
	// Under hooks Rounds only caps the run, which ends at the plan's horizon.
	Hooks *CampaignHooks
}

type SchemaRun struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
	Clients   int    `json:"clients"`
	Rounds    int    `json:"rounds"`
}

// SchemaMarker labels a contender: the unique column its schema adds.
type SchemaMarker struct {
	Column string `json:"column"`
}

func (cfg SchemaConfig) Validate() error {
	if cfg.Clients < 2 || cfg.Rounds <= 0 || cfg.Clients > int(^uint(0)>>1)/cfg.Rounds {
		return fmt.Errorf("schema requires at least 2 clients and positive rounds without overflow")
	}
	for _, level := range strings.Split(cfg.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("schema requires nonempty namespace levels")
		}
	}
	return nil
}

// SchemaColumn names the column contender p adds in round r.
func SchemaColumn(round, p int) string { return fmt.Sprintf("c_%d_%d", round, p) }

// RunSchema creates a fresh table without dropping anything. Setup is
// unrecorded; the checked window starts with the run record and baseline GET.
// Each round, every contender guards on the same observed current schema ID
// and adds its own column, so at most one may commit. Requests are sent once.
func RunSchema(ctx context.Context, c *rest.Client, cfg SchemaConfig, rec *history.Recorder) (run SchemaRun, err error) {
	if c == nil || rec == nil || len(rec.Ops()) != 0 {
		return run, fmt.Errorf("schema requires a client and an empty recorder")
	}
	if err = cfg.Validate(); err != nil {
		return run, err
	}
	var nonce [12]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return run, err
	}
	name := "schema_" + hex.EncodeToString(nonce[:])
	run = SchemaRun{RunID: name, Workload: "schema-cas", Catalog: history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		Namespace: cfg.Namespace, Table: name, Clients: cfg.Clients, Rounds: cfg.Rounds}
	c = c.WithRecorder(nil)
	if response, e := c.Config(ctx); e != nil {
		return run, workloadResponseError("schema config", c, response)
	}
	if response := c.CreateNamespace(ctx, cfg.Namespace); response.Outcome() != "ok" && response.Status != 409 {
		return run, workloadResponseError("schema namespace", c, response)
	}
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Type: "long", Required: true}}}
	if response := c.CreateTable(ctx, cfg.Namespace, name, schema); response.Outcome() != "ok" {
		return run, workloadResponseError("schema create table", c, response)
	}
	value, err := runRecord(run, cfg.Hooks)
	if err != nil {
		return run, err
	}
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: value})
	c = c.WithRecorder(rec)
	var uuid string
rounds:
	for round := 0; round < cfg.Rounds; round++ {
		if ctx.Err() != nil || round > 0 && cfg.Hooks.stopped() {
			break
		}
		phase := "workload"
		if round == 0 {
			phase = "baseline"
		}
		label := strconv.Itoa(round)
		var base rest.TableMetadata
		for {
			response := c.LoadTable(history.WithOperation(ctx, history.Op{Process: -1, Phase: phase, Round: label}), cfg.Namespace, name)
			var e error
			if base, e = rest.ParseMetadata(response); e == nil {
				break
			}
			// The baseline is read before the plan starts and must succeed. Under
			// a campaign a failed guard read is retried until the horizon; a round
			// whose guard never succeeds is not started.
			if cfg.Hooks == nil || round == 0 {
				return run, workloadResponseError("schema "+phase+" load", c, response)
			}
			if !cfg.Hooks.waitRetry() {
				break rounds
			}
		}
		if uuid == "" {
			uuid = base.TableUUID
		}
		if base.TableUUID == "" || base.TableUUID != uuid {
			return run, fmt.Errorf("schema table identity is missing or changed")
		}
		if round == 0 {
			cfg.Hooks.ready()
		}
		changes := make([]rest.TableChange, cfg.Clients)
		labels := make([]json.RawMessage, cfg.Clients)
		for p := range changes {
			marker := rest.Field{ID: base.LastColumnID + 1, Name: SchemaColumn(round, p), Type: "long"}
			var e error
			if changes[p], e = schemaChange(base, marker); e != nil {
				return run, e
			}
			labels[p], _ = json.Marshal(SchemaMarker{Column: marker.Name})
		}
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(cfg.Clients)
		done.Add(cfg.Clients)
		for p := range changes {
			go func(p int) {
				defer done.Done()
				ready.Done()
				<-start
				if ctx.Err() != nil {
					return
				}
				call := history.WithOperation(ctx, history.Op{Process: p, Phase: "workload", Round: label, Value: labels[p]})
				c.CommitTable(call, cfg.Namespace, name, changes[p])
			}(p)
		}
		ready.Wait()
		close(start)
		done.Wait()
	}
	cfg.Hooks.waitHealed()
	c.LoadTable(history.WithOperation(ctx, history.Op{Process: -1, Phase: "final"}), cfg.Namespace, name)
	return run, ctx.Err()
}

// schemaChange guards on the observed table and current schema, then adds the
// current schema plus one column and makes it current. The proposed ID is
// unused in the full schema history, as the Java client allocates it.
func schemaChange(base rest.TableMetadata, marker rest.Field) (rest.TableChange, error) {
	nextID, err := nextSkewSchemaID(base.Schemas)
	if err != nil {
		return rest.TableChange{}, err
	}
	current, err := skewCurrentSchema(base)
	if err != nil {
		return rest.TableChange{}, err
	}
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(current.Raw, &schema); err != nil {
		return rest.TableChange{}, fmt.Errorf("decode current schema: %w", err)
	}
	schema["schema-id"], _ = json.Marshal(nextID)
	var fields []json.RawMessage
	json.Unmarshal(schema["fields"], &fields)
	added, _ := json.Marshal(marker)
	schema["fields"], _ = json.Marshal(append(fields, added))
	return rest.TableChange{
		Requirements: []map[string]any{
			{"type": "assert-table-uuid", "uuid": base.TableUUID},
			{"type": "assert-current-schema-id", "current-schema-id": base.CurrentSchemaID},
		},
		Updates: []map[string]any{
			{"action": "add-schema", "schema": schema, "last-column-id": marker.ID},
			{"action": "set-current-schema", "schema-id": -1},
		},
	}, nil
}
