package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type MaintenanceConfig struct {
	Namespace      string
	Appends        int
	ExpireInterval time.Duration
	ManifestRoot   string // Optional override; otherwise use the fresh table location.
	Model          string // Empty selects calve-maintenance-v1; never inferred from the catalog.
}

type MaintenanceRun struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Model     string `json:"model"`
	Table     string `json:"table"`
	Appends   int    `json:"appends"`
}

// MaintenanceOp labels a commit: an append of one snapshot, or an expiration
// of the listed snapshots.
type MaintenanceOp struct {
	Kind      string  `json:"kind"`
	Snapshot  int64   `json:"snapshot,omitempty"`
	Snapshots []int64 `json:"snapshots,omitempty"`
}

func (cfg MaintenanceConfig) Validate() error {
	if cfg.Appends <= 0 || cfg.ExpireInterval <= 0 {
		return fmt.Errorf("maintenance requires positive appends and expire interval")
	}
	for _, level := range strings.Split(cfg.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("maintenance requires nonempty namespace levels")
		}
	}
	if cfg.Model != "" && cfg.Model != "calve-maintenance-v1" && cfg.Model != "calve-maintenance-current-v1" {
		return fmt.Errorf("unsupported maintenance model")
	}
	if cfg.ManifestRoot != "" {
		_, err := snapshotManifestURI(cfg.ManifestRoot, "validation.avro")
		return err
	}
	return nil
}

// RunMaintenance races snapshot expiration against appends on a fresh table,
// which is never dropped. Setup is unrecorded; the checked window starts with
// the run record and baseline GET.
//
// Process 0 appends a chain of snapshots, each guarded on main's head as it
// knows it; after a rejected or unknown append it reloads the table and
// continues from what it sees. Process 1 repeatedly loads the table and
// expires every snapshot except main's head, guarded only on the table UUID,
// as the Java client's expiration does. Every request is sent once.
func RunMaintenance(ctx context.Context, c *rest.Client, cfg MaintenanceConfig, store ManifestWriter, rec *history.Recorder) (run MaintenanceRun, err error) {
	if c == nil || store == nil || rec == nil || len(rec.Ops()) != 0 {
		return run, fmt.Errorf("maintenance requires a client, manifest writer and empty recorder")
	}
	if err = cfg.Validate(); err != nil {
		return run, err
	}
	var nonce [12]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return run, err
	}
	name := "maint_" + hex.EncodeToString(nonce[:])
	model := cfg.Model
	if model == "" {
		model = "calve-maintenance-v1"
	}
	run = MaintenanceRun{RunID: name, Workload: "maintenance-race", Model: model, Catalog: history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		Namespace: cfg.Namespace, Table: name, Appends: cfg.Appends}
	c = c.WithRecorder(nil)
	if response, e := c.Config(ctx); e != nil {
		return run, workloadResponseError("maintenance config", c, response)
	}
	if response := c.CreateNamespace(ctx, cfg.Namespace); response.Outcome() != "ok" && response.Status != 409 {
		return run, workloadResponseError("maintenance namespace", c, response)
	}
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Type: "long", Required: true}}}
	if response := c.CreateTable(ctx, cfg.Namespace, name, schema); response.Outcome() != "ok" {
		return run, workloadResponseError("maintenance create table", c, response)
	}
	value, _ := json.Marshal(run)
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: value})
	c = c.WithRecorder(rec)
	load := func(process int, phase string) (snapshotBase, error) {
		response := c.LoadTable(history.WithOperation(ctx, history.Op{Process: process, Phase: phase}), cfg.Namespace, name)
		if response.Outcome() != "ok" {
			return snapshotBase{}, workloadResponseError("maintenance "+phase+" load", c, response)
		}
		return snapshotBaseMetadata(response)
	}
	base, err := load(-1, "baseline")
	if err != nil {
		return run, err
	}
	if len(base.Snapshots) != 0 || base.Current != nil || *base.Sequence != 0 {
		return run, fmt.Errorf("new maintenance table is not empty")
	}
	uuid := base.UUID
	root := cfg.ManifestRoot
	if root == "" {
		root = base.Location
	}

	appended := make(chan struct{})
	var expirer sync.WaitGroup
	expirer.Add(1)
	go func() {
		defer expirer.Done()
		for {
			select {
			case <-appended:
				return
			default:
			}
			if seen, e := load(1, "workload"); e == nil && seen.Current != nil {
				var ids []int64
				for _, raw := range seen.Snapshots {
					var snapshot struct {
						ID int64 `json:"snapshot-id"`
					}
					if json.Unmarshal(raw, &snapshot) == nil && snapshot.ID != *seen.Current {
						ids = append(ids, snapshot.ID)
					}
				}
				if len(ids) > 0 {
					label, _ := json.Marshal(MaintenanceOp{Kind: "expire", Snapshots: ids})
					c.CommitTable(history.WithOperation(ctx, history.Op{Process: 1, Phase: "workload", Value: label}), cfg.Namespace, name,
						rest.TableChange{
							Requirements: []map[string]any{{"type": "assert-table-uuid", "uuid": uuid}},
							Updates:      []map[string]any{{"action": "remove-snapshots", "snapshot-ids": ids}},
						})
				}
			}
			timer := time.NewTimer(cfg.ExpireInterval)
			select {
			case <-appended:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()

	head, sequence, schemaID := base.Current, *base.Sequence, *base.Schema
	for i := 1; i <= cfg.Appends && ctx.Err() == nil; i++ {
		id := int64(i)
		location, e := snapshotManifestURI(root, fmt.Sprintf("%s-%d.avro", run.RunID, id))
		if e != nil {
			err = e
			break
		}
		body, e := emptyManifestList(id, head, sequence+1)
		if e != nil {
			err = e
			break
		}
		if e = store.Put(ctx, location, body); e != nil {
			err = fmt.Errorf("prepare maintenance manifest: %w", e)
			break
		}
		snapshot := map[string]any{"snapshot-id": id, "sequence-number": sequence + 1, "timestamp-ms": time.Now().UnixMilli(),
			"manifest-list": location, "schema-id": schemaID,
			"summary": map[string]string{"operation": "append", "total-data-files": "0", "total-delete-files": "0", "total-records": "0", "total-files-size": "0"}}
		if head != nil {
			snapshot["parent-snapshot-id"] = *head
		}
		label, _ := json.Marshal(MaintenanceOp{Kind: "append", Snapshot: id})
		response := c.CommitTable(history.WithOperation(ctx, history.Op{Process: 0, Phase: "workload", Value: label}), cfg.Namespace, name,
			rest.TableChange{
				Requirements: []map[string]any{{"type": "assert-table-uuid", "uuid": uuid}, {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": head}},
				Updates:      []map[string]any{{"action": "add-snapshot", "snapshot": snapshot}, {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id}},
			})
		if response.Outcome() == "ok" {
			head, sequence = &id, sequence+1
			continue
		}
		seen, e := load(0, "workload")
		if e != nil {
			err = e
			break
		}
		if seen.UUID != uuid {
			err = fmt.Errorf("maintenance table identity changed")
			break
		}
		head, sequence = seen.Current, *seen.Sequence
	}
	close(appended)
	expirer.Wait()
	c.LoadTable(history.WithOperation(ctx, history.Op{Process: -1, Phase: "final"}), cfg.Namespace, name)
	if err != nil {
		return run, err
	}
	return run, ctx.Err()
}
