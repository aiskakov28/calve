package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type SnapshotConfig struct {
	Namespace       string
	Clients, Rounds int
	ManifestRoot    string // Optional override; otherwise use the fresh table location.
	Model           string // Empty selects calve-snapshot-v1; never inferred from the catalog.
	// Hooks drive the run under a fault campaign; nil runs without faults.
	// Under hooks Rounds only caps the run, which ends at the plan's horizon.
	Hooks *CampaignHooks
}

type SnapshotRun struct {
	RunID     string `json:"run_id"`
	Workload  string `json:"workload"`
	Model     string `json:"model"`
	Catalog   string `json:"catalog"`
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
	Commits   int    `json:"commits"`
}

type ManifestWriter interface {
	Put(context.Context, string, []byte) error
}

func (cfg SnapshotConfig) Validate() error {
	if cfg.Clients <= 0 || cfg.Rounds <= 0 || cfg.Clients > int(^uint(0)>>1)/cfg.Rounds {
		return fmt.Errorf("snapshot requires positive clients/rounds without overflow")
	}
	for _, level := range strings.Split(cfg.Namespace, ".") {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("snapshot requires nonempty namespace levels")
		}
	}
	if cfg.Model != "" && cfg.Model != "calve-snapshot-v1" && cfg.Model != "calve-snapshot-current-v1" {
		return fmt.Errorf("unsupported snapshot model")
	}
	if cfg.ManifestRoot != "" {
		_, err := snapshotManifestURI(cfg.ManifestRoot, "validation.avro")
		return err
	}
	return nil
}

type snapshotBase struct {
	UUID      string            `json:"table-uuid"`
	Format    int               `json:"format-version"`
	Sequence  *int64            `json:"last-sequence-number"`
	Current   *int64            `json:"current-snapshot-id"`
	Schema    *int              `json:"current-schema-id"`
	Location  string            `json:"location"`
	Snapshots []json.RawMessage `json:"snapshots"`
}

func snapshotBaseMetadata(response rest.Response) (snapshotBase, error) {
	var body struct {
		Metadata snapshotBase `json:"metadata"`
	}
	if json.Unmarshal(response.Body, &body) != nil || body.Metadata.Format != 2 || body.Metadata.UUID == "" ||
		body.Metadata.Sequence == nil || *body.Metadata.Sequence < 0 || body.Metadata.Schema == nil || *body.Metadata.Schema < 0 {
		return snapshotBase{}, fmt.Errorf("snapshot workload requires usable v2 metadata, UUID, schema ID and last sequence number")
	}
	if body.Metadata.Current != nil && *body.Metadata.Current == -1 {
		body.Metadata.Current = nil
	}
	return body.Metadata, nil
}

func snapshotManifestURI(root, name string) (string, error) {
	if filepath.IsAbs(root) {
		path := filepath.ToSlash(root)
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		root = (&url.URL{Scheme: "file", Path: path}).String()
	}
	u, err := url.Parse(root)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		u.Scheme != "file" && u.Scheme != "s3" || u.Scheme == "s3" && u.Host == "" ||
		u.Scheme == "file" && (u.Host != "" && u.Host != "localhost" || !strings.HasPrefix(u.Path, "/")) {
		return "", fmt.Errorf("snapshot manifest root must be an absolute file: or s3: location")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/metadata/" + name
	u.RawPath = ""
	return u.String(), nil
}

// RunSnapshots creates a fresh table without dropping anything. Setup is
// unrecorded; a new checked window starts with the manifest and baseline GET.
// Each round offers independent, single-attempt snapshots using one observed
// main-ref guard. Contenders propose the next sequence, as normal clients do.
func RunSnapshots(ctx context.Context, c *rest.Client, cfg SnapshotConfig, store ManifestWriter, rec *history.Recorder) (run SnapshotRun, err error) {
	if c == nil || store == nil || rec == nil || len(rec.Ops()) != 0 {
		return run, fmt.Errorf("snapshot requires a client, manifest writer and empty recorder")
	}
	if err = cfg.Validate(); err != nil {
		return run, err
	}
	var nonce [12]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return run, err
	}
	name := "snapshot_" + hex.EncodeToString(nonce[:])
	model := cfg.Model
	if model == "" {
		model = "calve-snapshot-v1"
	}
	run = SnapshotRun{RunID: name, Workload: "snapshot-cas", Model: model,
		Catalog: history.RedactText(history.SafeURL(c.BaseURL), "", c.Token), Namespace: cfg.Namespace, Table: name, Commits: cfg.Clients * cfg.Rounds}
	c = c.WithRecorder(nil)
	if response, e := c.Config(ctx); e != nil {
		return run, workloadResponseError("snapshot config", c, response)
	}
	if response := c.CreateNamespace(ctx, cfg.Namespace); response.Outcome() != "ok" && response.Status != 409 {
		return run, workloadResponseError("snapshot namespace", c, response)
	}
	schema := rest.Schema{Type: "struct", Fields: []rest.Field{{ID: 1, Name: "id", Type: "long", Required: true}}}
	if response := c.CreateTable(ctx, cfg.Namespace, name, schema); response.Outcome() != "ok" {
		return run, workloadResponseError("snapshot create table", c, response)
	}
	value, err := runRecord(run, cfg.Hooks)
	if err != nil {
		return run, err
	}
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: value})
	c = c.WithRecorder(rec)
	load := func(phase string) (snapshotBase, error) {
		response := c.LoadTable(history.WithOperation(ctx, history.Op{Process: -1, Phase: phase}), cfg.Namespace, name)
		if response.Outcome() != "ok" {
			return snapshotBase{}, workloadResponseError("snapshot "+phase+" load", c, response)
		}
		return snapshotBaseMetadata(response)
	}
	base, err := load("baseline")
	if err != nil {
		return run, err
	}
	if len(base.Snapshots) != 0 || base.Current != nil || *base.Sequence != 0 {
		return run, fmt.Errorf("new snapshot table is not empty")
	}
	uuid := base.UUID
	root := cfg.ManifestRoot
	if root == "" {
		root = base.Location
	}
	// The load phase starts here: under a campaign, once the plan has started.
	cfg.Hooks.ready()
	for round := 0; round < cfg.Rounds; round++ {
		if ctx.Err() != nil || round > 0 && cfg.Hooks.stopped() {
			break
		}
		if round > 0 {
			base, err = load("workload")
			// Under a campaign a failed guard read is retried until the horizon.
			for err != nil && cfg.Hooks != nil && cfg.Hooks.waitRetry() {
				base, err = load("workload")
			}
			if err != nil && cfg.Hooks != nil && cfg.Hooks.stopped() {
				err = nil
				break
			}
			if err != nil {
				return run, err
			}
			// A guard read that started before the horizon may succeed after it;
			// the round is not started then.
			if cfg.Hooks.stopped() {
				break
			}
		}
		if base.UUID != uuid || *base.Sequence == math.MaxInt64 {
			return run, fmt.Errorf("snapshot table identity changed or sequence numbers exhausted")
		}
		changes := make([]rest.TableChange, cfg.Clients)
		for p := 0; p < cfg.Clients; p++ {
			id := int64(round*cfg.Clients + p + 1)
			seq := *base.Sequence + 1
			location, e := snapshotManifestURI(root, fmt.Sprintf("%s-%d.avro", run.RunID, id))
			if e != nil {
				return run, e
			}
			body, e := emptyManifestList(id, base.Current, seq)
			if e != nil {
				return run, e
			}
			if e = store.Put(ctx, location, body); e != nil {
				return run, fmt.Errorf("prepare snapshot manifest: %w", e)
			}
			snapshot := map[string]any{"snapshot-id": id, "sequence-number": seq, "timestamp-ms": time.Now().UnixMilli(),
				"manifest-list": location, "schema-id": *base.Schema,
				"summary": map[string]string{"operation": "append", "total-data-files": "0", "total-delete-files": "0", "total-records": "0", "total-files-size": "0"}}
			if base.Current != nil {
				snapshot["parent-snapshot-id"] = *base.Current
			}
			changes[p] = rest.TableChange{
				Requirements: []map[string]any{{"type": "assert-table-uuid", "uuid": uuid}, {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": base.Current}},
				Updates:      []map[string]any{{"action": "add-snapshot", "snapshot": snapshot}, {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id}},
			}
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
				if ctx.Err() != nil {
					return
				}
				call := history.WithOperation(ctx, history.Op{Process: p, Phase: "workload"})
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
