package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
	"github.com/aiskakov28/calve/internal/workload"
)

func snapshotCmd(args []string) {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	cf := commonFlags(fs)
	clients := fs.Int("clients", 4, "concurrent CAS contenders per round")
	rounds := fs.Int("rounds", 2, "rounds using fresh observations of main")
	model := fs.String("model", checker.SnapshotModelVersion, "snapshot model: calve-snapshot-v1 or calve-snapshot-current-v1")
	root := fs.String("manifest-root", "", "manifest storage root (default: the fresh table's location)")
	out := fs.String("out", "history-snapshot.jsonl", "snapshot HTTP evidence for calve check-table")
	search := fs.Duration("check-timeout", 5*time.Second, "maximum Porcupine search time")
	endpoint := fs.String("s3-endpoint", "", "path-style S3 endpoint for manifest uploads")
	region := fs.String("s3-region", "us-east-1", "S3 signing region")
	readback := readbackFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected snapshot arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	store := storage.ManifestStore{Endpoint: *endpoint, Region: *region, AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN"), HTTP: c.HTTP}
	if err := store.Validate(); err != nil {
		fail("%v", err)
	}
	res, err := executeSnapshot(context.Background(), c, workload.SnapshotConfig{Namespace: cf.namespace, Clients: *clients, Rounds: *rounds, ManifestRoot: *root, Model: *model},
		store, *out, *search, *readback)
	if err != nil {
		fail("%s", history.RedactText(err.Error(), cf.catalog, cf.token, store.SecretKey, store.SessionToken))
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportSnapshot(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeSnapshot(ctx context.Context, c *rest.Client, cfg workload.SnapshotConfig, store workload.ManifestWriter, out string, search time.Duration, readback readbackConfig) (result checker.SnapshotResult, err error) {
	if err = refuseExisting(out, "--out"); err != nil {
		return result, err
	}
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	if err = readback.validate(); err != nil {
		return result, err
	}
	if search <= 0 {
		return result, fmt.Errorf("snapshot checker needs a positive search timeout")
	}
	rec := history.NewRecorder()
	defer func() {
		// Setup is outside this evidence contract: save nothing when failure
		// happens before any checked window was recorded.
		if ops := rec.Ops(); len(ops) > 0 {
			if e := history.SaveFile(out, ops); e != nil {
				err = errors.Join(err, fmt.Errorf("save snapshot history: %w", e))
			}
		}
	}()
	if _, err = workload.RunSnapshots(ctx, c, cfg, store, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckSnapshotHistory(rec.Ops(), search)
	if err != nil || result.Verdict != checker.Inconclusive || readback.Timeout == 0 {
		return result, err
	}
	err = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
		var e error
		result, e = checker.CheckSnapshotHistory(rec.Ops(), search)
		return result.Verdict != checker.Inconclusive, e
	})
	return result, err
}

// executeSnapshotCampaign runs the snapshot workload under camp: guarded
// rounds run from the plan's start to its horizon, a failed guard read is
// retried, and the final read follows healing.
func executeSnapshotCampaign(ctx context.Context, c *rest.Client, cfg workload.SnapshotConfig, store workload.ManifestWriter,
	readback readbackConfig, camp *campaign, dir, target, runID string) (res checker.SnapshotResult, err error) {
	_, _, err = executeCampaign(camp, dir, target, runID, "snapshot-cas", func(path string) (any, checker.Verdict, error) {
		rec := history.NewRecorder()
		link := map[string]any{"fault_run_id": runID, "fault_seed": camp.plan.Seed, "fault_plan": filepath.Base(camp.planPath),
			"fault_history": filepath.Base(camp.faultsPath)}
		runErr := camp.runHooked(ctx, runID, rec.Start(), link, func(hooks *workload.CampaignHooks) error {
			cfg.Hooks = hooks
			_, e := workload.RunSnapshots(ctx, c, cfg, store, rec)
			return e
		})
		if runErr == nil {
			res, runErr = checker.CheckSnapshotHistory(rec.Ops(), campaignSnapshotSearch)
		}
		if runErr == nil && res.Verdict == checker.Inconclusive && readback.Timeout > 0 {
			runErr = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
				var e error
				res, e = checker.CheckSnapshotHistory(rec.Ops(), campaignSnapshotSearch)
				return res.Verdict != checker.Inconclusive, e
			})
		}
		if saveErr := history.SaveFile(path, rec.Ops()); saveErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("save snapshot history: %w", saveErr))
		}
		return res, res.Verdict, runErr
	})
	return res, err
}
