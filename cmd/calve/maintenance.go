package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
	"github.com/aiskakov28/calve/internal/workload"
)

func maintenanceCmd(args []string) {
	fs := flag.NewFlagSet("maintenance", flag.ExitOnError)
	cf := commonFlags(fs)
	appends := fs.Int("appends", 40, "guarded snapshot appends to race against expiration")
	interval := fs.Duration("expire-interval", 10*time.Millisecond, "pause between expiration passes")
	model := fs.String("model", checker.MaintenanceModelVersion, "maintenance model: calve-maintenance-v1 or calve-maintenance-current-v1")
	root := fs.String("manifest-root", "", "manifest storage root (default: the fresh table's location)")
	out := fs.String("out", "history-maintenance.jsonl", "where to save HTTP evidence")
	endpoint := fs.String("s3-endpoint", "", "path-style S3 endpoint for manifest uploads")
	region := fs.String("s3-region", "us-east-1", "S3 signing region")
	readback := readbackFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected maintenance arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	store := storage.ManifestStore{Endpoint: *endpoint, Region: *region, AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN"), HTTP: c.HTTP}
	if err := store.Validate(); err != nil {
		fail("%v", err)
	}
	res, err := executeMaintenance(context.Background(), c, workload.MaintenanceConfig{Namespace: cf.namespace, Appends: *appends,
		ExpireInterval: *interval, ManifestRoot: *root, Model: *model}, store, *out, *readback)
	if err != nil {
		fail("%s", history.RedactText(err.Error(), cf.catalog, cf.token, store.SecretKey, store.SessionToken))
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportMaintenance(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeMaintenance(ctx context.Context, c *rest.Client, cfg workload.MaintenanceConfig, store workload.ManifestWriter, out string, readback readbackConfig) (result checker.MaintenanceResult, err error) {
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	if err = readback.validate(); err != nil {
		return result, err
	}
	rec := history.NewRecorder()
	defer func() {
		// Setup is outside this evidence contract. Preserve an existing file
		// when failure happens before any checked window was recorded.
		if ops := rec.Ops(); len(ops) > 0 {
			if e := history.SaveFile(out, ops); e != nil {
				err = errors.Join(err, fmt.Errorf("save maintenance history: %w", e))
			}
		}
	}()
	if _, err = workload.RunMaintenance(ctx, c, cfg, store, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckMaintenance(rec.Ops())
	if err != nil || result.Verdict != checker.Inconclusive || readback.Timeout == 0 {
		return result, err
	}
	err = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
		var e error
		result, e = checker.CheckMaintenance(rec.Ops())
		return result.Verdict != checker.Inconclusive, e
	})
	return result, err
}

func reportMaintenance(out io.Writer, res checker.MaintenanceResult) int {
	fmt.Fprintf(out, "maintenance race (%s): %d appends (%d ok, %d rejected, %d unknown, %d resolved as committed); %d unresolved\n",
		res.Model, res.Appends, res.AppendsOK, res.AppendsFailed, res.AppendsUnknown, res.ResolvedCommitted, res.Unresolved)
	fmt.Fprintf(out, "expirations: %d sent, %d acknowledged, %d raced an append\n", res.Expirations, res.ExpirationsOK, res.RacingExpirations)
	switch res.Verdict {
	case checker.Invalid:
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		encoder.Encode(res.Anomalies)
		fmt.Fprintln(out, "INVALID: snapshot expiration racing appends broke the single-table contract")
		return 3
	case checker.Inconclusive:
		fmt.Fprintln(out, "INCONCLUSIVE: maintenance history could not be fully decided")
		for _, reason := range res.Reasons {
			fmt.Fprintf(out, "  %s\n", reason)
		}
		return 4
	default:
		fmt.Fprintln(out, "VALID: expiration removed only listed snapshots and kept every committed append on main")
		return 0
	}
}

// executeMaintenanceCampaign runs the maintenance workload under camp: appends
// and expirations run from the plan's start to its horizon, a failed reload is
// retried, and the final read follows healing.
func executeMaintenanceCampaign(ctx context.Context, c *rest.Client, cfg workload.MaintenanceConfig, store workload.ManifestWriter,
	readback readbackConfig, camp *campaign, dir, target, runID string) (res checker.MaintenanceResult, err error) {
	_, _, err = executeCampaign(camp, dir, target, runID, "maintenance-race", func(path string) (any, checker.Verdict, error) {
		rec := history.NewRecorder()
		link := map[string]any{"fault_run_id": runID, "fault_seed": camp.plan.Seed, "fault_plan": filepath.Base(camp.planPath),
			"fault_history": filepath.Base(camp.faultsPath)}
		runErr := camp.runHooked(ctx, runID, rec.Start(), link, func(hooks *workload.CampaignHooks) error {
			cfg.Hooks = hooks
			_, e := workload.RunMaintenance(ctx, c, cfg, store, rec)
			return e
		})
		if runErr == nil {
			res, runErr = checker.CheckMaintenance(rec.Ops())
		}
		if runErr == nil && res.Verdict == checker.Inconclusive && readback.Timeout > 0 {
			runErr = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
				var e error
				res, e = checker.CheckMaintenance(rec.Ops())
				return res.Verdict != checker.Inconclusive, e
			})
		}
		if saveErr := history.SaveFile(path, rec.Ops()); saveErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("save maintenance history: %w", saveErr))
		}
		return res, res.Verdict, runErr
	})
	return res, err
}
