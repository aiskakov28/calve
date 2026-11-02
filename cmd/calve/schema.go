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

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func schemaCmd(args []string) {
	fs := flag.NewFlagSet("schema", flag.ExitOnError)
	cf := commonFlags(fs)
	clients := fs.Int("clients", 4, "concurrent schema contenders per round (at least 2)")
	rounds := fs.Int("rounds", 10, "rounds, each guarded on a fresh read of the current schema")
	out := fs.String("out", "history-schema.jsonl", "where to save HTTP evidence")
	readback := readbackFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected schema arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	res, err := executeSchema(context.Background(), c, workload.SchemaConfig{Namespace: cf.namespace, Clients: *clients, Rounds: *rounds}, *out, *readback)
	if err != nil {
		fail("%s", history.RedactText(err.Error(), cf.catalog, cf.token))
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportSchema(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeSchema(ctx context.Context, c *rest.Client, cfg workload.SchemaConfig, out string, readback readbackConfig) (result checker.SchemaResult, err error) {
	if err = refuseExisting(out, "--out"); err != nil {
		return result, err
	}
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	if err = readback.validate(); err != nil {
		return result, err
	}
	rec := history.NewRecorder()
	defer func() {
		// Setup is outside this evidence contract: save nothing when failure
		// happens before any checked window was recorded.
		if ops := rec.Ops(); len(ops) > 0 {
			if e := history.SaveFile(out, ops); e != nil {
				err = errors.Join(err, fmt.Errorf("save schema history: %w", e))
			}
		}
	}()
	if _, err = workload.RunSchema(ctx, c, cfg, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckSchema(rec.Ops())
	if err != nil || result.Verdict != checker.Inconclusive || readback.Timeout == 0 {
		return result, err
	}
	err = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"0": true}), func() (bool, error) {
		var e error
		result, e = checker.CheckSchema(rec.Ops())
		return result.Verdict != checker.Inconclusive, e
	})
	return result, err
}

func reportSchema(out io.Writer, res checker.SchemaResult) int {
	fmt.Fprintf(out, "schema CAS: %d rounds, %d with a winner; %d ok, %d rejected, %d unknown (%d resolved as committed); %d unresolved\n",
		res.Rounds, res.RoundsWithWinner, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, res.Unresolved)
	switch res.Verdict {
	case checker.Invalid:
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		encoder.Encode(res.Anomalies)
		fmt.Fprintln(out, "INVALID: guarded schema commits broke the single-table contract")
		return 3
	case checker.Inconclusive:
		fmt.Fprintln(out, "INCONCLUSIVE: schema history could not be fully decided")
		for _, reason := range res.Reasons {
			fmt.Fprintf(out, "  %s\n", reason)
		}
		return 4
	default:
		fmt.Fprintln(out, "VALID: at most one guarded schema change took effect per round, and no change was lost or wrongly rejected")
		return 0
	}
}

// historyWorkload names the workload in a JSONL history's run record.
func historyWorkload(ops []history.Op) string {
	var run struct {
		Workload string `json:"workload"`
	}
	if len(ops) > 0 && ops[0].Type == history.Meta && ops[0].F == "run" {
		json.Unmarshal(ops[0].Value, &run)
	}
	return run.Workload
}

// executeSchemaCampaign runs the schema workload under camp: rounds run from
// the plan's start to its horizon, with failed guard reads retried, and the
// final read after every fault is healed and the catalog answers.
func executeSchemaCampaign(ctx context.Context, c *rest.Client, cfg workload.SchemaConfig, readback readbackConfig,
	camp *campaign, dir, target, runID string) (res checker.SchemaResult, err error) {
	_, _, err = executeCampaign(camp, dir, target, runID, "schema-cas", func(path string) (any, checker.Verdict, error) {
		rec := history.NewRecorder()
		link := map[string]any{"fault_run_id": runID, "fault_seed": camp.plan.Seed, "fault_plan": filepath.Base(camp.planPath),
			"fault_history": filepath.Base(camp.faultsPath)}
		runErr := camp.runHooked(ctx, runID, rec.Start(), link, func(hooks *workload.CampaignHooks) error {
			cfg.Hooks = hooks
			_, e := workload.RunSchema(ctx, c, cfg, rec)
			return e
		})
		if runErr == nil {
			res, runErr = checker.CheckSchema(rec.Ops())
		}
		if runErr == nil && res.Verdict == checker.Inconclusive && readback.Timeout > 0 {
			runErr = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"0": true}), func() (bool, error) {
				var e error
				res, e = checker.CheckSchema(rec.Ops())
				return res.Verdict != checker.Inconclusive, e
			})
		}
		if saveErr := history.SaveFile(path, rec.Ops()); saveErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("save schema history: %w", saveErr))
		}
		return res, res.Verdict, runErr
	})
	return res, err
}
