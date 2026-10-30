package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

// executeSkewCampaign runs the skew workload under camp: every round is set up
// before the plan starts, concurrent rounds run until its horizon, and the
// final reads and both controls follow healing.
func executeSkewCampaign(ctx context.Context, c *rest.Client, cfg workload.SkewConfig, readback readbackConfig,
	camp *campaign, dir, target, runID string) (res checker.SkewResult, err error) {
	if err := readback.validate(); err != nil {
		return res, err
	}
	_, _, err = executeCampaign(camp, dir, target, runID, "skew", func(path string) (any, checker.Verdict, error) {
		rec := history.NewRecorder()
		link := map[string]any{"fault_run_id": runID, "fault_seed": camp.plan.Seed, "fault_plan": filepath.Base(camp.planPath),
			"fault_history": filepath.Base(camp.faultsPath)}
		runErr := camp.runHooked(ctx, runID, rec.Start(), link, func(hooks *workload.CampaignHooks) error {
			cfg.Hooks = hooks
			_, e := workload.RunSkew(ctx, c, cfg, rec)
			return e
		})
		if runErr == nil {
			res, runErr = checker.CheckSkew(rec.Ops())
		}
		if rounds := skewUnobserved(res); runErr == nil && len(rounds) != 0 && res.Verdict != checker.Invalid && readback.Timeout > 0 {
			runErr = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), rounds), func() (bool, error) {
				var e error
				res, e = checker.CheckSkew(rec.Ops())
				return len(skewUnobserved(res)) == 0 || res.Verdict == checker.Invalid, e
			})
		}
		if saveErr := history.SaveFile(path, rec.Ops()); saveErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("save skew history: %w", saveErr))
		}
		return res, res.Verdict, runErr
	})
	return res, err
}

// skewUnobserved lists rounds that later reads could still decide: an unknown
// transaction, or a final read that failed.
func skewUnobserved(res checker.SkewResult) map[string]bool {
	rounds := map[string]bool{}
	for _, round := range res.Rounds {
		for _, txn := range round.Transactions {
			if txn.CommitState == "unknown" {
				rounds[round.Round] = true
			}
		}
		for _, reason := range round.Reasons {
			if strings.HasPrefix(reason, "final observation: ") {
				rounds[round.Round] = true
			}
		}
	}
	return rounds
}
