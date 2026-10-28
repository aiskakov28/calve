package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// Read-back is observation only. Its deadline never establishes that server-side
// writes have stopped, and its GETs must never retry a commit.
type readbackConfig struct {
	Timeout  time.Duration
	Interval time.Duration
}

func readbackFlags(fs *flag.FlagSet) *readbackConfig {
	cfg := &readbackConfig{}
	fs.DurationVar(&cfg.Timeout, "readback-timeout", 5*time.Second, "budget for observing unknown commits; 0 disables extra reads")
	fs.DurationVar(&cfg.Interval, "readback-interval", 100*time.Millisecond, "interval between unknown-commit observation passes")
	return cfg
}

func (cfg readbackConfig) validate() error {
	if cfg.Timeout < 0 || cfg.Interval < 0 || cfg.Timeout > 0 && cfg.Interval == 0 {
		return fmt.Errorf("read-back timeout must be nonnegative and enabled read-back needs a positive interval")
	}
	return nil
}

type readbackTarget struct {
	URL   string
	Round string
}

func readbackTargets(ops []history.Op, rounds map[string]bool) []readbackTarget {
	var targets []readbackTarget
	for _, op := range ops {
		if op.Type == history.Invoke && op.F == "load-table" && op.Phase == "baseline" &&
			op.Request != nil && rounds[op.Round] {
			targets = append(targets, readbackTarget{URL: op.Request.URL, Round: op.Round})
		}
	}
	return targets
}

func readBack(ctx context.Context, c *rest.Client, cfg readbackConfig, targets []readbackTarget, decided func() (bool, error)) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.Timeout == 0 || len(targets) == 0 {
		return nil
	}
	// Cancelled workload requests do not cancel this independently bounded
	// observation budget. This does not heal faults or join server-side work.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Timeout)
	defer cancel()
	for {
		for _, target := range targets {
			if ctx.Err() != nil {
				return nil
			}
			callCtx := history.WithOperation(ctx, history.Op{Process: -1, Phase: "recovery", Round: target.Round})
			c.ReloadTable(callCtx, target.URL)
			done, err := decided()
			if err != nil || done {
				return err
			}
		}
		timer := time.NewTimer(cfg.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
