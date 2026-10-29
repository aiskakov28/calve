package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func atomicCmd(args []string) {
	fs := flag.NewFlagSet("atomic", flag.ExitOnError)
	cf := commonFlags(fs)
	tables := fs.Int("tables", 2, "tables updated by every transaction (2..32)")
	clients := fs.Int("clients", 4, "concurrent transaction clients")
	perClient := fs.Int("per-client", 10, "single-attempt transactions per client")
	out := fs.String("out", "history-atomic.jsonl", "where to save HTTP evidence")
	readback := readbackFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected atomic arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	res, err := executeAtomic(context.Background(), c, workload.AtomicConfig{Namespace: cf.namespace, Tables: *tables, Clients: *clients, PerClient: *perClient}, *out, *readback)
	if err != nil {
		fail("%v", err)
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportAtomic(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeAtomic(ctx context.Context, c *rest.Client, cfg workload.AtomicConfig, out string, readback readbackConfig) (result checker.AtomicResult, err error) {
	rec := history.NewRecorder()
	defer func() {
		if saveErr := history.SaveFile(out, rec.Ops()); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save atomic history: %w", saveErr))
		}
	}()
	if err := readback.validate(); err != nil {
		return result, err
	}
	if _, err = workload.RunAtomic(ctx, c, cfg, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckAtomic(rec.Ops())
	if err != nil || result.Verdict != checker.Inconclusive || readback.Timeout == 0 {
		return result, err
	}
	config, _ := json.Marshal(map[string]int64{"timeout_ns": readback.Timeout.Nanoseconds(), "interval_ns": readback.Interval.Nanoseconds()})
	rec.Record(history.Op{Type: history.Meta, Process: -1, Phase: "recovery", F: "readback-config", Value: config})
	err = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
		var checkErr error
		result, checkErr = checker.CheckAtomic(rec.Ops())
		return result.Verdict != checker.Inconclusive, checkErr
	})
	return result, err
}

func reportAtomic(out io.Writer, res checker.AtomicResult) int {
	fmt.Fprintf(out, "multi-table atomic: %d ok, %d rejected, %d unknown (%d resolved as committed); %d unresolved\n",
		res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, res.Unresolved)
	switch res.Verdict {
	case checker.Valid:
		fmt.Fprintln(out, "VALID: all checked transaction effects resolved; no lost or rejected markers")
		return 0
	case checker.Invalid:
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		encoder.Encode(res.Anomalies)
		fmt.Fprintln(out, "INVALID: multi-table transaction invariant violated")
		for _, finding := range res.Anomalies {
			if finding.Basis == "atomic-visibility-model" {
				fmt.Fprintln(out, "atomic-visibility findings assume fresh table loads; confirm the catalog's guarantees before reporting a bug")
				break
			}
		}
		return 3
	default:
		fmt.Fprintln(out, "INCONCLUSIVE: multi-table transaction effects could not be fully decided")
		for _, reason := range res.Reasons {
			fmt.Fprintln(out, "  "+reason)
		}
		return 4
	}
}

// checkArtifact retains run-directory replay and adds atomic JSONL replay.
func checkArtifact(path string, out io.Writer) (int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return checkRun(path, out)
	}
	ops, err := readOps(path)
	if err != nil {
		return 0, err
	}
	res, err := checker.CheckAtomic(ops)
	if err != nil {
		return 0, err
	}
	return reportAtomic(out, res), nil
}
