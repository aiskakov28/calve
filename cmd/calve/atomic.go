package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

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
	readers := fs.Int("readers", 1, "concurrent table readers (0 disables)")
	readInterval := fs.Duration("read-interval", 10*time.Millisecond, "pause between each reader's full table passes")
	out := fs.String("out", "history-atomic.jsonl", "where to save HTTP evidence")
	readback := readbackFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected atomic arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	res, err := executeAtomic(context.Background(), c, workload.AtomicConfig{Namespace: cf.namespace, Tables: *tables, Clients: *clients, PerClient: *perClient,
		Readers: *readers, ReadInterval: *readInterval}, *out, *readback)
	if err != nil {
		fail("%s", history.RedactText(err.Error(), cf.catalog, cf.token))
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportAtomic(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeAtomic(ctx context.Context, c *rest.Client, cfg workload.AtomicConfig, out string, readback readbackConfig) (result checker.AtomicResult, err error) {
	// Reject arguments before arming the history save: an empty failed run must
	// not replace an existing evidence file.
	if err := cfg.Validate(); err != nil {
		return result, err
	}
	if err := readback.validate(); err != nil {
		return result, err
	}
	rec := history.NewRecorder()
	defer func() {
		if saveErr := history.SaveFile(out, rec.Ops()); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save atomic history: %w", saveErr))
		}
	}()
	if _, err = workload.RunAtomic(ctx, c, cfg, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckAtomic(rec.Ops())
	decided := func() bool {
		// Later GETs cannot create concurrent coverage. Continue recovery for
		// unknown commits or missing final evidence, then stop if this is the
		// only remaining limitation.
		return result.Verdict != checker.Inconclusive ||
			result.ReadCoverageMissing && result.Unresolved == 0 && len(result.Reasons) == 1
	}
	if err != nil || decided() || readback.Timeout == 0 {
		return result, err
	}
	config, _ := json.Marshal(map[string]int64{"timeout_ns": readback.Timeout.Nanoseconds(), "interval_ns": readback.Interval.Nanoseconds()})
	rec.Record(history.Op{Type: history.Meta, Process: -1, Phase: "recovery", F: "readback-config", Value: config})
	err = readBack(ctx, c.WithRecorder(rec), readback, readbackTargets(rec.Ops(), map[string]bool{"": true}), func() (bool, error) {
		var checkErr error
		result, checkErr = checker.CheckAtomic(rec.Ops())
		return decided(), checkErr
	})
	return result, err
}

func reportAtomic(out io.Writer, res checker.AtomicResult) int {
	fmt.Fprintf(out, "multi-table atomic: %d ok, %d rejected, %d unknown (%d resolved as committed); %d unresolved\n",
		res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, res.Unresolved)
	fmt.Fprintf(out, "workload reads: %d attempted; %d usable during transactions\n", res.WorkloadReads, res.ConcurrentReads)
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
			if finding.Basis == "atomic-visibility-model" || finding.Basis == "fresh-read-model" {
				fmt.Fprintln(out, "visibility findings assume fresh table loads; confirm the catalog's guarantees before reporting a bug")
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
	if historyWorkload(ops) == "schema-cas" {
		res, err := checker.CheckSchema(ops)
		if err != nil {
			return 0, err
		}
		return reportSchema(out, res), nil
	}
	res, err := checker.CheckAtomic(ops)
	if err != nil {
		return 0, err
	}
	return reportAtomic(out, res), nil
}
