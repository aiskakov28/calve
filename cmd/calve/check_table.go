package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
)

func checkTableCmd(args []string) {
	code, err := checkTable(args, os.Stdout)
	if err != nil {
		fail("%v", err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

func checkTable(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("check-table", flag.ContinueOnError)
	fs.SetOutput(out)
	timeout := fs.Duration("timeout", 5*time.Second, "maximum Porcupine search time")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, nil
		}
		return 0, err
	}
	if fs.NArg() != 1 || *timeout <= 0 {
		return 0, fmt.Errorf("usage: calve check-table [--timeout 5s] <snapshot-history.jsonl>")
	}
	ops, err := readOps(fs.Arg(0))
	if err != nil {
		return 0, err
	}
	res, err := checker.CheckSnapshotHistory(ops, *timeout)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(out, "snapshot check (%s): %d ok, %d rejected, %d unknown (%d resolved as committed)\n",
		res.Model, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
	switch res.Verdict {
	case checker.Invalid:
		fmt.Fprintln(out, "INVALID: snapshot history is not linearizable under the guarded snapshot model")
		fmt.Fprintf(out, "  evidence operation IDs: %v (whole history, not a minimized witness)\n", res.OperationIDs)
		return 3, nil
	case checker.Inconclusive:
		fmt.Fprintln(out, "INCONCLUSIVE: snapshot history could not be fully decided")
		for _, reason := range res.Reasons {
			fmt.Fprintf(out, "  %s\n", reason)
		}
		return 4, nil
	default:
		fmt.Fprintln(out, "VALID: snapshot CAS commits and reads fit an atomic real-time ordering")
		return 0, nil
	}
}
