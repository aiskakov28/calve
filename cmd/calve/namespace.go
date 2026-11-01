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

func namespaceCmd(args []string) {
	fs := flag.NewFlagSet("namespace", flag.ExitOnError)
	cf := commonFlags(fs)
	clients := fs.Int("clients", 6, "concurrent creates of each round's namespace (at least 2)")
	rounds := fs.Int("rounds", 10, "rounds, each on a fresh namespace under -namespace")
	out := fs.String("out", "history-namespace.jsonl", "where to save HTTP evidence")
	fs.Parse(args)
	if fs.NArg() != 0 {
		fail("unexpected namespace arguments")
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	res, err := executeNamespace(context.Background(), c, workload.NamespaceConfig{Parent: cf.namespace, Clients: *clients, Rounds: *rounds}, *out)
	if err != nil {
		fail("%s", history.RedactText(err.Error(), cf.catalog, cf.token))
	}
	fmt.Printf("history saved to %s\n", *out)
	if code := reportNamespace(os.Stdout, res); code != 0 {
		os.Exit(code)
	}
}

func executeNamespace(ctx context.Context, c *rest.Client, cfg workload.NamespaceConfig, out string) (result checker.NamespaceResult, err error) {
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	rec := history.NewRecorder()
	defer func() {
		// Setup is outside this evidence contract. Preserve an existing file
		// when failure happens before any checked window was recorded.
		if ops := rec.Ops(); len(ops) > 0 {
			if e := history.SaveFile(out, ops); e != nil {
				err = errors.Join(err, fmt.Errorf("save namespace history: %w", e))
			}
		}
	}()
	if _, err = workload.RunNamespace(ctx, c, cfg, rec); err != nil {
		return result, err
	}
	return checker.CheckNamespace(rec.Ops())
}

func reportNamespace(out io.Writer, res checker.NamespaceResult) int {
	fmt.Fprintf(out, "namespace creates: %d rounds, %d with a winner; %d ok, %d rejected, %d unknown (%d resolved as committed); %d unresolved\n",
		res.Rounds, res.RoundsWithWinner, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, res.Unresolved)
	switch res.Verdict {
	case checker.Invalid:
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		encoder.Encode(res.Anomalies)
		fmt.Fprintln(out, "INVALID: concurrent namespace creates broke the create contract")
		return 3
	case checker.Inconclusive:
		fmt.Fprintln(out, "INCONCLUSIVE: namespace history could not be fully decided")
		for _, reason := range res.Reasons {
			fmt.Fprintf(out, "  %s\n", reason)
		}
		return 4
	default:
		fmt.Fprintln(out, "VALID: at most one create of each new namespace took effect, with its own properties, and the rest were rejected")
		return 0
	}
}
