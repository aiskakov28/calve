// Command calve runs correctness workloads against Iceberg REST catalogs.
//
//	calve smoke  --catalog http://localhost:8181
//	calve append --catalog http://localhost:8181 --clients 8 --per-client 50
//	calve skew   --catalog http://localhost:8181 --rounds 10
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: calve <command> [flags]

commands:
  smoke    create a table, commit once, read it back
  append   concurrent appends, then check no acknowledged append was lost
  skew     conflicting multi-table transactions, then check at most one commits
  run      generate a seeded fault plan (--plan-only while controllers are pending)

exit status: 0 success, 1 error, 3 invalid, 4 inconclusive (skew only)
run "calve <command> -h" for flags`)
	os.Exit(2)
}

type common struct {
	catalog   string
	warehouse string
	token     string
	namespace string
	timeout   time.Duration
}

func commonFlags(fs *flag.FlagSet) *common {
	c := &common{}
	fs.StringVar(&c.catalog, "catalog", "http://localhost:8181", "catalog base URL (without /v1)")
	fs.StringVar(&c.warehouse, "warehouse", "", "warehouse passed to /v1/config, if the catalog needs one")
	fs.StringVar(&c.token, "token", os.Getenv("CALVE_TOKEN"), "bearer token (default $CALVE_TOKEN)")
	fs.StringVar(&c.namespace, "namespace", "calve", "namespace to create test tables in")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "per-request timeout")
	return c
}

func (c *common) client(ctx context.Context) *rest.Client {
	client := rest.NewClient(c.catalog, c.warehouse, c.token, c.timeout)
	if _, err := client.Config(ctx); err != nil {
		fail("%v", err)
	}
	return client
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "calve: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "smoke":
		smoke(os.Args[2:])
	case "append":
		appendCmd(os.Args[2:])
	case "skew":
		skewCmd(os.Args[2:])
	case "run":
		runCmd(os.Args[2:])
	default:
		usage()
	}
}

func smoke(args []string) {
	fs := flag.NewFlagSet("smoke", flag.ExitOnError)
	cf := commonFlags(fs)
	fs.Parse(args)

	ctx := context.Background()
	c := cf.client(ctx)
	table := fmt.Sprintf("smoke_%d", time.Now().Unix())
	if err := workload.SetupTable(ctx, c, cf.namespace, table); err != nil {
		fail("%v", err)
	}
	change := rest.TableChange{
		Requirements: []map[string]any{},
		Updates: []map[string]any{{
			"action": "set-properties", "updates": map[string]string{"calve.smoke": "ok"},
		}},
	}
	if r := c.CommitTable(ctx, cf.namespace, table, change); r.Outcome() != "ok" {
		fail("commit: %s", rest.Describe(r))
	}
	meta, err := rest.ParseMetadata(c.LoadTable(ctx, cf.namespace, table))
	if err != nil {
		fail("load: %v", err)
	}
	if meta.Properties["calve.smoke"] != "ok" {
		fail("commit was acknowledged but the property is missing on reload")
	}
	c.DropTable(ctx, cf.namespace, table)
	fmt.Printf("smoke ok: %s (table %s.%s, uuid %s)\n", cf.catalog, cf.namespace, table, meta.TableUUID)
}

func appendCmd(args []string) {
	fs := flag.NewFlagSet("append", flag.ExitOnError)
	cf := commonFlags(fs)
	clients := fs.Int("clients", 8, "concurrent clients")
	perClient := fs.Int("per-client", 50, "appends per client")
	out := fs.String("out", "history.jsonl", "where to save the history")
	readback := readbackFlags(fs)
	fs.Parse(args)

	ctx := context.Background()
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	cfg := workload.AppendConfig{
		Namespace: cf.namespace,
		Table:     fmt.Sprintf("append_%d", time.Now().UnixNano()),
		Clients:   *clients,
		PerClient: *perClient,
	}
	res, elapsed, err := executeAppend(ctx, c, cfg, *out, *readback)
	if err != nil {
		fail("%v", err)
	}

	fmt.Printf("append on %s.%s: %d ok, %d failed, %d unknown (%d resolved as committed) in %s\n",
		cfg.Namespace, cfg.Table, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, elapsed.Round(time.Millisecond))
	fmt.Printf("history saved to %s\n", *out)
	if res.Valid {
		fmt.Println("VALID: all checked appends resolved; no lost or phantom properties")
		return
	}
	if res.Verdict == checker.Inconclusive {
		for _, reason := range res.Reasons {
			fmt.Println("  " + reason)
		}
		fmt.Printf("INCONCLUSIVE: append outcomes or final observations could not be decided (%d unknown appends unresolved)\n", res.Unresolved)
		os.Exit(4)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(res.Anomalies)
	fmt.Printf("INVALID: %d anomalies\n", len(res.Anomalies))
	os.Exit(3)
}

// executeAppend returns errors before the CLI calls os.Exit, allowing the saved
// evidence to include failed setup/baseline/final reads as well as commits.
func executeAppend(ctx context.Context, c *rest.Client, cfg workload.AppendConfig, out string, readback readbackConfig) (result checker.AppendResult, elapsed time.Duration, err error) {
	rec := history.NewRecorder()
	c = c.WithRecorder(rec)
	defer func() {
		if saveErr := history.SaveFile(out, rec.Ops()); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save history: %w", saveErr))
		}
	}()
	manifest, _ := json.Marshal(map[string]any{
		"run_id": cfg.Table, "started_at": time.Now().UTC(), "workload": "append",
		"catalog":   history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		"warehouse": history.RedactText(c.Warehouse, "", c.Token),
		"namespace": cfg.Namespace, "table": cfg.Table,
		"clients": cfg.Clients, "per_client": cfg.PerClient, "request_timeout_ns": c.HTTP.Timeout.Nanoseconds(),
		"readback_timeout_ns": readback.Timeout.Nanoseconds(), "readback_interval_ns": readback.Interval.Nanoseconds(),
	})
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: manifest})
	if err := readback.validate(); err != nil {
		return result, 0, err
	}
	if cfg.Clients <= 0 || cfg.PerClient <= 0 {
		return result, 0, fmt.Errorf("clients and per-client must be positive")
	}
	setup := history.WithOperation(ctx, history.Op{Process: -1, Phase: "setup"})
	if _, err = c.Config(setup); err != nil {
		return result, 0, err
	}
	if err = workload.SetupTable(setup, c, cfg.Namespace, cfg.Table); err != nil {
		return result, 0, err
	}
	baseline := history.WithOperation(ctx, history.Op{Process: -1, Phase: "baseline"})
	base, err := rest.ParseMetadata(c.LoadTable(baseline, cfg.Namespace, cfg.Table))
	if err != nil {
		return result, 0, fmt.Errorf("baseline read: %w", err)
	}
	if base.TableUUID == "" {
		return result, 0, fmt.Errorf("baseline read: missing table UUID")
	}
	start := time.Now()
	workload.RunAppend(ctx, c, cfg, rec)
	elapsed = time.Since(start)
	finalCtx := history.WithOperation(ctx, history.Op{Process: -1, Phase: "final"})
	final, finalErr := rest.ParseMetadata(c.LoadTable(finalCtx, cfg.Namespace, cfg.Table))
	if finalErr == nil && final.TableUUID != base.TableUUID {
		return result, elapsed, fmt.Errorf("final read: table UUID changed; run is inconclusive")
	}
	result, err = checker.CheckAppendHistory(rec.Ops())
	if finalErr != nil && (result.Unknown == 0 || readback.Timeout == 0) {
		return result, elapsed, fmt.Errorf("final read: %w", finalErr)
	}
	if err != nil || result.Unresolved == 0 && finalErr == nil {
		return result, elapsed, err
	}
	targets := readbackTargets(rec.Ops(), map[string]bool{"": true})
	err = readBack(ctx, c, readback, targets, func() (bool, error) {
		var checkErr error
		result, checkErr = checker.CheckAppendHistory(rec.Ops())
		return result.Verdict == checker.Valid || result.Verdict == checker.Invalid, checkErr
	})
	return result, elapsed, err
}

func skewCmd(args []string) {
	fs := flag.NewFlagSet("skew", flag.ExitOnError)
	cf := commonFlags(fs)
	rounds := fs.Int("rounds", 10, "concurrent rounds; two sequential controls always follow")
	out := fs.String("out", "history-skew.jsonl", "where to save the history")
	readback := readbackFlags(fs)
	fs.Parse(args)

	ctx := context.Background()
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	cfg := workload.SkewConfig{Namespace: cf.namespace, Rounds: *rounds}
	res, err := executeSkew(ctx, c, cfg, *out, *readback)
	if err != nil {
		fail("%v", err)
	}

	fmt.Println(skewSummary(cf.catalog, cf.token, res))
	fmt.Printf("history saved to %s\n", *out)
	switch res.Verdict {
	case checker.SkewValid:
		fmt.Println("VALID: at most one transaction committed in every round; stale controls were rejected")
	case checker.SkewInvalid:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(res.Anomalies)
		fmt.Printf("INVALID: %s\n", skewKinds(res.Anomalies))
		if hasIsolationLead(res.Anomalies) {
			fmt.Println("write-skew is an isolation lead under the serializable model, not a confirmed catalog bug")
		}
		os.Exit(3)
	default:
		for _, reason := range skewReasons(res) {
			fmt.Println("  " + reason)
		}
		fmt.Printf("INCONCLUSIVE: %d of %d rounds could not be decided\n", res.InconclusiveRounds, len(res.Rounds))
		os.Exit(4)
	}
}

// executeSkew saves the history even when the run fails partway, so the
// evidence for failed setup, reads or transactions is never lost.
func executeSkew(ctx context.Context, c *rest.Client, cfg workload.SkewConfig, out string, readback readbackConfig) (result checker.SkewResult, err error) {
	rec := history.NewRecorder()
	defer func() {
		if saveErr := history.SaveFile(out, rec.Ops()); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save history: %w", saveErr))
		}
	}()
	if err := readback.validate(); err != nil {
		return result, err
	}
	if _, err = workload.RunSkew(ctx, c, cfg, rec); err != nil {
		return result, err
	}
	result, err = checker.CheckSkew(rec.Ops())
	if err != nil || result.Unknown == result.ResolvedCommitted {
		return result, err
	}
	rounds := map[string]bool{}
	for _, round := range result.Rounds {
		unresolved := false
		for _, txn := range round.Transactions {
			unresolved = unresolved || txn.CommitState == "unknown"
		}
		if unresolved {
			rounds[round.Round] = true
		}
	}
	targets := readbackTargets(rec.Ops(), rounds)
	config, _ := json.Marshal(map[string]int64{"timeout_ns": readback.Timeout.Nanoseconds(), "interval_ns": readback.Interval.Nanoseconds()})
	rec.Record(history.Op{Type: history.Meta, Process: -1, Phase: "recovery", F: "readback-config", Value: config})
	c = c.WithRecorder(rec)
	err = readBack(ctx, c, readback, targets, func() (bool, error) {
		var checkErr error
		result, checkErr = checker.CheckSkew(rec.Ops())
		return result.Unknown == result.ResolvedCommitted || result.Verdict == checker.Invalid, checkErr
	})
	return result, err
}

// skewSummary redacts the catalog URL and token the same way the history does,
// since stdout often ends up in CI logs.
func skewSummary(catalog, token string, res checker.SkewResult) string {
	return fmt.Sprintf("skew on %s: %d rounds, %d ok, %d rejected, %d unknown (%d resolved as committed)",
		history.RedactText(history.SafeURL(catalog), "", token), len(res.Rounds), res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
}

// hasIsolationLead reports whether any finding rests on the serializable model
// rather than a direct catalog-contract violation.
func hasIsolationLead(anomalies []checker.SkewAnomaly) bool {
	for _, a := range anomalies {
		if a.Basis == "serializable-model" {
			return true
		}
	}
	return false
}

// skewKinds counts findings by kind, e.g. "10 write-skew, 1 lost".
func skewKinds(anomalies []checker.SkewAnomaly) string {
	counts := map[string]int{}
	var order []string
	for _, a := range anomalies {
		if counts[a.Kind] == 0 {
			order = append(order, a.Kind)
		}
		counts[a.Kind]++
	}
	parts := make([]string, len(order))
	for i, kind := range order {
		parts[i] = fmt.Sprintf("%d %s", counts[kind], kind)
	}
	return strings.Join(parts, ", ")
}

// skewReasons lists each distinct reason a round or the run was inconclusive.
func skewReasons(res checker.SkewResult) []string {
	counts := map[string]int{}
	var order []string
	add := func(reason string) {
		if counts[reason] == 0 {
			order = append(order, reason)
		}
		counts[reason]++
	}
	for _, reason := range res.Reasons {
		add(reason)
	}
	for _, round := range res.Rounds {
		for _, reason := range round.Reasons {
			add(reason)
		}
	}
	out := make([]string, len(order))
	for i, reason := range order {
		out[i] = fmt.Sprintf("%dx %s", counts[reason], reason)
	}
	return out
}
