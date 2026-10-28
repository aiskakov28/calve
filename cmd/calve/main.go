// Command calve runs correctness workloads against Iceberg REST catalogs.
//
//	calve smoke  --catalog http://localhost:8181
//	calve append --catalog http://localhost:8181 --clients 8 --per-client 50
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
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
	fs.Parse(args)

	ctx := context.Background()
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	cfg := workload.AppendConfig{
		Namespace: cf.namespace,
		Table:     fmt.Sprintf("append_%d", time.Now().UnixNano()),
		Clients:   *clients,
		PerClient: *perClient,
	}
	res, elapsed, err := executeAppend(ctx, c, cfg, *out)
	if err != nil {
		fail("%v", err)
	}

	fmt.Printf("append on %s.%s: %d ok, %d failed, %d unknown in %s\n",
		cfg.Namespace, cfg.Table, res.OK, res.Failed, res.Unknown, elapsed.Round(time.Millisecond))
	fmt.Printf("history saved to %s\n", *out)
	if res.Valid {
		fmt.Println("VALID: no lost, duplicated or phantom appends")
		return
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(res.Anomalies)
	fmt.Printf("INVALID: %d anomalies\n", len(res.Anomalies))
	os.Exit(3)
}

// executeAppend returns errors before the CLI calls os.Exit, allowing the saved
// evidence to include failed setup/baseline/final reads as well as commits.
func executeAppend(ctx context.Context, c *rest.Client, cfg workload.AppendConfig, out string) (result checker.AppendResult, elapsed time.Duration, err error) {
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
	})
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: manifest})
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
	final, err := rest.ParseMetadata(c.LoadTable(finalCtx, cfg.Namespace, cfg.Table))
	if err != nil {
		return result, elapsed, fmt.Errorf("final read: %w", err)
	}
	if final.TableUUID != base.TableUUID {
		return result, elapsed, fmt.Errorf("final read: table UUID changed; run is inconclusive")
	}
	result, err = checker.CheckAppend(rec.Ops(), workload.AppendKeys(final))
	return result, elapsed, err
}
