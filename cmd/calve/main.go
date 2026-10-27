// Command calve runs correctness workloads against Iceberg REST catalogs.
//
//	calve smoke  --catalog http://localhost:8181
//	calve append --catalog http://localhost:8181 --clients 8 --per-client 50
package main

import (
	"context"
	"encoding/json"
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
	c := cf.client(ctx)
	cfg := workload.AppendConfig{
		Namespace: cf.namespace,
		Table:     fmt.Sprintf("append_%d", time.Now().Unix()),
		Clients:   *clients,
		PerClient: *perClient,
	}
	if err := workload.SetupTable(ctx, c, cfg.Namespace, cfg.Table); err != nil {
		fail("%v", err)
	}

	rec := history.NewRecorder()
	start := time.Now()
	workload.RunAppend(ctx, c, cfg, rec)
	elapsed := time.Since(start)

	final, err := workload.FinalAppendKeys(ctx, c, cfg.Namespace, cfg.Table)
	if err != nil {
		fail("%v", err)
	}
	ops := rec.Ops()
	if err := history.SaveFile(*out, ops); err != nil {
		fail("save history: %v", err)
	}
	res, err := checker.CheckAppend(ops, final)
	if err != nil {
		fail("check: %v", err)
	}

	fmt.Printf("append on %s.%s: %d ok, %d failed, %d unknown in %s\n",
		cfg.Namespace, cfg.Table, res.OK, res.Failed, res.Unknown, elapsed.Round(time.Millisecond))
	fmt.Printf("history saved to %s (%d records)\n", *out, len(ops))
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
