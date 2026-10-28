package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func runCmd(args []string) {
	code, err := runCampaign(args, os.Stdout)
	if err != nil {
		fail("%v", err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

// runCampaign generates a seeded fault plan. With --plan-only it only saves the
// plan; otherwise it runs the append workload under the plan against one
// catalog and returns the checker's exit status.
func runCampaign(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(out)
	cf := commonFlags(fs)
	cfg := fault.DefaultConfig()
	seed := fs.Int64("seed", 0, "fault schedule seed")
	planOnly := fs.Bool("plan-only", false, "generate the complete plan without running workloads or faults")
	path := fs.String("plan", "fault-plan.json", "new file for the generated plan")
	targets := fs.String("targets", strings.Join(cfg.Targets, ","), "comma-separated logical catalog target IDs; a campaign takes one")
	kinds := fs.String("faults", "latency,reset,partition,kill,pause,restart", "comma-separated fault kinds")
	fs.DurationVar(&cfg.RunDuration, "duration", cfg.RunDuration, "planned workload duration")
	fs.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "initial healthy warmup")
	fs.DurationVar(&cfg.MinHealthy, "min-healthy", cfg.MinHealthy, "minimum healthy gap before a fault")
	fs.DurationVar(&cfg.MaxHealthy, "max-healthy", cfg.MaxHealthy, "maximum healthy gap before a fault")
	fs.DurationVar(&cfg.MinFault, "min-fault", cfg.MinFault, "minimum fault window")
	fs.DurationVar(&cfg.MaxFault, "max-fault", cfg.MaxFault, "maximum fault window")
	fs.DurationVar(&cfg.ActionTimeout, "action-timeout", cfg.ActionTimeout, "control action and healing budget")
	fs.DurationVar(&cfg.CleanupTimeout, "cleanup-timeout", cfg.CleanupTimeout, "total independent cleanup budget")
	fs.DurationVar(&cfg.Latency, "latency", cfg.Latency, "delay for latency faults; jitter is zero")
	fs.DurationVar(&cfg.StopGrace, "stop-grace", cfg.StopGrace, "SIGTERM grace for controlled stop/start")
	clients := fs.Int("clients", 4, "concurrent append clients")
	perClient := fs.Int("per-client", 0, "appends per client; 0 spans --duration at --interval")
	interval := fs.Duration("interval", 250*time.Millisecond, "pause between one client's appends")
	historyOut := fs.String("out", "history-run.jsonl", "where to save the workload history")
	faultsOut := fs.String("fault-history", "history-faults.jsonl", "new file for the fault controller history")
	api := fs.String("toxiproxy", "http://localhost:8474", "Toxiproxy API")
	proxy := fs.String("proxy", "", "Toxiproxy proxy for the target (default: its docker-compose proxy)")
	container := fs.String("container", "", "container for the target (default: its docker-compose container)")
	readyTimeout := fs.Duration("ready-timeout", time.Minute, "how long to wait for the catalog to answer after healing")
	readback := readbackFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, nil
		}
		return 0, err
	}
	if fs.NArg() != 0 {
		return 0, fmt.Errorf("unexpected run arguments: %s", strings.Join(fs.Args(), " "))
	}
	cfg.Targets = strings.Split(*targets, ",")
	for i := range cfg.Targets {
		cfg.Targets[i] = strings.TrimSpace(cfg.Targets[i])
	}
	cfg.Kinds = nil
	for _, kind := range strings.Split(*kinds, ",") {
		cfg.Kinds = append(cfg.Kinds, fault.Kind(strings.TrimSpace(kind)))
	}
	plan, err := fault.Generate(*seed, cfg)
	if err != nil {
		return 0, err
	}
	if *planOnly {
		if err := fault.Save(*path, plan); err != nil {
			return 0, fmt.Errorf("save fault plan: %w", err)
		}
		_, err = fmt.Fprintf(out, "PLAN ONLY: saved %d faults for seed %d to %s (%s)\n", len(plan.Events), plan.Seed, *path, plan.Version)
		return 0, err
	}

	if len(plan.Config.Targets) != 1 {
		return 0, errors.New("a campaign runs against one catalog; pass a single --targets")
	}
	// Execute refuses existing artifacts too, but only after setup has touched
	// the catalog; refuse them before anything runs.
	for _, p := range []string{*path, *faultsOut} {
		if _, err := os.Stat(p); err == nil {
			return 0, fmt.Errorf("%s already exists; choose a new path", p)
		}
	}
	// The workload history is written last and truncates its file, so it
	// must not share a path with the plan or fault history.
	seen := map[string]string{}
	for _, p := range []string{*path, *faultsOut, *historyOut} {
		abs, err := filepath.Abs(p)
		if err != nil {
			return 0, fmt.Errorf("artifact path %s: %w", p, err)
		}
		if other, dup := seen[abs]; dup {
			return 0, fmt.Errorf("%s and %s name the same file; each artifact needs its own path", other, p)
		}
		seen[abs] = p
	}
	target := plan.Config.Targets[0]
	defaults, known := composeTargets[target]
	if *proxy == "" {
		*proxy = defaults.proxy
	}
	if *container == "" {
		*container = defaults.container
	}
	if !known && (*proxy == "" || *container == "") {
		return 0, fmt.Errorf("target %q is not a docker-compose catalog; pass --proxy and --container", target)
	}
	if *perClient == 0 {
		if *interval <= 0 {
			return 0, errors.New("--per-client 0 needs a positive --interval")
		}
		*perClient = int(plan.Config.RunDuration / *interval)
	}

	ctx := context.Background()
	controllers, toxiproxy := faultControllers(target, *proxy, *container, *api)
	for _, event := range plan.Events {
		if event.Kind == fault.Latency || event.Kind == fault.Reset || event.Kind == fault.Partition {
			listen, err := toxiproxy.Listen(ctx, target)
			if err != nil {
				return 0, err
			}
			if err := checkProxied(cf.catalog, listen); err != nil {
				return 0, err
			}
			break
		}
	}
	camp := &campaign{
		plan: plan, controllers: controllers, planPath: *path, faultsPath: *faultsOut,
		probe:        rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout),
		readyTimeout: *readyTimeout,
	}
	appendCfg := workload.AppendConfig{
		Namespace: cf.namespace,
		Table:     fmt.Sprintf("run_%d", time.Now().UnixNano()),
		Clients:   *clients,
		PerClient: *perClient,
		Interval:  *interval,
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	res, elapsed, err := executeAppendWith(ctx, c, appendCfg, *historyOut, *readback, camp)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(out, "run seed %d on %s: %d faults applied and healed; %d ok, %d failed, %d unknown (%d resolved as committed) in %s\n",
		plan.Seed, target, camp.result.Healed, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, elapsed.Round(time.Millisecond))
	fmt.Fprintf(out, "plan %s, fault history %s, workload history %s\n", *path, *faultsOut, *historyOut)
	return reportAppend(out, res), nil
}
