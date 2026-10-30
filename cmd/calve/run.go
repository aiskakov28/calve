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
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/storage"
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
	path := fs.String("plan", "fault-plan.json", "new file for the plan with --plan-only")
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
	workloadName := fs.String("workload", "append", "workload under the plan: append, schema, maintenance or snapshot")
	model := fs.String("model", "", "maintenance or snapshot model (default: the workload's retained-history model)")
	snapshotClients := fs.Int("snapshot-clients", 2, "snapshot: concurrent contenders per round")
	expireInterval := fs.Duration("expire-interval", 10*time.Millisecond, "maintenance: pause between expiration passes")
	manifestRoot := fs.String("manifest-root", "", "maintenance and snapshot: manifest storage root (default: the fresh table's location)")
	s3Endpoint := fs.String("s3-endpoint", "", "maintenance and snapshot: path-style S3 endpoint for manifest uploads")
	s3Region := fs.String("s3-region", "us-east-1", "maintenance and snapshot: S3 signing region")
	clients := fs.Int("clients", 4, "concurrent append clients, or schema contenders per round")
	perClient := fs.Int("per-client", 0, "appends per client; 0 spans --duration at --interval")
	interval := fs.Duration("interval", 250*time.Millisecond, "pause between one client's appends")
	outDir := fs.String("out-dir", "", "new directory for the run's plan, histories and result (default runs/<run ID>)")
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
			// Bounded like a controller action: a hung API must not hang the run.
			listenCtx, cancel := context.WithTimeout(ctx, plan.Config.ActionTimeout)
			listen, err := toxiproxy.Listen(listenCtx, target)
			cancel()
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
		plan: plan, controllers: controllers,
		probe:        rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout),
		readyTimeout: *readyTimeout,
	}
	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	if *outDir == "" {
		*outDir = filepath.Join("runs", runID)
	}
	c := rest.NewClient(cf.catalog, cf.warehouse, cf.token, cf.timeout)
	switch *workloadName {
	case "append":
		appendCfg := workload.AppendConfig{Namespace: cf.namespace, Table: runID, Clients: *clients, PerClient: *perClient, Interval: *interval}
		res, elapsed, err := executeAppendCampaign(ctx, c, appendCfg, *readback, camp, *outDir, target)
		if err != nil {
			return 0, fmt.Errorf("%w (run directory %s)", err, *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s: %d faults applied and healed; %d ok, %d failed, %d unknown (%d resolved as committed) in %s\n",
			plan.Seed, target, camp.result.Healed, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted, elapsed.Round(time.Millisecond))
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["append"], res, res.Verdict, camp.verdict, camp.coverage), nil
	case "schema":
		// Rounds only cap the history: no round takes under a millisecond, so
		// the cap cannot be reached before the plan's horizon.
		schemaCfg := workload.SchemaConfig{Namespace: cf.namespace, Clients: *clients, Rounds: int(plan.Config.RunDuration / time.Millisecond)}
		res, err := executeSchemaCampaign(ctx, c, schemaCfg, *readback, camp, *outDir, target, runID)
		if err != nil {
			return 0, fmt.Errorf("%w (run directory %s)", err, *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s (schema): %d faults applied and healed; %d rounds, %d with a winner\n",
			plan.Seed, target, camp.result.Healed, res.Rounds, res.RoundsWithWinner)
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["schema-cas"], res, res.Verdict, camp.verdict, camp.coverage), nil
	case "maintenance", "snapshot":
		store := storage.ManifestStore{Endpoint: *s3Endpoint, Region: *s3Region, AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN"), HTTP: c.HTTP}
		if err := store.Validate(); err != nil {
			return 0, err
		}
		if *workloadName == "snapshot" {
			// Rounds only cap the history, as for schema.
			snapshotCfg := workload.SnapshotConfig{Namespace: cf.namespace, Clients: *snapshotClients,
				Rounds: int(plan.Config.RunDuration / time.Millisecond), ManifestRoot: *manifestRoot, Model: *model}
			res, err := executeSnapshotCampaign(ctx, c, snapshotCfg, store, *readback, camp, *outDir, target, runID)
			if err != nil {
				return 0, fmt.Errorf("%s (run directory %s)", history.RedactText(err.Error(), cf.catalog, cf.token, store.SecretKey, store.SessionToken), *outDir)
			}
			fmt.Fprintf(out, "run seed %d on %s (snapshot): %d faults applied and healed; %d ok, %d rejected, %d unknown (%d resolved as committed)\n",
				plan.Seed, target, camp.result.Healed, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
			fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
			return reportCampaign(out, campaignCheckers["snapshot-cas"], res, res.Verdict, camp.verdict, camp.coverage), nil
		}
		// Appends only cap the history: none takes under a millisecond, so the
		// cap cannot be reached before the plan's horizon.
		maintenanceCfg := workload.MaintenanceConfig{Namespace: cf.namespace, Appends: int(plan.Config.RunDuration / time.Millisecond),
			ExpireInterval: *expireInterval, ManifestRoot: *manifestRoot, Model: *model}
		res, err := executeMaintenanceCampaign(ctx, c, maintenanceCfg, store, *readback, camp, *outDir, target, runID)
		if err != nil {
			return 0, fmt.Errorf("%s (run directory %s)", history.RedactText(err.Error(), cf.catalog, cf.token, store.SecretKey, store.SessionToken), *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s (maintenance): %d faults applied and healed; %d appends, %d ok; %d expirations acknowledged\n",
			plan.Seed, target, camp.result.Healed, res.Appends, res.AppendsOK, res.ExpirationsOK)
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["maintenance-race"], res, res.Verdict, camp.verdict, camp.coverage), nil
	default:
		return 0, fmt.Errorf("unsupported --workload %q: use append, schema, maintenance or snapshot", *workloadName)
	}
}
