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
	ctx, stop := interruptible(os.Stderr)
	code, err := runCampaign(ctx, args, os.Stdout)
	if interrupted := stop(); err != nil && interrupted {
		fail("interrupted: %v", err)
	}
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
//
// Cancelling ctx stops the load and heals every fault, then saves the run
// directory with an error verdict.
func runCampaign(ctx context.Context, args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(out)
	f := defineRunFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, nil
		}
		return 0, err
	}
	// The flags are parsed, so cfg holds their values.
	cf, cfg, seed, planOnly, path, targets, kinds, workloadName, atomicTables, atomicReaders, readInterval, model, snapshotClients, expireInterval, manifestRoot, s3Endpoint, s3Region, clients, perClient, interval, outDir, api, proxy, container, readyTimeout, readback :=
		f.cf, f.cfg, f.seed, f.planOnly, f.path, f.targets, f.kinds, f.workloadName, f.atomicTables, f.atomicReaders, f.readInterval, f.model, f.snapshotClients, f.expireInterval, f.manifestRoot, f.s3Endpoint, f.s3Region, f.clients, f.perClient, f.interval, f.outDir, f.api, f.proxy, f.container, f.readyTimeout, f.readback
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
	if !known && (*proxy == "" || *container == "") {
		return 0, fmt.Errorf("target %q is not a docker-compose catalog; pass --proxy and --container", target)
	}
	if *proxy == "" {
		*proxy = defaults.proxy
	}
	// Refuse an earlier run's directory before calling docker or Toxiproxy.
	// executeCampaign's exclusive create still guards against a race.
	if *outDir != "" {
		if err := refuseExisting(*outDir, "--out-dir"); err != nil {
			return 0, fmt.Errorf("run directory: %w", err)
		}
	}
	// Only process faults need the container, so a network-only run never
	// calls docker.
	if *container == "" && hasProcessFault(plan) {
		// Bounded like a controller action: a hung docker must not hang the run.
		findCtx, cancel := context.WithTimeout(ctx, plan.Config.ActionTimeout)
		id, err := composeContainer(findCtx, defaults.service, dockerCLI)
		cancel()
		if err != nil {
			return 0, err
		}
		*container = id
	}
	if *perClient == 0 {
		if *interval <= 0 {
			return 0, errors.New("--per-client 0 needs a positive --interval")
		}
		*perClient = int(plan.Config.RunDuration / *interval)
	}

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
	// Best effort, bounded like a controller action: a missing docker is
	// recorded in provenance.json, not a reason to stop.
	prov := provenance{Calve: readBuild(), Target: target, StartedAt: time.Now().UTC()}
	// The local Compose service is this catalog only if the URL is local: a
	// remote catalog must not be recorded with a local stack's image.
	service := defaults.service
	if !localCatalog(cf.catalog) {
		service = ""
	}
	inspectCtx, cancelInspect := context.WithTimeout(ctx, plan.Config.ActionTimeout)
	prov.Catalog = inspectCatalog(inspectCtx, *container, service, dockerCLI)
	cancelInspect()
	camp := &campaign{
		plan: plan, controllers: controllers,
		provenance:   prov,
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
		// Appends only bound history size: an append can answer in under a
		// millisecond, so the cap allows one per microsecond and the plan's
		// horizon ends the run.
		maintenanceCfg := workload.MaintenanceConfig{Namespace: cf.namespace, Appends: int(plan.Config.RunDuration / time.Microsecond),
			ExpireInterval: *expireInterval, ManifestRoot: *manifestRoot, Model: *model}
		res, err := executeMaintenanceCampaign(ctx, c, maintenanceCfg, store, *readback, camp, *outDir, target, runID)
		if err != nil {
			return 0, fmt.Errorf("%s (run directory %s)", history.RedactText(err.Error(), cf.catalog, cf.token, store.SecretKey, store.SessionToken), *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s (maintenance): %d faults applied and healed; %d appends, %d ok; %d expirations acknowledged\n",
			plan.Seed, target, camp.result.Healed, res.Appends, res.AppendsOK, res.ExpirationsOK)
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["maintenance-race"], res, res.Verdict, camp.verdict, camp.coverage), nil
	case "atomic":
		// PerClient only bounds history size: a rejected transaction can answer in
		// under a millisecond, so the cap allows one per microsecond and the
		// plan's horizon ends the run.
		atomicCfg := workload.AtomicConfig{Namespace: cf.namespace, Tables: *atomicTables, Clients: *clients,
			PerClient: int(plan.Config.RunDuration / time.Microsecond), Readers: *atomicReaders, ReadInterval: *readInterval}
		res, err := executeAtomicCampaign(ctx, c, atomicCfg, *readback, camp, *outDir, target, runID)
		if err != nil {
			return 0, fmt.Errorf("%w (run directory %s)", err, *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s (atomic): %d faults applied and healed; %d ok, %d rejected, %d unknown (%d resolved as committed)\n",
			plan.Seed, target, camp.result.Healed, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["atomic"], res, res.Verdict, camp.verdict, camp.coverage), nil
	case "skew":
		// Every round is set up before the plan starts; rounds start --interval
		// apart, so they span the plan's duration.
		if *interval <= 0 || plan.Config.RunDuration < *interval {
			return 0, errors.New("skew needs an --interval no longer than --duration")
		}
		skewCfg := workload.SkewConfig{Namespace: cf.namespace, Rounds: int(plan.Config.RunDuration / *interval), Interval: *interval}
		res, err := executeSkewCampaign(ctx, c, skewCfg, *readback, camp, *outDir, target, runID)
		if err != nil {
			return 0, fmt.Errorf("%w (run directory %s)", err, *outDir)
		}
		fmt.Fprintf(out, "run seed %d on %s (skew): %d faults applied and healed; %d rounds checked, %d not started; %d ok, %d rejected, %d unknown (%d resolved as committed)\n",
			plan.Seed, target, camp.result.Healed, len(res.Rounds), res.NotStarted, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
		fmt.Fprintf(out, "run saved to %s; re-check with: calve check %s\n", *outDir, *outDir)
		return reportCampaign(out, campaignCheckers["skew"], res, res.Verdict, camp.verdict, camp.coverage), nil
	default:
		return 0, fmt.Errorf("unsupported --workload %q: use append, schema, maintenance, snapshot, atomic or skew", *workloadName)
	}
}

// runFlags are calve run's flags. calve shrink parses them too, to read a
// run's settings before it reruns the run with smaller ones.
type runFlags struct {
	cf              *common
	cfg             fault.Config
	seed            *int64
	planOnly        *bool
	path            *string
	targets         *string
	kinds           *string
	workloadName    *string
	atomicTables    *int
	atomicReaders   *int
	readInterval    *time.Duration
	model           *string
	snapshotClients *int
	expireInterval  *time.Duration
	manifestRoot    *string
	s3Endpoint      *string
	s3Region        *string
	clients         *int
	perClient       *int
	interval        *time.Duration
	outDir          *string
	api             *string
	proxy           *string
	container       *string
	readyTimeout    *time.Duration
	readback        *readbackConfig
}

func defineRunFlags(fs *flag.FlagSet) *runFlags {
	f := &runFlags{cfg: fault.DefaultConfig()}
	f.cf = commonFlags(fs)
	f.seed = fs.Int64("seed", 0, "fault schedule seed")
	f.planOnly = fs.Bool("plan-only", false, "generate the complete plan without running workloads or faults")
	f.path = fs.String("plan", "fault-plan.json", "new file for the plan with --plan-only")
	f.targets = fs.String("targets", strings.Join(f.cfg.Targets, ","), "comma-separated logical catalog target IDs; a campaign takes one")
	f.kinds = fs.String("faults", "latency,reset,partition,kill,pause,restart", "comma-separated fault kinds")
	fs.DurationVar(&f.cfg.RunDuration, "duration", f.cfg.RunDuration, "planned workload duration")
	fs.DurationVar(&f.cfg.Warmup, "warmup", f.cfg.Warmup, "initial healthy warmup")
	fs.DurationVar(&f.cfg.MinHealthy, "min-healthy", f.cfg.MinHealthy, "minimum healthy gap before a fault")
	fs.DurationVar(&f.cfg.MaxHealthy, "max-healthy", f.cfg.MaxHealthy, "maximum healthy gap before a fault")
	fs.DurationVar(&f.cfg.MinFault, "min-fault", f.cfg.MinFault, "minimum fault window")
	fs.DurationVar(&f.cfg.MaxFault, "max-fault", f.cfg.MaxFault, "maximum fault window")
	fs.DurationVar(&f.cfg.ActionTimeout, "action-timeout", f.cfg.ActionTimeout, "control action and healing budget")
	fs.DurationVar(&f.cfg.CleanupTimeout, "cleanup-timeout", f.cfg.CleanupTimeout, "total independent cleanup budget")
	fs.DurationVar(&f.cfg.Latency, "latency", f.cfg.Latency, "delay for latency faults; jitter is zero")
	fs.DurationVar(&f.cfg.StopGrace, "stop-grace", f.cfg.StopGrace, "SIGTERM grace for controlled stop/start")
	f.workloadName = fs.String("workload", "append", "workload under the plan: append, schema, maintenance, snapshot, atomic or skew")
	f.atomicTables = fs.Int("tables", 2, "atomic: tables updated by every transaction (2..32)")
	f.atomicReaders = fs.Int("readers", 1, "atomic: concurrent table readers")
	f.readInterval = fs.Duration("read-interval", 10*time.Millisecond, "atomic: pause between each reader's passes")
	f.model = fs.String("model", "", "maintenance or snapshot model (default: the workload's retained-history model)")
	f.snapshotClients = fs.Int("snapshot-clients", 2, "snapshot: concurrent contenders per round")
	f.expireInterval = fs.Duration("expire-interval", 10*time.Millisecond, "maintenance: pause between expiration passes")
	f.manifestRoot = fs.String("manifest-root", "", "maintenance and snapshot: manifest storage root (default: the fresh table's location)")
	f.s3Endpoint = fs.String("s3-endpoint", "", "maintenance and snapshot: path-style S3 endpoint for manifest uploads")
	f.s3Region = fs.String("s3-region", "us-east-1", "maintenance and snapshot: S3 signing region")
	f.clients = fs.Int("clients", 4, "concurrent append clients, or schema contenders per round")
	f.perClient = fs.Int("per-client", 0, "appends per client; 0 spans --duration at --interval")
	f.interval = fs.Duration("interval", 250*time.Millisecond, "pause between one client's appends, or between skew rounds")
	f.outDir = fs.String("out-dir", "", "new directory for the run's plan, histories and result (default runs/<run ID>)")
	f.api = fs.String("toxiproxy", "http://localhost:8474", "Toxiproxy API")
	f.proxy = fs.String("proxy", "", "Toxiproxy proxy for the target (default: its docker-compose proxy)")
	f.container = fs.String("container", "", "container name or ID for process faults (default: the target's service from docker compose ps)")
	f.readyTimeout = fs.Duration("ready-timeout", time.Minute, "how long to wait for the catalog to answer after healing")
	f.readback = readbackFlags(fs)
	return f
}
