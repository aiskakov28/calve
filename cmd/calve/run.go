package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aiskakov28/calve/internal/fault"
)

func runCmd(args []string) {
	if err := runPlan(args, os.Stdout); err != nil {
		fail("%v", err)
	}
}

// runPlan is intentionally plan-only until issues #13/#14 supply controllers.
// It does not fabricate a fault campaign or a checker verdict.
func runPlan(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(out)
	cfg := fault.DefaultConfig()
	seed := fs.Int64("seed", 0, "fault schedule seed")
	planOnly := fs.Bool("plan-only", false, "generate the complete plan without running workloads or faults")
	path := fs.String("plan", "fault-plan.json", "new file for the generated plan")
	targets := fs.String("targets", strings.Join(cfg.Targets, ","), "comma-separated logical catalog target IDs")
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
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %s", strings.Join(fs.Args(), " "))
	}
	if !*planOnly {
		return errors.New("fault controllers are not integrated yet (issues #13/#14); use run --seed N --plan-only to generate a schedule")
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
		return err
	}
	if err := fault.Save(*path, plan); err != nil {
		return fmt.Errorf("save fault plan: %w", err)
	}
	_, err = fmt.Fprintf(out, "PLAN ONLY: saved %d faults for seed %d to %s (%s)\n", len(plan.Events), plan.Seed, *path, plan.Version)
	return err
}
