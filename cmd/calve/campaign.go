package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

// campaign runs the append workload under a fault plan (issue #54). Setup and
// the baseline read happen before the plan starts; the final read and
// read-back happen only after every fault is healed and the catalog answers.
type campaign struct {
	plan         fault.Plan
	controllers  map[fault.Kind]fault.Controller
	planPath     string
	faultsPath   string
	probe        *rest.Client // unrecorded: readiness polls are not evidence
	readyTimeout time.Duration
	result       fault.ExecutionResult
}

// composeTargets maps each docker-compose catalog to its Toxiproxy proxy and
// container. The container names assume the compose project is named calve.
var composeTargets = map[string]struct{ proxy, container string }{
	"reference":  {"reference", "calve-rest-fixture-1"},
	"lakekeeper": {"lakekeeper", "calve-lakekeeper-1"},
	"nessie":     {"nessie", "calve-nessie-1"},
	"polaris":    {"polaris", "calve-polaris-1"},
}

// faultControllers registers Toxiproxy for network faults and Docker for
// process faults, both scoped to the one target.
func faultControllers(target, proxy, container, api string) (map[fault.Kind]fault.Controller, *fault.Toxiproxy) {
	toxiproxy := fault.NewToxiproxy(api, map[string]string{target: proxy})
	docker := fault.NewDocker(map[string]string{target: container})
	return map[fault.Kind]fault.Controller{
		fault.Latency: toxiproxy, fault.Reset: toxiproxy, fault.Partition: toxiproxy,
		fault.Kill: docker, fault.Pause: docker, fault.Restart: docker,
	}, toxiproxy
}

// checkProxied refuses a catalog URL that bypasses the proxy: network faults
// would never reach the workload, and the run would pass without testing them.
func checkProxied(catalog, listen string) error {
	u, err := url.Parse(catalog)
	if err != nil {
		return fmt.Errorf("catalog URL: %w", err)
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("toxiproxy listen address %q: %w", listen, err)
	}
	if u.Port() != port {
		return fmt.Errorf("network faults need the catalog URL to go through the toxiproxy proxy on port %s, got %s", port, history.SafeURL(catalog))
	}
	return nil
}

func (cp *campaign) run(ctx context.Context, c *rest.Client, cfg workload.AppendConfig, rec *history.Recorder) error {
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	done := make(chan struct{})
	started := false
	var err error
	cp.result, err = fault.Execute(ctx, cp.plan, fault.ExecuteOptions{
		RunID: cfg.Table, PlanPath: cp.planPath, HistoryPath: cp.faultsPath,
		Recorder: history.NewRecorder(), Controllers: cp.controllers,
		// The appends get their own context, not the plan's: the plan's ends
		// with its run duration, and cutting the workload short would leave
		// configured appends unrecorded. They stop early only if the plan fails.
		OnStart: func(context.Context) {
			started = true
			go func() {
				defer close(done)
				workload.RunAppend(loadCtx, c, cfg, rec)
			}()
		},
	})
	if err != nil {
		stopLoad()
	}
	if started {
		<-done
	}
	if err != nil {
		return fmt.Errorf("fault campaign: %w", err)
	}
	return waitReady(ctx, cp.probe, cp.readyTimeout)
}

// waitReady polls /v1/config until the catalog answers. A healed controller
// only means the container runs or the toxics are gone; a restarted catalog can
// take seconds more to serve requests.
func waitReady(ctx context.Context, probe *rest.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		_, err := probe.Config(ctx)
		if err == nil {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("catalog not ready %s after healing: %w", timeout, err)
		case <-timer.C:
		}
	}
}

// A run directory holds everything needed to re-check a campaign offline with
// calve check. Every file in it is created new; nothing is overwritten.
const (
	planFile      = "plan.json"
	faultsFile    = "faults.jsonl"
	historyFile   = "history.jsonl"
	resultFile    = "result.json"
	resultVersion = "calve-run-v1"
)

// runResult is the run's own verdict, saved last. calve check recomputes it
// from the histories and refuses a result that disagrees.
type runResult struct {
	Version  string `json:"version"`
	RunID    string `json:"run_id"`
	Workload string `json:"workload"`
	Target   string `json:"target"`
	Seed     int64  `json:"seed"`
	// A checker.Verdict, or "error". The error text itself is not saved: it can
	// quote a catalog response, which only the histories store redacted.
	Verdict           string                `json:"verdict"`
	Faults            fault.ExecutionResult `json:"faults"`
	OK                int                   `json:"ok"`
	Failed            int                   `json:"failed"`
	Unknown           int                   `json:"unknown"`
	ResolvedCommitted int                   `json:"resolved_committed"`
	Unresolved        int                   `json:"unresolved"`
	Anomalies         []checker.Anomaly     `json:"anomalies,omitempty"`
}

// executeCampaign runs the campaign into a new run directory and saves its
// result there, including when the campaign fails.
func executeCampaign(ctx context.Context, c *rest.Client, cfg workload.AppendConfig, readback readbackConfig, camp *campaign, dir, target string) (res checker.AppendResult, elapsed time.Duration, err error) {
	// Clean first: with a trailing slash, Dir would name the run directory itself.
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return res, 0, fmt.Errorf("run directory: %w", err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return res, 0, fmt.Errorf("run directory: %w", err)
	}
	camp.planPath, camp.faultsPath = filepath.Join(dir, planFile), filepath.Join(dir, faultsFile)
	defer func() {
		result := runResult{Version: resultVersion, RunID: cfg.Table, Workload: "append", Target: target, Seed: camp.plan.Seed,
			Verdict: string(res.Verdict), Faults: camp.result, OK: res.OK, Failed: res.Failed, Unknown: res.Unknown,
			ResolvedCommitted: res.ResolvedCommitted, Unresolved: res.Unresolved, Anomalies: res.Anomalies}
		if err != nil {
			result.Verdict = "error"
		}
		if saveErr := saveNew(filepath.Join(dir, resultFile), result); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save result: %w", saveErr))
		}
	}()
	return executeAppendWith(ctx, c, cfg, filepath.Join(dir, historyFile), readback, camp)
}

func saveNew(path string, v any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(v), f.Sync(), f.Close())
}
