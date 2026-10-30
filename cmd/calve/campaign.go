package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

// campaign runs a workload under a fault plan (issues #54 and #76). Setup and
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
	// Set by executeCampaign: load coverage and the verdict it was combined into.
	coverage []faultCoverage
	verdict  checker.Verdict
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

// run executes the plan, calling start from the executor's OnStart in its own
// goroutine, and returns once the load has joined and the catalog answers.
// start gets the load's context, cancelled only if the plan fails, and the
// plan's, which ends at the plan's horizon.
func (cp *campaign) run(ctx context.Context, runID string, origin time.Time, start func(loadCtx, planCtx context.Context)) error {
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	done := make(chan struct{})
	started := false
	var err error
	cp.result, err = fault.Execute(ctx, cp.plan, fault.ExecuteOptions{
		RunID: runID, PlanPath: cp.planPath, HistoryPath: cp.faultsPath,
		// The fault history shares the workload history's clock origin, so load
		// coverage can compare their times.
		Recorder: history.NewRecorderAt(origin), Controllers: cp.controllers,
		// The load gets its own context, not the plan's: the plan's ends with
		// its run duration, and cutting the workload short would leave
		// configured requests unrecorded. It stops early only if the plan fails.
		OnStart: func(planCtx context.Context) {
			started = true
			go func() {
				defer close(done)
				start(loadCtx, planCtx)
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
	resultVersion = "calve-run-v4"
	// resultVersionV3 records coverage with each fault's window opening when its
	// apply completed, not when it started.
	resultVersionV3 = "calve-run-v3"
	// resultVersionV2 has the complete checker result but no load coverage; its
	// fault and workload histories have separate clock origins.
	resultVersionV2 = "calve-run-v2"
	// resultVersionV1 is the append-only format, still accepted by calve check.
	resultVersionV1 = "calve-run-v1"
)

// runResult is the run's own verdict, saved last. calve check recomputes it
// from the histories and refuses a result that disagrees. Result holds the
// workload checker's complete result.
type runResult struct {
	Version  string `json:"version"`
	RunID    string `json:"run_id"`
	Workload string `json:"workload"`
	Target   string `json:"target"`
	Seed     int64  `json:"seed"`
	// A checker.Verdict, or "error". The error text itself is not saved: it can
	// quote a catalog response, which only the histories store redacted.
	Verdict string                `json:"verdict"`
	Faults  fault.ExecutionResult `json:"faults"`
	// Coverage counts the commits in flight during each applied fault. A fault
	// that met none turns a valid checker result into an inconclusive Verdict.
	Coverage []faultCoverage `json:"coverage,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
}

// runResultV1 is the calve-run-v1 format, written for append before #76.
type runResultV1 struct {
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

// campaignChecker re-checks one workload's history offline for calve check.
type campaignChecker struct {
	check  func(ops []history.Op) (any, checker.Verdict, error)
	report func(out io.Writer, result any) int
	// withoutSearch, if set, splits an encoded result into whether a
	// wall-clock-bounded search ran out of time and the fields that do not
	// depend on that search.
	withoutSearch func(encoded []byte) (rest map[string]any, limited bool, err error)
}

// campaignCheckers lists the workloads calve run supports, by recorded name.
var campaignCheckers = map[string]campaignChecker{
	"append": {
		check: func(ops []history.Op) (any, checker.Verdict, error) {
			res, err := checker.CheckAppendHistory(ops)
			return res, res.Verdict, err
		},
		report: func(out io.Writer, result any) int { return reportAppend(out, result.(checker.AppendResult)) },
	},
	"schema-cas": {
		check: func(ops []history.Op) (any, checker.Verdict, error) {
			res, err := checker.CheckSchema(ops)
			return res, res.Verdict, err
		},
		report: func(out io.Writer, result any) int { return reportSchema(out, result.(checker.SchemaResult)) },
	},
	"maintenance-race": {
		check: func(ops []history.Op) (any, checker.Verdict, error) {
			res, err := checker.CheckMaintenance(ops)
			return res, res.Verdict, err
		},
		report: func(out io.Writer, result any) int { return reportMaintenance(out, result.(checker.MaintenanceResult)) },
	},
	"snapshot-cas": {
		check: func(ops []history.Op) (any, checker.Verdict, error) {
			res, err := checker.CheckSnapshotHistory(ops, campaignSnapshotSearch)
			return res, res.Verdict, err
		},
		report:        func(out io.Writer, result any) int { return reportSnapshot(out, result.(checker.SnapshotResult)) },
		withoutSearch: snapshotWithoutSearch,
	},
}

// campaignSnapshotSearch bounds the Porcupine search for snapshot campaigns,
// live and in calve check alike, so both reach the same verdict.
const campaignSnapshotSearch = 5 * time.Second

// executeCampaign creates a new run directory, lets run write the workload
// history into it under camp, and saves the result there, including when the
// campaign fails. run saves the history and returns the workload checker's
// complete result and verdict; the returned verdict also reflects load
// coverage, which is left in camp.
func executeCampaign(camp *campaign, dir, target, runID, workloadName string,
	run func(historyPath string) (any, checker.Verdict, error)) (result any, verdict checker.Verdict, err error) {
	// Clean first: with a trailing slash, Dir would name the run directory itself.
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, "", fmt.Errorf("run directory: %w", err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, "", fmt.Errorf("run directory: %w", err)
	}
	camp.planPath, camp.faultsPath = filepath.Join(dir, planFile), filepath.Join(dir, faultsFile)
	defer func() {
		saved := runResult{Version: resultVersion, RunID: runID, Workload: workloadName, Target: target, Seed: camp.plan.Seed,
			Verdict: string(verdict), Faults: camp.result, Coverage: camp.coverage}
		if err != nil {
			saved.Verdict = "error"
		} else if saved.Result, err = json.Marshal(result); err != nil {
			saved.Verdict, err = "error", fmt.Errorf("encode result: %w", err)
		}
		if saveErr := saveNew(filepath.Join(dir, resultFile), saved); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save result: %w", saveErr))
		}
	}()
	historyPath := filepath.Join(dir, historyFile)
	result, verdict, err = run(historyPath)
	if err != nil {
		return result, verdict, err
	}
	// Coverage is computed from the saved files, exactly as calve check does.
	faults, faultsErr := readOps(camp.faultsPath)
	ops, opsErr := readOps(historyPath)
	if faultsErr != nil || opsErr != nil {
		return result, verdict, errors.Join(faultsErr, opsErr)
	}
	camp.coverage = loadCoverage(faults, ops)
	camp.verdict = campaignVerdict(verdict, camp.coverage)
	return result, camp.verdict, nil
}

// runHooked runs a workload driven by campaign hooks under camp: run gets the
// hooks, prepares, waits in Ready until the plan starts, loads until the plan's
// horizon, and does its final reads once every fault is healed and the
// catalog answers. It returns run's error, or the campaign's.
func (cp *campaign) runHooked(ctx context.Context, runID string, origin time.Time, link map[string]any,
	run func(hooks *workload.CampaignHooks) error) error {
	prepared, release, stop, loaded, healed, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}),
		make(chan struct{}), make(chan struct{}), make(chan struct{})
	var readyOnce, loadedOnce, releaseOnce, stopOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }
	hooks := &workload.CampaignHooks{Link: link, Stop: stop, Healed: healed,
		Ready:  func() { readyOnce.Do(func() { close(prepared) }); <-release },
		Loaded: func() { loadedOnce.Do(func() { close(loaded) }) },
	}
	var runErr error
	go func() {
		defer close(finished)
		runErr = run(hooks)
	}()
	select {
	case <-prepared:
	case <-finished:
		if runErr == nil {
			runErr = errors.New("workload ended before the fault plan could start")
		}
		return runErr
	}
	campErr := cp.run(ctx, runID, origin, func(_, planCtx context.Context) {
		go func() {
			// The executor cancels planCtx at the plan's horizon, or early if
			// the plan fails: no new rounds start after that.
			<-planCtx.Done()
			closeStop()
		}()
		closeRelease()
		select {
		case <-loaded:
		case <-finished:
		}
	})
	// A plan that failed before starting the load never released the workload:
	// release and stop it so it can finish. The final reads run once the
	// catalog answers, and on a campaign error too, so the history is saved.
	closeStop()
	closeRelease()
	close(healed)
	<-finished
	if campErr != nil {
		return campErr
	}
	return runErr
}

// executeAppendCampaign runs the append workload as a campaign.
func executeAppendCampaign(ctx context.Context, c *rest.Client, cfg workload.AppendConfig, readback readbackConfig, camp *campaign, dir, target string) (res checker.AppendResult, elapsed time.Duration, err error) {
	_, _, err = executeCampaign(camp, dir, target, cfg.Table, "append", func(path string) (any, checker.Verdict, error) {
		var runErr error
		res, elapsed, runErr = executeAppendWith(ctx, c, cfg, path, readback, camp)
		return res, res.Verdict, runErr
	})
	return res, elapsed, err
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

// snapshotWithoutSearch removes what depends on the Porcupine search from an
// encoded snapshot result: the search outcome, the verdict and anomaly it
// decides, and its timeout reason. Only a search that ran out of time can
// differ between machines; a finished search is deterministic.
func snapshotWithoutSearch(encoded []byte) (map[string]any, bool, error) {
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, false, err
	}
	limited := fields["search"] == string(porcupine.Unknown)
	delete(fields, "search")
	delete(fields, "verdict")
	delete(fields, "anomalies")
	if reasons, ok := fields["reasons"].([]any); ok {
		kept := reasons[:0]
		for _, reason := range reasons {
			if reason != "Porcupine search timed out" {
				kept = append(kept, reason)
			}
		}
		fields["reasons"] = kept
		if len(kept) == 0 {
			delete(fields, "reasons")
		}
	}
	return fields, limited, nil
}
