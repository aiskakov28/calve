package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
)

func checkCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: calve check <run directory, atomic or schema history.jsonl>")
		os.Exit(2)
	}
	code, err := checkArtifact(args[0], os.Stdout)
	if err != nil {
		fail("%v", err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

// checkRun re-checks a saved run directory without a catalog. The plan must
// match its seed, the fault history must belong to that plan and run, the
// workload history must name both, and the append verdict recomputed from the
// workload history must match the saved result.
func checkRun(dir string, out io.Writer) (int, error) {
	// Read the result first: a run that failed early may have no plan to check.
	var result runResult
	data, err := os.ReadFile(filepath.Join(dir, resultFile))
	if err != nil {
		return 0, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return 0, fmt.Errorf("%s: %w", resultFile, err)
	}
	if result.Version != resultVersion {
		return 0, fmt.Errorf("%s: unsupported version %q", resultFile, result.Version)
	}
	if result.Verdict == "error" {
		return 0, fmt.Errorf("run %s ended with an error, not a verdict; its output at the time has the error", result.RunID)
	}

	data, err = os.ReadFile(filepath.Join(dir, planFile))
	if err != nil {
		return 0, err
	}
	plan, err := fault.Read(bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", planFile, err)
	}

	faults, err := readOps(filepath.Join(dir, faultsFile))
	if err != nil {
		return 0, err
	}
	var planned struct {
		RunID string     `json:"run_id"`
		Plan  fault.Plan `json:"plan"`
	}
	// Each planned event needs exactly one successful execution apply and heal,
	// carrying that event as planned; counts alone could double one fault.
	events := map[string]fault.Event{}
	for _, event := range plan.Events {
		events[event.ID] = event
	}
	done := map[string]int{}
	plans, applied, healed := 0, 0, 0
	for _, op := range faults {
		switch {
		case op.Type == history.Meta && op.F == "fault-plan":
			plans++
			if err := json.Unmarshal(op.Value, &planned); err != nil {
				return 0, fmt.Errorf("%s: fault plan record: %w", faultsFile, err)
			}
		case op.F == "fault" && op.Type == history.OK:
			var action fault.ActionRecord
			if err := json.Unmarshal(op.Value, &action); err != nil {
				return 0, fmt.Errorf("%s: fault action %d: %w", faultsFile, op.Index, err)
			}
			if action.Injection.RunID != planned.RunID {
				return 0, fmt.Errorf("%s: fault action %d belongs to run %q", faultsFile, op.Index, action.Injection.RunID)
			}
			if action.Stage != "execution" {
				continue
			}
			event, ok := events[action.Injection.Event.ID]
			if !ok || !reflect.DeepEqual(event, action.Injection.Event) {
				return 0, fmt.Errorf("%s: fault action %d is not a planned event", faultsFile, op.Index)
			}
			key := action.Action + " " + event.ID
			if done[key]++; done[key] > 1 {
				return 0, fmt.Errorf("%s: %s recorded more than once", faultsFile, key)
			}
			switch action.Action {
			case "apply":
				applied++
			case "heal":
				healed++
			}
		}
	}
	if plans != 1 || planned.RunID == "" || !reflect.DeepEqual(planned.Plan, plan) {
		return 0, fmt.Errorf("%s does not record exactly the saved plan", faultsFile)
	}
	for _, event := range plan.Events {
		if done["apply "+event.ID] != 1 || done["heal "+event.ID] != 1 {
			return 0, fmt.Errorf("%s has no successful apply and heal for %s", faultsFile, event.ID)
		}
	}

	ops, err := readOps(filepath.Join(dir, historyFile))
	if err != nil {
		return 0, err
	}
	var manifest struct {
		RunID        string `json:"run_id"`
		FaultSeed    int64  `json:"fault_seed"`
		FaultPlan    string `json:"fault_plan"`
		FaultHistory string `json:"fault_history"`
	}
	if len(ops) == 0 || ops[0].Type != history.Meta || ops[0].F != "run" || json.Unmarshal(ops[0].Value, &manifest) != nil ||
		manifest.RunID != planned.RunID || manifest.FaultSeed != plan.Seed ||
		manifest.FaultPlan != planFile || manifest.FaultHistory != faultsFile {
		return 0, fmt.Errorf("%s does not belong to this run's plan and fault history", historyFile)
	}

	if result.RunID != planned.RunID || result.Seed != plan.Seed || result.Workload != "append" {
		return 0, fmt.Errorf("%s does not belong to this run", resultFile)
	}
	if !result.Faults.Completed || result.Faults.Applied != applied || result.Faults.Healed != healed || applied != len(plan.Events) || healed != applied {
		return 0, fmt.Errorf("%s reports %d applied and %d healed faults; %s shows %d applied and %d healed of %d planned",
			resultFile, result.Faults.Applied, result.Faults.Healed, faultsFile, applied, healed, len(plan.Events))
	}

	res, err := checker.CheckAppendHistory(ops)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", historyFile, err)
	}
	if string(res.Verdict) != result.Verdict || res.OK != result.OK || res.Failed != result.Failed || res.Unknown != result.Unknown ||
		res.ResolvedCommitted != result.ResolvedCommitted || res.Unresolved != result.Unresolved || len(res.Anomalies) != len(result.Anomalies) {
		return 0, fmt.Errorf("%s says %s, but %s re-checks as %s", resultFile, result.Verdict, historyFile, res.Verdict)
	}
	fmt.Fprintf(out, "check %s: run %s, seed %d on %s: %d planned faults applied and healed; %d ok, %d failed, %d unknown (%d resolved as committed)\n",
		dir, result.RunID, plan.Seed, result.Target, applied, res.OK, res.Failed, res.Unknown, res.ResolvedCommitted)
	return reportAppend(out, res), nil
}

func readOps(path string) ([]history.Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ops, err := history.ReadJSONL(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return ops, nil
}
