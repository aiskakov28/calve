package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fault"
	"github.com/aiskakov28/calve/internal/history"
)

// faultCoverage counts the workload commits in flight while one applied fault
// was active (docs/fault-campaigns.md, load coverage).
type faultCoverage struct {
	Event   string     `json:"event"`
	Kind    fault.Kind `json:"kind"`
	Commits int        `json:"commits"`
}

// loadCoverage pairs each fault's window, from the start of its successful
// apply to the start of its heal, with the workload's commits: workload-phase
// operations other than reads. The window opens when the apply starts, so a
// commit caught mid-flight when a process is killed or stopped counts. The fault and workload histories share a clock
// origin. A commit with no completion stays in flight to the end.
func loadCoverage(faults, ops []history.Op) []faultCoverage {
	return coverageFrom(faults, ops, true)
}

// coverageFrom computes coverage with each window opening at the start of the
// apply, or, for calve-run-v3 results, when the apply completed.
func coverageFrom(faults, ops []history.Op, fromApplyStart bool) []faultCoverage {
	starts := map[int]int64{}
	for _, op := range faults {
		if op.Type == history.Invoke {
			starts[op.ID] = op.Time
		}
	}
	type window struct {
		start, end int64
		kind       fault.Kind
	}
	windows := map[string]*window{}
	var order []string
	for _, op := range faults {
		if op.F != "fault" || op.Type != history.OK {
			continue
		}
		var action fault.ActionRecord
		if json.Unmarshal(op.Value, &action) != nil || action.Stage != "execution" {
			continue
		}
		id := action.Injection.Event.ID
		switch action.Action {
		case "apply":
			start := starts[op.ID]
			if !fromApplyStart {
				start = op.Time
			}
			windows[id] = &window{start: start, end: math.MaxInt64, kind: action.Injection.Event.Kind}
			order = append(order, id)
		case "heal":
			if w := windows[id]; w != nil {
				w.end = starts[op.ID]
			}
		}
	}
	ends := map[int]int64{}
	for _, op := range ops {
		if op.Type == history.OK || op.Type == history.Fail || op.Type == history.Info {
			ends[op.ID] = op.Time
		}
	}
	var commits [][2]int64
	for _, op := range ops {
		if op.Type != history.Invoke || op.Phase != "workload" || op.F == "load-table" {
			continue
		}
		end, ok := ends[op.ID]
		if !ok {
			end = math.MaxInt64
		}
		commits = append(commits, [2]int64{op.Time, end})
	}
	out := make([]faultCoverage, 0, len(order))
	for _, id := range order {
		w := windows[id]
		n := 0
		for _, c := range commits {
			if c[0] < w.end && c[1] > w.start {
				n++
			}
		}
		out = append(out, faultCoverage{Event: id, Kind: w.kind, Commits: n})
	}
	return out
}

// campaignVerdict turns a valid checker verdict inconclusive when an applied
// fault met no commit: that fault tested nothing. Findings stay findings.
func campaignVerdict(v checker.Verdict, coverage []faultCoverage) checker.Verdict {
	if v != checker.Valid {
		return v
	}
	for _, c := range coverage {
		if c.Commits == 0 {
			return checker.Inconclusive
		}
	}
	return v
}

// reportCampaign prints the checker's report, or, when only load coverage
// keeps the run from being valid, the faults that met no commit.
func reportCampaign(out io.Writer, chk campaignChecker, result any, checked, verdict checker.Verdict, coverage []faultCoverage) int {
	if checked != checker.Valid || verdict == checker.Valid {
		return chk.report(out, result)
	}
	fmt.Fprintln(out, "INCONCLUSIVE: the checker found no anomaly, but some faults met no commit")
	for _, c := range coverage {
		if c.Commits == 0 {
			fmt.Fprintf(out, "  fault %s (%s) was active with no commit in flight\n", c.Event, c.Kind)
		}
	}
	return 4
}
