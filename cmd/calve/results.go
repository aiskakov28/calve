package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
)

// resultsCmd re-checks every saved history under a sweep directory and prints
// one table row per catalog and workload. The histories are the source of
// truth: verdicts are recomputed offline, never read from run logs.
func resultsCmd(args []string) {
	fs := flag.NewFlagSet("results", flag.ExitOnError)
	search := fs.Duration("check-timeout", 5*time.Second, "maximum Porcupine search time per snapshot history")
	asJSON := fs.Bool("json", false, "print rows as JSON instead of a Markdown table")
	fs.Parse(args)
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: calve results [--json] <sweep directory>")
		os.Exit(2)
	}
	rows, err := sweepResults(fs.Arg(0), *search)
	if err != nil {
		fail("%v", err)
	}
	if *asJSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(rows); err != nil {
			fail("%v", err)
		}
		return
	}
	writeResultsTable(os.Stdout, rows)
}

// resultRow summarizes every run of one workload on one catalog under one fault.
type resultRow struct {
	Catalog      string         `json:"catalog"`
	Version      string         `json:"version,omitempty"`
	Workload     string         `json:"workload"`
	Fault        string         `json:"fault"`
	Runs         int            `json:"runs"`
	Valid        int            `json:"valid"`
	Invalid      int            `json:"invalid"`
	Inconclusive int            `json:"inconclusive"`
	Errors       int            `json:"errors"`
	Commits      int            `json:"commits"`
	Anomalies    map[string]int `json:"anomalies,omitempty"`
	Histories    []string       `json:"histories"`
}

// sweepResults expects, as written by scripts/sweep.sh, baseline histories at
// <dir>/<catalog>/<workload>/<run>.jsonl and campaign run directories at
// <dir>/<catalog>/<workload>/<fault>/<run>/, with <run>.missing for a run that
// saved nothing, and an optional <dir>/catalogs.tsv of catalog and image.
func sweepResults(dir string, search time.Duration) ([]resultRow, error) {
	versions, err := readCatalogVersions(filepath.Join(dir, "catalogs.tsv"))
	if err != nil {
		return nil, err
	}
	rows := map[string]*resultRow{}
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		ext := filepath.Ext(path)
		// A campaign run is a directory, or <run>.missing, one level below its fault.
		campaign := len(parts) == 4 && (entry.IsDir() || ext == ".missing")
		if entry.IsDir() && !campaign || !entry.IsDir() && ext != ".jsonl" && ext != ".missing" {
			return nil
		}
		if !campaign && len(parts) != 3 {
			return fmt.Errorf("%s is not <catalog>/<workload>/<run>.jsonl, <catalog>/<workload>/<fault>/<run>/ or .missing", rel)
		}
		fault := "none"
		if campaign {
			fault = parts[2]
		}
		key := parts[0] + "/" + parts[1] + "/" + fault
		row := rows[key]
		if row == nil {
			row = &resultRow{Catalog: parts[0], Version: versions[parts[0]], Workload: parts[1], Fault: fault, Anomalies: map[string]int{}}
			rows[key] = row
		}
		row.Runs++
		if ext == ".missing" {
			// The run saved no history, so it has no verdict; count it as an error.
			row.Errors++
			return nil
		}
		row.Histories = append(row.Histories, filepath.ToSlash(rel))
		var verdict checker.Verdict
		var kinds []string
		var commits int
		if campaign {
			verdict, kinds, commits, err = recheckRun(path, parts[1])
		} else {
			verdict, kinds, commits, err = recheckHistory(path, parts[1], search)
		}
		var next error // Its files are one run, already checked.
		if campaign && entry.IsDir() {
			next = filepath.SkipDir
		}
		if err != nil {
			// A history that cannot be checked is counted, never dropped.
			row.Errors++
			return next
		}
		switch verdict {
		case checker.Valid:
			row.Valid++
		case checker.Invalid:
			row.Invalid++
		default:
			row.Inconclusive++
		}
		row.Commits += commits
		for _, kind := range kinds {
			row.Anomalies[kind]++
		}
		return next
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no histories under %s", dir)
	}
	out := make([]resultRow, 0, len(rows))
	for _, row := range rows {
		sort.Strings(row.Histories)
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Catalog != out[j].Catalog {
			return out[i].Catalog < out[j].Catalog
		}
		if out[i].Workload != out[j].Workload {
			return out[i].Workload < out[j].Workload
		}
		return out[i].Fault < out[j].Fault
	})
	return out, nil
}

// recheckRun checks a campaign run directory as calve check does, including
// its load coverage, and counts its commit attempts. The directory names the
// workload; the run must agree.
func recheckRun(path, workload string) (checker.Verdict, []string, int, error) {
	code, recomputed, err := checkRunResult(path, io.Discard)
	if err != nil {
		return "", nil, 0, err
	}
	ops, err := readOps(filepath.Join(path, historyFile))
	if err != nil {
		return "", nil, 0, err
	}
	data, err := os.ReadFile(filepath.Join(path, resultFile))
	if err != nil {
		return "", nil, 0, err
	}
	var saved struct {
		Workload string `json:"workload"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		return "", nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	// The anomalies come from this check, as the verdict does: after a search
	// timeout the saved result may differ.
	encoded, err := json.Marshal(recomputed)
	if err != nil {
		return "", nil, 0, err
	}
	var result struct {
		Anomalies []struct {
			Kind string `json:"kind"`
		} `json:"anomalies"`
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return "", nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	if recordedWorkloads[workload] == "" || saved.Workload != recordedWorkloads[workload] {
		return "", nil, 0, fmt.Errorf("%s is not a %s run", path, workload)
	}
	verdict := map[int]checker.Verdict{0: checker.Valid, 3: checker.Invalid, 4: checker.Inconclusive}[code]
	if verdict == "" {
		return "", nil, 0, fmt.Errorf("%s: unexpected check status %d", path, code)
	}
	var kinds []string
	for _, a := range result.Anomalies {
		kinds = append(kinds, a.Kind)
	}
	return verdict, kinds, commitAttempts(ops), nil
}

// recordedWorkloads maps the sweep's workload names to the names histories and
// run results record.
var recordedWorkloads = map[string]string{"append": "append", "skew": "skew", "atomic": "atomic",
	"snapshot": "snapshot-cas", "schema": "schema-cas", "maintenance": "maintenance-race"}

// recheckHistory runs the workload's checker on a saved history and counts its
// commit attempts. The directory names the workload; the history's run record
// must agree.
func recheckHistory(path, workload string, search time.Duration) (checker.Verdict, []string, int, error) {
	ops, err := readOps(path)
	if err != nil {
		return "", nil, 0, err
	}
	recorded := recordedWorkloads[workload]
	if recorded == "" || historyWorkload(ops) != recorded {
		return "", nil, 0, fmt.Errorf("%s is not a %s history", path, workload)
	}
	verdict, kinds, err := checkHistory(ops, workload, search)
	return verdict, kinds, commitAttempts(ops), err
}

func checkHistory(ops []history.Op, workload string, search time.Duration) (checker.Verdict, []string, error) {
	var kinds []string
	switch workload {
	case "append":
		res, err := checker.CheckAppendHistory(ops)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	case "skew":
		res, err := checker.CheckSkew(ops)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	case "atomic":
		res, err := checker.CheckAtomic(ops)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	case "snapshot":
		res, err := checker.CheckSnapshotHistory(ops, search)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	case "schema":
		res, err := checker.CheckSchema(ops)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	default:
		res, err := checker.CheckMaintenance(ops)
		for _, a := range res.Anomalies {
			kinds = append(kinds, a.Kind)
		}
		return res.Verdict, kinds, err
	}
}

// commitCalls are the calls that ask a catalog to commit, one per attempt.
var commitCalls = map[string]bool{"append": true, "atomic-txn": true, "skew-txn": true, "commit-table": true}

// commitAttempts counts the commit requests a history sent, whatever their
// outcome: the denominator for anomalies per 1,000 commits.
func commitAttempts(ops []history.Op) int {
	n := 0
	for _, op := range ops {
		if op.Type == history.Invoke && commitCalls[op.F] {
			n++
		}
	}
	return n
}

func readCatalogVersions(path string) (map[string]string, error) {
	versions := map[string]string{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return versions, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name, image, ok := strings.Cut(scanner.Text(), "\t")
		if ok && name != "" {
			versions[name] = image
		}
	}
	return versions, scanner.Err()
}

func writeResultsTable(out io.Writer, rows []resultRow) {
	fmt.Fprintln(out, "| Catalog | Version | Workload | Fault | Runs | Valid | Invalid | Inconclusive | Errors | Commits | Per 1k | Anomalies |")
	fmt.Fprintln(out, "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, row := range rows {
		var kinds []string
		total := 0
		for kind, n := range row.Anomalies {
			kinds = append(kinds, fmt.Sprintf("%s ×%d", kind, n))
			total += n
		}
		sort.Strings(kinds)
		// Without a checked commit there is no rate to give.
		rate := ""
		if row.Commits > 0 {
			rate = fmt.Sprintf("%.1f", float64(total)*1000/float64(row.Commits))
		}
		fmt.Fprintf(out, "| %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %s | %s |\n", row.Catalog, row.Version, row.Workload, row.Fault,
			row.Runs, row.Valid, row.Invalid, row.Inconclusive, row.Errors, row.Commits, rate, strings.Join(kinds, ", "))
	}
}
