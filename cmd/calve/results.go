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
	Anomalies    map[string]int `json:"anomalies,omitempty"`
	Histories    []string       `json:"histories"`
}

// sweepResults expects <dir>/<catalog>/<workload>/<run>.jsonl, or <run>.missing
// for a run that saved no history, as written by scripts/sweep.sh, and an
// optional <dir>/catalogs.tsv of catalog and image.
func sweepResults(dir string, search time.Duration) ([]resultRow, error) {
	versions, err := readCatalogVersions(filepath.Join(dir, "catalogs.tsv"))
	if err != nil {
		return nil, err
	}
	rows := map[string]*resultRow{}
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		ext := filepath.Ext(path)
		if err != nil || entry.IsDir() || ext != ".jsonl" && ext != ".missing" {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 {
			return fmt.Errorf("%s is not <catalog>/<workload>/<run>.jsonl or .missing", rel)
		}
		key := parts[0] + "/" + parts[1]
		row := rows[key]
		if row == nil {
			row = &resultRow{Catalog: parts[0], Version: versions[parts[0]], Workload: parts[1], Fault: "none", Anomalies: map[string]int{}}
			rows[key] = row
		}
		row.Runs++
		if ext == ".missing" {
			// The run saved no history, so it has no verdict; count it as an error.
			row.Errors++
			return nil
		}
		row.Histories = append(row.Histories, filepath.ToSlash(rel))
		verdict, kinds, err := recheckHistory(path, parts[1], search)
		if err != nil {
			// A history that cannot be checked is counted, never dropped.
			row.Errors++
			return nil
		}
		switch verdict {
		case checker.Valid:
			row.Valid++
		case checker.Invalid:
			row.Invalid++
		default:
			row.Inconclusive++
		}
		for _, kind := range kinds {
			row.Anomalies[kind]++
		}
		return nil
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
		return out[i].Workload < out[j].Workload
	})
	return out, nil
}

// recheckHistory runs the workload's checker on a saved history. The
// directory names the workload; the history's run record must agree.
func recheckHistory(path, workload string, search time.Duration) (checker.Verdict, []string, error) {
	ops, err := readOps(path)
	if err != nil {
		return "", nil, err
	}
	recorded := map[string]string{"append": "append", "skew": "skew", "atomic": "atomic",
		"snapshot": "snapshot-cas", "schema": "schema-cas", "maintenance": "maintenance-race"}[workload]
	if recorded == "" || historyWorkload(ops) != recorded {
		return "", nil, fmt.Errorf("%s is not a %s history", path, workload)
	}
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
	fmt.Fprintln(out, "| Catalog | Version | Workload | Fault | Runs | Valid | Invalid | Inconclusive | Errors | Anomalies |")
	fmt.Fprintln(out, "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, row := range rows {
		var kinds []string
		for kind, n := range row.Anomalies {
			kinds = append(kinds, fmt.Sprintf("%s ×%d", kind, n))
		}
		sort.Strings(kinds)
		fmt.Fprintf(out, "| %s | %s | %s | %s | %d | %d | %d | %d | %d | %s |\n", row.Catalog, row.Version, row.Workload, row.Fault,
			row.Runs, row.Valid, row.Invalid, row.Inconclusive, row.Errors, strings.Join(kinds, ", "))
	}
}
