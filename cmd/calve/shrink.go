package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/history"
)

func shrinkCmd(args []string) {
	ctx, stop := interruptible(os.Stderr)
	attempt := func(args []string, dir, workload string, log io.Writer) (checker.Verdict, []string, error) {
		return liveAttempt(ctx, args, dir, workload, log)
	}
	code, err := shrinkRun(ctx, args, os.Stdout, attempt)
	stop()
	if err != nil {
		fail("%v", err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

// shrinkAttempt runs calve run once with args, saving the run directory at
// dir, and returns the verdict and anomaly kinds that calve check recomputes.
type shrinkAttempt func(args []string, dir, workload string, log io.Writer) (checker.Verdict, []string, error)

func liveAttempt(ctx context.Context, args []string, dir, workload string, log io.Writer) (checker.Verdict, []string, error) {
	if _, err := runCampaign(ctx, append(append([]string(nil), args...), "--out-dir", dir), log); err != nil {
		return "", nil, err
	}
	verdict, kinds, _, err := recheckRun(dir, workload)
	return verdict, kinds, err
}

// shrinkDim is one setting the shrinker halves toward its minimum.
type shrinkDim struct {
	flag     string
	value    int64
	min      int64
	duration bool
}

func (d shrinkDim) format(v int64) string {
	if d.duration {
		return time.Duration(v).String()
	}
	return strconv.FormatInt(v, 10)
}

// shrinkRecord is one attempt in shrink.jsonl.
type shrinkRecord struct {
	Attempt    int               `json:"attempt"`
	Settings   map[string]string `json:"settings"`
	Dir        string            `json:"dir"`
	Verdict    checker.Verdict   `json:"verdict,omitempty"`
	Anomalies  map[string]int    `json:"anomalies,omitempty"`
	Reproduced bool              `json:"reproduced"`
	Error      string            `json:"error,omitempty"`
}

// shrinkRun reruns a failing calve run with smaller settings: a shorter plan,
// fewer fault kinds and fewer clients, tables or readers. A candidate
// reproduces when one of its attempts is invalid with an anomaly kind the
// original showed; failures under concurrency are not deterministic, so each
// candidate gets several attempts. Each setting is halved toward its minimum
// until halving stops reproducing, and passes repeat until none changes, so
// the result is small, not necessarily minimal. The seed stays fixed: each
// plan is regenerated from it, so every attempt is a run calve check accepts.
func shrinkRun(ctx context.Context, args []string, out io.Writer, attempt shrinkAttempt) (int, error) {
	fs := flag.NewFlagSet("shrink", flag.ContinueOnError)
	fs.SetOutput(out)
	attempts := fs.Int("attempts", 3, "runs per candidate before it counts as not reproducing")
	outDir := fs.String("out-dir", "", "new directory for every attempt's run directory and the shrink log (default shrinks/<shrink ID>)")
	fs.Usage = func() {
		fmt.Fprintln(out, "usage: calve shrink [--attempts N] [--out-dir DIR] -- <calve run flags>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, nil
		}
		return 0, err
	}
	if *attempts < 1 {
		return 0, errors.New("--attempts must be at least 1")
	}
	runArgs := fs.Args()

	// Parse the run's flags as calve run will, to read its settings.
	rfs := flag.NewFlagSet("run", flag.ContinueOnError)
	rfs.SetOutput(io.Discard)
	rf := defineRunFlags(rfs)
	if err := rfs.Parse(runArgs); err != nil {
		return 0, fmt.Errorf("calve run flags: %w", err)
	}
	if rfs.NArg() != 0 {
		return 0, fmt.Errorf("unexpected run arguments: %s", strings.Join(rfs.Args(), " "))
	}
	var owned []string
	rfs.Visit(func(f *flag.Flag) {
		if f.Name == "out-dir" || f.Name == "plan-only" || f.Name == "plan" {
			owned = append(owned, "--"+f.Name)
		}
	})
	if len(owned) != 0 {
		return 0, fmt.Errorf("calve shrink runs every attempt into its own directory; drop %s", strings.Join(owned, ", "))
	}
	workload := *rf.workloadName
	if recordedWorkloads[workload] == "" {
		return 0, fmt.Errorf("unsupported --workload %q", workload)
	}

	cfg := rf.cfg
	// Below this a plan cannot fit warmup, one fault and its healing budget.
	floor := cfg.Warmup + cfg.MaxHealthy + cfg.MaxFault + cfg.ActionTimeout
	dims := []shrinkDim{{flag: "duration", value: int64(cfg.RunDuration), min: int64(floor), duration: true}}
	switch workload {
	case "append":
		dims = append(dims, shrinkDim{flag: "clients", value: int64(*rf.clients), min: 1})
		if *rf.perClient > 0 {
			dims = append(dims, shrinkDim{flag: "per-client", value: int64(*rf.perClient), min: 1})
		}
	case "schema":
		// A schema round needs two contenders.
		dims = append(dims, shrinkDim{flag: "clients", value: int64(*rf.clients), min: 2})
	case "atomic":
		dims = append(dims, shrinkDim{flag: "clients", value: int64(*rf.clients), min: 1},
			shrinkDim{flag: "tables", value: int64(*rf.atomicTables), min: 2},
			shrinkDim{flag: "readers", value: int64(*rf.atomicReaders), min: 0})
	case "snapshot":
		dims = append(dims, shrinkDim{flag: "snapshot-clients", value: int64(*rf.snapshotClients), min: 1})
	}
	var kinds []string
	for _, kind := range strings.Split(*rf.kinds, ",") {
		if kind = strings.TrimSpace(kind); kind != "" {
			kinds = append(kinds, kind)
		}
	}

	if *outDir == "" {
		*outDir = filepath.Join("shrinks", fmt.Sprintf("shrink_%d", time.Now().UnixNano()))
	}
	if entries, err := os.ReadDir(*outDir); err == nil && len(entries) != 0 {
		return 0, fmt.Errorf("%s is not empty; use a new directory", *outDir)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return 0, err
	}
	logFile, err := os.OpenFile(filepath.Join(*outDir, "shrink.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	records := json.NewEncoder(logFile)

	shown := redactRunArgs(runArgs)
	settings := map[string]string{}
	var target map[string]bool
	tried := map[string]bool{}
	n := 0
	var reproducing string
	// try runs a candidate up to the attempt budget and reports whether it
	// reproduced. The original run is a candidate with no changed settings;
	// its first invalid attempt sets the anomaly kinds to keep.
	try := func(candidate map[string]string) (bool, error) {
		key := settingsKey(candidate)
		if tried[key] {
			return false, nil
		}
		tried[key] = true
		for i := 0; i < *attempts; i++ {
			n++
			dir := filepath.Join(*outDir, fmt.Sprintf("attempt-%03d", n))
			rec := shrinkRecord{Attempt: n, Settings: candidate, Dir: dir}
			verdict, found, err := runShrinkAttempt(attempt, applySettings(runArgs, candidate), dir, workload)
			if err != nil {
				// A catalog's error can echo its response, so it is redacted as calve
				// run redacts its own errors.
				rec.Error = history.RedactText(err.Error(), rf.cf.catalog, rf.cf.token,
					os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN"))
			} else {
				rec.Verdict = verdict
				if len(found) != 0 {
					rec.Anomalies = map[string]int{}
				}
				for _, kind := range found {
					rec.Anomalies[kind]++
				}
				if verdict == checker.Invalid && target == nil {
					target = map[string]bool{}
					for _, kind := range found {
						target[kind] = true
					}
				}
				for _, kind := range found {
					rec.Reproduced = rec.Reproduced || verdict == checker.Invalid && target[kind]
				}
			}
			if err := records.Encode(rec); err != nil {
				return false, err
			}
			fmt.Fprintf(out, "attempt %d: %s: %s\n", n, describeSettings(candidate), describeAttempt(rec))
			// The interrupted attempt has healed its faults and is logged; start
			// no other.
			if ctx.Err() != nil {
				return false, fmt.Errorf("interrupted after attempt %d (log in %s)", n, *outDir)
			}
			if rec.Reproduced {
				reproducing = dir
				return true, nil
			}
		}
		return false, nil
	}

	ok, err := try(settings)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("calve run %s did not give an invalid verdict in %d attempts; nothing to shrink (log in %s)",
			strings.Join(shown, " "), *attempts, *outDir)
	}
	for changed := true; changed; {
		changed = false
		for i := range dims {
			d := &dims[i]
			for {
				next := max(d.min, d.value/2)
				if next >= d.value {
					break
				}
				candidate := withSetting(settings, d.flag, d.format(next))
				ok, err := try(candidate)
				if err != nil {
					return 0, err
				}
				if !ok {
					break
				}
				settings, d.value, changed = candidate, next, true
			}
			// A shorter plan makes every later attempt cheaper, so fault kinds
			// come right after the duration.
			if d.flag == "duration" && len(kinds) > 1 {
				for _, kind := range kinds {
					candidate := withSetting(settings, "faults", kind)
					ok, err := try(candidate)
					if err != nil {
						return 0, err
					}
					if ok {
						settings, kinds, changed = candidate, []string{kind}, true
						break
					}
				}
			}
		}
	}

	var anomalies []string
	for kind := range target {
		anomalies = append(anomalies, kind)
	}
	sort.Strings(anomalies)
	command := "calve run " + strings.Join(applySettings(shown, settings), " ")
	summary := map[string]any{"original": shown, "settings": settings, "anomalies": anomalies, "run": reproducing, "command": command, "attempts": n}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(*outDir, "shrink.json"), append(data, '\n'), 0o600); err != nil {
		return 0, err
	}
	fmt.Fprintf(out, "shrunk in %d attempts: %s\n", n, command)
	fmt.Fprintf(out, "reproduces %s; last reproducing run %s (calve check %s)\n", strings.Join(anomalies, ", "), reproducing, reproducing)
	return 0, nil
}

// runShrinkAttempt keeps the run's own output in a log beside its directory.
func runShrinkAttempt(attempt shrinkAttempt, args []string, dir, workload string) (checker.Verdict, []string, error) {
	log, err := os.OpenFile(dir+".log", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", nil, err
	}
	verdict, kinds, err := attempt(args, dir, workload, log)
	return verdict, kinds, errors.Join(err, log.Close())
}

func withSetting(settings map[string]string, name, value string) map[string]string {
	out := map[string]string{name: value}
	for k, v := range settings {
		if k != name {
			out[k] = v
		}
	}
	return out
}

// settingsArgs renders changed settings as flags, in name order.
func settingsArgs(settings map[string]string) []string {
	names := make([]string, 0, len(settings))
	for name := range settings {
		names = append(names, name)
	}
	sort.Strings(names)
	var args []string
	for _, name := range names {
		args = append(args, "--"+name, settings[name])
	}
	return args
}

// applySettings replaces each changed setting's flags in the original
// arguments, in either the "--name value" or "--name=value" form, and appends
// the new values.
func applySettings(args []string, settings map[string]string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		name, _, inline := strings.Cut(strings.TrimLeft(args[i], "-"), "=")
		if _, changed := settings[name]; changed && strings.HasPrefix(args[i], "-") {
			if !inline {
				i++ // its value
			}
			continue
		}
		out = append(out, args[i])
	}
	return append(out, settingsArgs(settings)...)
}

func settingsKey(settings map[string]string) string {
	return strings.Join(settingsArgs(settings), " ")
}

func describeSettings(settings map[string]string) string {
	if len(settings) == 0 {
		return "original"
	}
	return settingsKey(settings)
}

func describeAttempt(rec shrinkRecord) string {
	if rec.Error != "" {
		return "error: " + rec.Error
	}
	s := string(rec.Verdict)
	if len(rec.Anomalies) != 0 {
		var kinds []string
		for kind, n := range rec.Anomalies {
			kinds = append(kinds, fmt.Sprintf("%s ×%d", kind, n))
		}
		sort.Strings(kinds)
		s += " (" + strings.Join(kinds, ", ") + ")"
	}
	if rec.Reproduced {
		s += ", reproduced"
	}
	return s
}

// redactRunArgs hides a --token value before the arguments are printed or
// saved; CALVE_TOKEN keeps it out of the arguments altogether.
func redactRunArgs(args []string) []string {
	out := append([]string(nil), args...)
	for i, arg := range out {
		name, _, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if name != "token" || !strings.HasPrefix(arg, "-") {
			continue
		}
		if hasValue {
			out[i] = arg[:strings.Index(arg, "=")+1] + "REDACTED"
		} else if i+1 < len(out) {
			out[i+1] = "REDACTED"
		}
	}
	return out
}
