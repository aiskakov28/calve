package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aiskakov28/calve/internal/checker"
)

// fakeShrink decides each attempt from the settings calve run would parse, so
// the search can be checked without a catalog.
func fakeShrink(t *testing.T, decide func(rf *runFlags, call int) (checker.Verdict, []string, error)) (shrinkAttempt, *int) {
	calls := 0
	return func(args []string, dir, workload string, log io.Writer) (checker.Verdict, []string, error) {
		t.Helper()
		calls++
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		rf := defineRunFlags(fs)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		if *rf.workloadName != workload {
			t.Fatalf("attempt for %s ran %s", workload, *rf.workloadName)
		}
		return decide(rf, calls)
	}, &calls
}

func readShrinkSummary(t *testing.T, dir string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "shrink.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary map[string]any
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	return summary
}

// Each setting is halved until halving stops reproducing: settings the failure
// does not depend on reach their minimum, and the ones it does stop above it.
func TestShrinkHalvesEverySettingItCan(t *testing.T) {
	attempt, _ := fakeShrink(t, func(rf *runFlags, _ int) (checker.Verdict, []string, error) {
		partition := strings.Contains(*rf.kinds, "partition")
		if *rf.clients >= 2 && rf.cfg.RunDuration.Seconds() >= 30 && partition {
			return checker.Invalid, []string{"lost"}, nil
		}
		return checker.Valid, nil, nil
	})
	dir := filepath.Join(t.TempDir(), "shrink")
	var out bytes.Buffer
	code, err := shrinkRun([]string{"--attempts", "1", "--out-dir", dir, "--", "--workload", "atomic", "--seed", "7",
		"--duration", "2m", "--faults", "latency,partition", "--clients", "8", "--tables", "4", "--readers", "2"}, &out, attempt)
	if err != nil || code != 0 {
		t.Fatalf("%d, %v\n%s", code, err, out.String())
	}
	summary := readShrinkSummary(t, dir)
	want := map[string]any{"duration": "30s", "faults": "partition", "clients": "2", "tables": "2", "readers": "0"}
	if !reflect.DeepEqual(summary["settings"], want) {
		t.Fatalf("settings %v, want %v\n%s", summary["settings"], want, out.String())
	}
	if !strings.Contains(summary["command"].(string), "--seed 7") || !reflect.DeepEqual(summary["anomalies"], []any{"lost"}) {
		t.Fatalf("summary: %v", summary)
	}
	// The plan's floor (warmup, one fault and healing: 18s by default) bounds
	// the duration, so 15s is never tried.
	if strings.Contains(out.String(), "--duration 15s") {
		t.Fatalf("tried a duration below the plan's floor:\n%s", out.String())
	}
}

// Failures under concurrency are not deterministic: a candidate gets several
// attempts before it counts as not reproducing.
func TestShrinkRetriesEachCandidate(t *testing.T) {
	flaky := func(_ *runFlags, call int) (checker.Verdict, []string, error) {
		if call%3 == 0 {
			return checker.Invalid, []string{"write-skew"}, nil
		}
		return checker.Inconclusive, nil, nil
	}
	attempt, _ := fakeShrink(t, flaky)
	dir := filepath.Join(t.TempDir(), "retries")
	if _, err := shrinkRun([]string{"--attempts", "3", "--out-dir", dir, "--", "--workload", "skew", "--duration", "40s",
		"--faults", "reset,pause"}, io.Discard, attempt); err != nil {
		t.Fatal(err)
	}
	if got := readShrinkSummary(t, dir)["settings"]; !reflect.DeepEqual(got, map[string]any{"duration": "18s", "faults": "reset"}) {
		t.Fatalf("settings %v", got)
	}

	// With one attempt the original never fails, so there is nothing to shrink.
	attempt, _ = fakeShrink(t, flaky)
	_, err := shrinkRun([]string{"--attempts", "1", "--out-dir", filepath.Join(t.TempDir(), "once"), "--",
		"--workload", "skew", "--duration", "40s"}, io.Discard, attempt)
	if err == nil || !strings.Contains(err.Error(), "did not give an invalid verdict") {
		t.Fatalf("want the original to be reported as not reproducing: %v", err)
	}
}

// A smaller run that fails differently, or errors, has not reproduced the
// finding.
func TestShrinkKeepsTheOriginalAnomaly(t *testing.T) {
	attempt, _ := fakeShrink(t, func(rf *runFlags, _ int) (checker.Verdict, []string, error) {
		switch {
		case *rf.clients >= 4:
			return checker.Invalid, []string{"lost"}, nil
		case *rf.clients == 2:
			return checker.Invalid, []string{"duplicate"}, nil
		default:
			return "", nil, os.ErrDeadlineExceeded
		}
	})
	dir := filepath.Join(t.TempDir(), "kinds")
	if _, err := shrinkRun([]string{"--attempts", "1", "--out-dir", dir, "--", "--workload", "append", "--duration", "20s",
		"--faults", "pause", "--clients", "4"}, io.Discard, attempt); err != nil {
		t.Fatal(err)
	}
	// Only the duration, which this failure does not depend on, shrinks.
	if got := readShrinkSummary(t, dir)["settings"]; !reflect.DeepEqual(got, map[string]any{"duration": "18s"}) {
		t.Fatalf("a different anomaly or an error counted as reproducing: %v", got)
	}
	data, err := os.ReadFile(filepath.Join(dir, "shrink.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"verdict":"invalid","anomalies":{"duplicate":1},"reproduced":false`) {
		t.Fatalf("log:\n%s", data)
	}
}

func TestShrinkRejectsWhatItOwnsAndHidesTheToken(t *testing.T) {
	attempt, calls := fakeShrink(t, func(*runFlags, int) (checker.Verdict, []string, error) {
		return checker.Invalid, []string{"write-skew"}, nil
	})
	for _, args := range [][]string{
		{"--", "--out-dir", "elsewhere"},
		{"--", "--plan-only"},
		{"--", "--workload", "nope"},
		{"--attempts", "0", "--"},
	} {
		if _, err := shrinkRun(append([]string{"--out-dir", filepath.Join(t.TempDir(), "x")}, args...), io.Discard, attempt); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if *calls != 0 {
		t.Fatal("ran an attempt for rejected arguments")
	}

	dir := filepath.Join(t.TempDir(), "token")
	var out bytes.Buffer
	if _, err := shrinkRun([]string{"--attempts", "1", "--out-dir", dir, "--", "--workload", "skew", "--duration", "20s",
		"--faults", "pause", "--token", "s3cret", "--warehouse=calve"}, &out, attempt); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shrink.json", "shrink.jsonl"} {
		data, _ := os.ReadFile(filepath.Join(dir, name))
		if bytes.Contains(data, []byte("s3cret")) {
			t.Fatalf("%s holds the token", name)
		}
	}
	if strings.Contains(out.String(), "s3cret") || !strings.Contains(out.String(), "--token REDACTED") {
		t.Fatalf("output:\n%s", out.String())
	}
	// The same directory cannot hold a second shrink.
	if _, err := shrinkRun([]string{"--out-dir", dir, "--", "--workload", "skew"}, io.Discard, attempt); err == nil {
		t.Fatal("reused a shrink directory")
	}
}

func TestRedactRunArgs(t *testing.T) {
	got := redactRunArgs([]string{"--token", "a", "-token=b", "--catalog", "token", "--tokens-file", "c"})
	want := []string{"--token", "REDACTED", "-token=REDACTED", "--catalog", "token", "--tokens-file", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
}

func TestApplySettingsReplacesTheOriginalValues(t *testing.T) {
	got := applySettings([]string{"--duration", "40s", "--seed", "1", "-faults=latency,reset", "--catalog", "http://x"},
		map[string]string{"duration": "18s", "faults": "latency"})
	want := []string{"--seed", "1", "--catalog", "http://x", "--duration", "18s", "--faults", "latency"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
}
