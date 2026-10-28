package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aiskakov28/calve/internal/fault"
)

func TestRunPlanOnlyReproducesSchedule(t *testing.T) {
	dir := t.TempDir()
	var artifacts [][]byte
	for _, name := range []string{"first.json", "second.json"} {
		path := filepath.Join(dir, name)
		var out bytes.Buffer
		if err := runPlan([]string{"--seed", "123", "--plan-only", "--plan", path, "--targets", "nessie,reference", "--faults", "pause,latency"}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "PLAN ONLY") {
			t.Fatal("planning looked like a real campaign")
		}
		data, _ := os.ReadFile(path)
		if _, err := fault.Read(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, data)
	}
	if !bytes.Equal(artifacts[0], artifacts[1]) {
		t.Fatal("same seed/config produced different saved plans")
	}
}

func TestRunDoesNotPretendControllersExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	var out bytes.Buffer
	err := runPlan([]string{"--seed", "123", "--plan", path}, &out)
	if err == nil || !strings.Contains(err.Error(), "controllers") {
		t.Fatal("unavailable backends were hidden")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("unsupported execution wrote a plan")
	}
	if strings.Contains(out.String(), "VALID") {
		t.Fatal("planning generated a correctness verdict")
	}
}
