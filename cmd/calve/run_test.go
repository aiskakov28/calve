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
		if _, err := runCampaign([]string{"--seed", "123", "--plan-only", "--plan", path, "--targets", "nessie,reference", "--faults", "pause,latency"}, &out); err != nil {
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

func TestRunCampaignNeedsOneCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	var out bytes.Buffer
	_, err := runCampaign([]string{"--seed", "123", "--plan", path, "--targets", "nessie,reference"}, &out)
	if err == nil || !strings.Contains(err.Error(), "one catalog") {
		t.Fatalf("a multi-catalog campaign was accepted: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("a refused campaign wrote a plan")
	}
	if strings.Contains(out.String(), "VALID") {
		t.Fatal("a refused campaign printed a verdict")
	}
}

func TestRunCampaignRefusesExistingArtifactsBeforeRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte("earlier run"), 0600); err != nil {
		t.Fatal(err)
	}
	// The catalog URL is unreachable: refusing must happen before any request.
	_, err := runCampaign([]string{"--seed", "1", "--plan", path, "--faults", "pause", "--catalog", "http://127.0.0.1:1"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("an existing plan was not refused up front: %v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != "earlier run" {
		t.Fatal("an earlier artifact was overwritten")
	}
}
