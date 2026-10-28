package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestRunCampaignRefusesAnExistingRunDirectory(t *testing.T) {
	dir := t.TempDir()
	earlier := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(earlier, []byte("earlier run"), 0600); err != nil {
		t.Fatal(err)
	}
	// The catalog URL is unreachable: refusing must happen before any request.
	_, err := runCampaign([]string{"--seed", "1", "--out-dir", dir, "--faults", "pause", "--catalog", "http://127.0.0.1:1"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "run directory") {
		t.Fatalf("an existing run directory was not refused: %v", err)
	}
	if data, _ := os.ReadFile(earlier); string(data) != "earlier run" {
		t.Fatal("an earlier run was overwritten")
	}
}

func TestRunCampaignBoundsAHungToxiproxy(t *testing.T) {
	release := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer api.Close()
	defer close(release)
	dir := t.TempDir()
	start := time.Now()
	_, err := runCampaign([]string{"--seed", "1", "--faults", "latency", "--action-timeout", "300ms", "--duration", "20s",
		"--toxiproxy", api.URL, "--out-dir", filepath.Join(dir, "run"), "--catalog", "http://127.0.0.1:1"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "deadline") || time.Since(start) > 5*time.Second {
		t.Fatalf("a hung Toxiproxy API was not bounded: %v after %s", err, time.Since(start))
	}
}
