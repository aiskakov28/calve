package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
)

func TestInspectCatalogRecordsTagAndImageID(t *testing.T) {
	docker := func(answers map[string]string, fail map[string]bool) func(context.Context, ...string) ([]byte, error) {
		return func(_ context.Context, args ...string) ([]byte, error) {
			if fail[args[0]] {
				return nil, errors.New("docker " + args[0] + " failed")
			}
			return []byte(answers[args[0]]), nil
		}
	}
	ok := map[string]string{"compose": "c0ffee\n", "inspect": `"quay.io/lakekeeper/catalog:v0.14.2" "sha256:abc"` + "\n"}
	want := catalogImage{Container: "c0ffee", Image: "quay.io/lakekeeper/catalog:v0.14.2", ImageID: "sha256:abc"}
	if got := inspectCatalog(context.Background(), "", "lakekeeper", docker(ok, nil)); got != want {
		t.Fatalf("compose service: %+v", got)
	}
	want.Container = "given"
	if got := inspectCatalog(context.Background(), "given", "", docker(ok, nil)); got != want {
		t.Fatalf("--container: %+v", got)
	}
	for name, got := range map[string]catalogImage{
		"not compose":   inspectCatalog(context.Background(), "", "", docker(ok, nil)),
		"compose fails": inspectCatalog(context.Background(), "", "lakekeeper", docker(ok, map[string]bool{"compose": true})),
		"inspect fails": inspectCatalog(context.Background(), "given", "", docker(ok, map[string]bool{"inspect": true})),
		"odd output":    inspectCatalog(context.Background(), "given", "", docker(map[string]string{"inspect": "sha256:abc\n"}, nil)),
	} {
		if got.Error == "" || got.Image != "" || got.ImageID != "" {
			t.Fatalf("%s: a failed lookup must be recorded, not guessed: %+v", name, got)
		}
	}
}

func TestCampaignSavesProvenanceFirst(t *testing.T) {
	camp, client, cfg, dir := testCampaign(t, fakecatalog.Bugs{}, loseResponses, nil)
	camp.provenance = provenance{Calve: readBuild(), Target: "reference", StartedAt: time.Now().UTC().Truncate(time.Second),
		Catalog: catalogImage{Container: "c0ffee", Image: "apache/iceberg-rest-fixture:1.10.1", ImageID: "sha256:abc"}}
	if _, _, err := executeAppendCampaign(context.Background(), client, cfg, readbackConfig{}, camp, dir, "reference"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, provenanceFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved provenance
	if err := json.Unmarshal(data, &saved); err != nil || !reflect.DeepEqual(saved, camp.provenance) {
		t.Fatalf("saved %+v, %v", saved, err)
	}
	if info, _ := os.Stat(path); runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("provenance mode %v", info.Mode().Perm())
	}
	// It describes the run; calve check still decides from the evidence alone.
	if code, err := checkRun(dir, &bytes.Buffer{}); err != nil || code != 0 {
		t.Fatalf("check with provenance: exit %d, %v", code, err)
	}
}

func TestVersionNamesTheBuild(t *testing.T) {
	var out bytes.Buffer
	if err := versionCmd(nil, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "calve ") || !strings.Contains(out.String(), runtime.Version()+" "+runtime.GOOS+"/"+runtime.GOARCH) {
		t.Fatalf("version output:\n%s", out.String())
	}
	if versionCmd([]string{"extra"}, &out) == nil {
		t.Fatal("version accepted arguments")
	}
}
