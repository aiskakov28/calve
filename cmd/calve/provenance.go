package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// provenanceFile records which Calve build ran a campaign against which catalog
// image. It describes the run, not its verdict, so calve check doesn't read it.
const provenanceFile = "provenance.json"

// build identifies this binary from the module and VCS data Go stamps into it:
// a tagged commit builds as its tag, other commits as a pseudo-version.
type build struct {
	Version  string `json:"version"`
	Commit   string `json:"commit,omitempty"`
	Modified bool   `json:"modified,omitempty"`
	Go       string `json:"go"`
	Platform string `json:"platform"`
}

func readBuild() build {
	b := build{Version: "(unknown)", Go: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	if info.Main.Version != "" {
		b.Version = info.Main.Version
	} else {
		b.Version = "(devel)"
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			b.Commit = s.Value
		case "vcs.modified":
			b.Modified = s.Value == "true"
		}
	}
	return b
}

// catalogImage is the catalog container's image as configured (a tag) and as
// run (the image ID, a digest). Error is set instead when it can't be found;
// provenance is best effort and never stops a run.
type catalogImage struct {
	Container string `json:"container,omitempty"`
	Image     string `json:"image,omitempty"`
	ImageID   string `json:"image_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

type provenance struct {
	Calve     build        `json:"calve"`
	Target    string       `json:"target"`
	Catalog   catalogImage `json:"catalog"`
	StartedAt time.Time    `json:"started_at"`
}

// inspectCatalog finds the image of container, or of service's Compose
// container when container is empty.
func inspectCatalog(ctx context.Context, container, service string, run func(context.Context, ...string) ([]byte, error)) catalogImage {
	if container == "" {
		if service == "" {
			return catalogImage{Error: "no container: not a docker-compose catalog and no --container"}
		}
		id, err := composeContainer(ctx, service, run)
		if err != nil {
			return catalogImage{Error: err.Error()}
		}
		container = id
	}
	out, err := run(ctx, "inspect", "--format", "{{json .Config.Image}} {{json .Image}}", container)
	if err != nil {
		return catalogImage{Container: container, Error: err.Error()}
	}
	var image, id string
	fields := strings.Fields(string(out))
	if len(fields) != 2 || json.Unmarshal([]byte(fields[0]), &image) != nil || json.Unmarshal([]byte(fields[1]), &id) != nil || image == "" || id == "" {
		return catalogImage{Container: container, Error: fmt.Sprintf("unexpected docker inspect output %q", strings.TrimSpace(string(out)))}
	}
	return catalogImage{Container: container, Image: image, ImageID: id}
}

func versionCmd(args []string, out io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("version takes no arguments")
	}
	b := readBuild()
	fmt.Fprintf(out, "calve %s\n", b.Version)
	if b.Commit != "" {
		modified := ""
		if b.Modified {
			modified = " (modified)"
		}
		fmt.Fprintf(out, "commit %s%s\n", b.Commit, modified)
	}
	_, err := fmt.Fprintf(out, "%s %s\n", b.Go, b.Platform)
	return err
}
