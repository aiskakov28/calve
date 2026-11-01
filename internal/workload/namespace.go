package workload

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

// NamespaceOwnerKey is the property each contender sets on the namespace it
// tries to create, so a later load shows which create took effect.
const NamespaceOwnerKey = "calve.namespace.owner"

type NamespaceConfig struct {
	// Parent holds every round's namespace; it is created if missing.
	Parent          string
	Clients, Rounds int
}

type NamespaceRun struct {
	RunID    string `json:"run_id"`
	Workload string `json:"workload"`
	Catalog  string `json:"catalog"`
	Parent   string `json:"parent"`
	Clients  int    `json:"clients"`
	Rounds   int    `json:"rounds"`
}

// NamespaceMarker labels a contender: the owner value its create sets.
type NamespaceMarker struct {
	Owner string `json:"owner"`
}

func (cfg NamespaceConfig) Validate() error {
	if cfg.Clients < 2 || cfg.Rounds <= 0 || cfg.Clients > int(^uint(0)>>1)/cfg.Rounds {
		return fmt.Errorf("namespace requires at least 2 clients and positive rounds without overflow")
	}
	for _, level := range strings.Split(cfg.Parent, ".") {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("namespace requires nonempty parent levels")
		}
	}
	return nil
}

// NamespaceName is round's namespace: a fresh child of the run's parent.
func NamespaceName(parent, runID string, round int) string {
	return fmt.Sprintf("%s.%s_%d", parent, runID, round)
}

// NamespaceOwner is the owner value contender p sets in round.
func NamespaceOwner(round, p int) string { return fmt.Sprintf("%d-%d", round, p) }

// RunNamespace races creates of one fresh namespace per round. Every contender
// sends the same name with its own owner property, once; at most one create
// may succeed, and the namespace must then hold that contender's owner. After
// every create of a round completes, one load records the namespace. Setup,
// creating the parent, is unrecorded; nothing is dropped.
func RunNamespace(ctx context.Context, c *rest.Client, cfg NamespaceConfig, rec *history.Recorder) (run NamespaceRun, err error) {
	if c == nil || rec == nil || len(rec.Ops()) != 0 {
		return run, fmt.Errorf("namespace requires a client and an empty recorder")
	}
	if err = cfg.Validate(); err != nil {
		return run, err
	}
	var nonce [12]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return run, err
	}
	id := "nscreate_" + hex.EncodeToString(nonce[:])
	run = NamespaceRun{RunID: id, Workload: "namespace-create", Catalog: history.RedactText(history.SafeURL(c.BaseURL), "", c.Token),
		Parent: cfg.Parent, Clients: cfg.Clients, Rounds: cfg.Rounds}
	c = c.WithRecorder(nil)
	if response, e := c.Config(ctx); e != nil {
		return run, workloadResponseError("namespace config", c, response)
	}
	if response := c.CreateNamespace(ctx, cfg.Parent); response.Outcome() != "ok" && response.Status != 409 {
		return run, workloadResponseError("namespace parent", c, response)
	}
	value, err := runRecord(run, nil)
	if err != nil {
		return run, err
	}
	rec.Record(history.Op{Type: history.Meta, Process: -1, F: "run", Value: value})
	c = c.WithRecorder(rec)
	for round := 0; round < cfg.Rounds && ctx.Err() == nil; round++ {
		name := NamespaceName(cfg.Parent, id, round)
		label := strconv.Itoa(round)
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(cfg.Clients)
		done.Add(cfg.Clients)
		for p := 0; p < cfg.Clients; p++ {
			go func(p int) {
				defer done.Done()
				owner := NamespaceOwner(round, p)
				marker, _ := json.Marshal(NamespaceMarker{Owner: owner})
				ready.Done()
				<-start
				if ctx.Err() != nil {
					return
				}
				call := history.WithOperation(ctx, history.Op{Process: p, Phase: "workload", Round: label, Value: marker})
				c.CreateNamespaceWithProperties(call, name, map[string]string{NamespaceOwnerKey: owner})
			}(p)
		}
		ready.Wait()
		close(start)
		done.Wait()
		c.LoadNamespace(history.WithOperation(ctx, history.Op{Process: -1, Phase: "final", Round: label}), name)
	}
	return run, ctx.Err()
}
