package checker

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

type maintenanceGateWriter func(context.Context, string) error

func (f maintenanceGateWriter) Put(ctx context.Context, uri string, _ []byte) error {
	return f(ctx, uri)
}

// Force the first expiration's GET -> append 3 ACK -> expiration publication
// -> next GET ordering. Keep append 4 behind that GET. Both the correct server
// and the planted modes execute the same real HTTP schedule, without sleeps or
// rewritten responses; the workload's normal expire interval is not the gate.
func maintenanceRaceHistory(t *testing.T, bugs fakecatalog.Bugs, model string) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	expiring, appended, returned, observed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstExpiration, firstReturn, firstObservation sync.Once
	fake.SetBeforeExpiration(func(ctx context.Context) {
		firstExpiration.Do(func() {
			close(expiring)
			select {
			case <-appended:
			case <-ctx.Done():
			}
		})
	})
	store := maintenanceGateWriter(func(ctx context.Context, uri string) error {
		var gate <-chan struct{}
		switch {
		case strings.HasSuffix(uri, "-3.avro"):
			gate = expiring
		case strings.HasSuffix(uri, "-4.avro"):
			// This runs only after append 3's completion was recorded.
			close(appended)
			gate = observed
		default:
			return nil
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	client := rest.NewClient(fake.Server.URL, "", "", 3*time.Second)
	client.HTTP.Transport = controlTransport(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && history.Operation(r.Context()).Process == 1 {
			if r.Method == http.MethodPost {
				firstReturn.Do(func() { close(returned) })
			} else if r.Method == http.MethodGet {
				select {
				case <-returned:
					firstObservation.Do(func() { close(observed) })
				default:
				}
			}
		}
		return response, err
	})
	rec := history.NewRecorder()
	cfg := workload.MaintenanceConfig{Namespace: "test", Appends: 4, ExpireInterval: time.Millisecond, ManifestRoot: t.TempDir(), Model: model}
	if _, err := workload.RunMaintenance(ctx, client, cfg, store, rec); err != nil {
		t.Fatal(err)
	}
	fake.Close()
	return replayControlHistory(t, rec.Ops())
}

func TestMaintenanceCheckerCorrectCatalog(t *testing.T) {
	for _, model := range []string{MaintenanceModelVersion, MaintenanceCurrentModelVersion} {
		t.Run(model, func(t *testing.T) {
			res, err := CheckMaintenance(maintenanceRaceHistory(t, fakecatalog.Bugs{}, model))
			if err != nil || res.Verdict != Valid || len(res.Anomalies) != 0 || res.AppendsOK != 4 || res.ExpirationsOK == 0 || res.RacingExpirations == 0 {
				t.Fatal(res, err)
			}
		})
	}
}

func TestMaintenanceCheckerPlantedBugs(t *testing.T) {
	for _, model := range []string{MaintenanceModelVersion, MaintenanceCurrentModelVersion} {
		for _, tt := range []struct {
			name  string
			bugs  fakecatalog.Bugs
			kinds []string
		}{
			{"expire from stale metadata", fakecatalog.Bugs{ExpireFromStaleMetadata: true}, []string{"lost-snapshot", "main-regressed"}},
			{"expire removes head", fakecatalog.Bugs{ExpireRemovesHead: true}, []string{"lost-snapshot", "dangling-main"}},
			{"expiration rejected after applying", fakecatalog.Bugs{RejectAppliedExpiration: true}, []string{"phantom-failed"}},
			{"append rejected after applying", fakecatalog.Bugs{RejectAppliedSnapshots: true}, []string{"phantom-failed"}},
			{"unrequested snapshot", fakecatalog.Bugs{UnexpectedSnapshots: true}, []string{"unexpected"}},
		} {
			t.Run(fmt.Sprintf("%s/%s", model, tt.name), func(t *testing.T) {
				res, err := CheckMaintenance(maintenanceRaceHistory(t, tt.bugs, model))
				if err != nil {
					t.Fatal(err)
				}
				// This profile ignores historical retention. A rejected
				// expiration removing only old snapshots supplies no finding
				// and no acknowledged race coverage, so it stays inconclusive.
				if model == MaintenanceCurrentModelVersion && tt.bugs.RejectAppliedExpiration {
					if res.Verdict != Inconclusive || len(res.Anomalies) != 0 || res.ExpirationsOK != 0 {
						t.Fatal(res)
					}
					return
				}
				if res.Verdict != Invalid {
					t.Fatal(res)
				}
				got := maintenanceKinds(res)
				for _, kind := range tt.kinds {
					if model == MaintenanceCurrentModelVersion && kind == "lost-snapshot" {
						continue
					}
					if got[kind] == 0 {
						t.Fatalf("missing %s: %+v", kind, res.Anomalies)
					}
				}
				for _, finding := range res.Anomalies {
					if finding.Basis != "catalog-contract" || len(finding.EvidenceIDs) == 0 {
						t.Fatal(finding)
					}
				}
			})
		}
	}
}
