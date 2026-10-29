package workload

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type atomicReaderTransport func(*http.Request) (*http.Response, error)

func (f atomicReaderTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAtomicReadersStopBeforeFinalObservations(t *testing.T) {
	for _, mode := range []string{"finish", "cancel", "paced"} {
		t.Run(mode, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ready := make(chan struct{})
			var once sync.Once
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			client.HTTP.Transport = atomicReaderTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
					select {
					case <-ready:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
					if mode == "cancel" {
						cancel()
						return nil, r.Context().Err()
					}
				}
				if r.Method == http.MethodGet && history.Operation(r.Context()).Phase == "workload" {
					if mode != "paced" {
						once.Do(func() { close(ready) })
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					response, err := http.DefaultTransport.RoundTrip(r)
					if strings.HasSuffix(r.URL.Path, "_t1") {
						once.Do(func() { close(ready) })
					}
					return response, err
				}
				return http.DefaultTransport.RoundTrip(r)
			})
			rec := history.NewRecorder()
			_, err := RunAtomic(ctx, client, AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1,
				Readers: 2, ReadInterval: time.Hour}, rec)
			if mode == "cancel" && !errors.Is(err, context.Canceled) || mode != "cancel" && err != nil {
				t.Fatal(err)
			}
			pending := map[int]bool{}
			reads, writes := 0, 0
			finalStarted := false
			for _, op := range rec.Ops() {
				if op.F == "atomic-txn" && op.Type == history.Invoke {
					writes++
				}
				if op.Phase == "final" {
					finalStarted = true
					if len(pending) != 0 {
						t.Fatal("final observations overlap readers")
					}
				}
				if op.F == "load-table" && op.Phase == "workload" {
					if finalStarted {
						t.Fatal("reader recorded after final observations")
					}
					if op.Type == history.Invoke {
						reads++
						pending[op.ID] = true
					} else {
						delete(pending, op.ID)
					}
				}
			}
			if reads == 0 || writes != 1 || len(pending) != 0 {
				t.Fatalf("%d reads, %d writes, %d pending", reads, writes, len(pending))
			}
		})
	}
}
