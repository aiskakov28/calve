package checker

import (
	"context"
	"encoding/json"
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

func TestAtomicConcurrentReadFailuresAreVisible(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	client := rest.NewClient(fake.Server.URL, "", "", time.Second)
	read := make(chan struct{})
	var once sync.Once
	client.HTTP.Transport = atomicTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && history.Operation(r.Context()).Phase == "workload" {
			once.Do(func() { close(read) })
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: http.NoBody}, nil
		}
		if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
			select {
			case <-read:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	rec := history.NewRecorder()
	_, err := workload.RunAtomic(context.Background(), client, workload.AtomicConfig{
		Namespace: "test", Tables: 2, Clients: 1, PerClient: 1, Readers: 1, ReadInterval: time.Millisecond,
	}, rec)
	if err != nil {
		t.Fatal(err)
	}
	res, err := CheckAtomic(rec.Ops())
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != Inconclusive || !res.ReadCoverageMissing || res.ConcurrentReads != 0 ||
		res.WorkloadReads == 0 || res.OK != 1 || len(res.Reasons) != 1 {
		t.Fatalf("all concurrent reads failed, but the result did not retain the coverage limitation: %+v", res)
	}
}

func TestAtomicSuccessfulReadsOutsideTransactionsDoNotSupplyCoverage(t *testing.T) {
	original := atomicHistory(t, fakecatalog.Bugs{}, 1, 1)
	baseline, txn, maxID := -1, -1, 0
	for i, op := range original {
		if op.Type == history.Invoke && op.Phase == "baseline" && baseline < 0 {
			baseline = i
		}
		if op.Type == history.Invoke && op.F == "atomic-txn" {
			txn = i
		}
		if op.ID > maxID {
			maxID = op.ID
		}
	}
	ops := append([]history.Op(nil), original[:txn]...)
	for _, op := range original[baseline : baseline+2] {
		op.ID, op.Phase = maxID+1, "workload"
		ops = append(ops, op)
	}
	ops = stampAtomic(append(ops, original[txn:]...))
	var manifest atomicManifest
	json.Unmarshal(ops[0].Value, &manifest)
	manifest.Readers, manifest.ReadIntervalNS = 1, int64(time.Millisecond)
	ops[0].Value = snapshotRaw(manifest)
	res := requireAtomic(t, ops, Inconclusive)
	if res.WorkloadReads != 1 || res.ConcurrentReads != 0 || !res.ReadCoverageMissing {
		t.Fatal(res)
	}
	// Existing histories and intentionally disabled readers keep their prior
	// final-state checks, without claiming concurrent coverage.
	manifest.Readers, manifest.ReadIntervalNS = 0, 0
	ops[0].Value = snapshotRaw(manifest)
	res = requireAtomic(t, ops, Valid)
	if res.ReadCoverageMissing || res.ConcurrentReads != 0 {
		t.Fatal(res)
	}
}
