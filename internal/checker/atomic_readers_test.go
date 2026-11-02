package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func TestAtomicReadersObservePartialPublicationBeforeAcknowledgment(t *testing.T) {
	backend := fakecatalog.New(fakecatalog.Bugs{})
	defer backend.Close()
	target, _ := url.Parse(backend.Server.URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	published, observed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	backendClient := rest.NewClient(backend.Server.URL, "", "", time.Second)
	// A real HTTP frontend deliberately publishes A, waits for a reader to
	// observe old B, then publishes B before acknowledging the transaction.
	// The reader's transport signals the observation, once it holds the whole
	// response: the acknowledgment stops the readers, so a read still in flight
	// would be cancelled and recorded without a response.
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/transactions/commit") {
			var body struct {
				Changes []rest.TableChange `json:"table-changes"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Changes) != 2 {
				http.Error(w, "bad transaction", 400)
				return
			}
			for i, change := range body.Changes {
				if i == 1 {
					close(published)
					select {
					case <-observed:
					case <-r.Context().Done():
						return
					}
				}
				if response := backendClient.CommitTable(r.Context(), "test", change.Identifier.Name, change); response.Outcome() != "ok" {
					http.Error(w, "backend commit failed", 500)
					return
				}
			}
			w.WriteHeader(204)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer front.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := rest.NewClient(front.URL, "", "", 3*time.Second)
	client.HTTP.Transport = atomicTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || history.Operation(r.Context()).Phase != "workload" {
			return http.DefaultTransport.RoundTrip(r)
		}
		select {
		case <-published:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && strings.HasSuffix(r.URL.Path, "_t1") {
			response.Body = readSignal{response.Body, func() { once.Do(func() { close(observed) }) }}
		}
		return response, err
	})
	rec := history.NewRecorder()
	if _, err := workload.RunAtomic(ctx, client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1,
		Readers: 1, ReadInterval: time.Millisecond}, rec); err != nil {
		t.Fatal(err)
	}
	result := requireAtomic(t, rec.Ops(), Invalid)
	if len(result.Anomalies) != 1 || result.Anomalies[0].Kind != "non-atomic-visibility" || len(result.Anomalies[0].Tables) != 2 || result.OK != 1 {
		t.Fatal(result)
	}
	// Assert actual intervals, not just phase labels: both ordered GETs start
	// before the ACK, and every reader completes before the first final GET.
	ack, final := -1, len(rec.Ops())
	for _, op := range rec.Ops() {
		if op.F == "atomic-txn" && op.Type == history.OK {
			ack = op.Index
		}
		if op.Phase == "final" && op.Index < final {
			final = op.Index
		}
	}
	for _, op := range rec.Ops() {
		if op.F == "load-table" && op.Phase == "workload" {
			isWitness := false
			for _, id := range result.Anomalies[0].EvidenceIDs {
				isWitness = isWitness || op.ID == id
			}
			if op.Index >= final || isWitness && op.Type == history.Invoke && op.Index >= ack {
				t.Fatalf("reader outside expected interval: %+v", op)
			}
		}
	}
	var saved bytes.Buffer
	if err := history.WriteJSONL(&saved, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	front.Close()
	backend.Close()
	ops, err := history.ReadJSONL(&saved)
	if err != nil {
		t.Fatal(err)
	}
	replayed := requireAtomic(t, ops, Invalid)
	a, _ := json.Marshal(result)
	b, _ := json.Marshal(replayed)
	if !bytes.Equal(a, b) {
		t.Fatal("offline result changed")
	}
}

// readSignal calls done when its body has been read to the end.
type readSignal struct {
	io.ReadCloser
	done func()
}

func (b readSignal) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.done()
	}
	return n, err
}

func TestAtomicSameTableRegressionUsesReadIntervals(t *testing.T) {
	original := atomicHistory(t, fakecatalog.Bugs{UnknownAppliedTransactions: true}, 1, 1)
	first, maxID := -1, 0
	for i, op := range original {
		if op.Phase == "final" && first < 0 {
			first = i
		}
		if op.ID > maxID {
			maxID = op.ID
		}
	}
	negative := [2]history.Op{original[first], original[first+1]}
	var body map[string]any
	json.Unmarshal(negative[1].Response.Body, &body)
	body["metadata"].(map[string]any)["properties"] = map[string]string{}
	response := *negative[1].Response
	response.Body = snapshotRaw(body)
	negative[1].Response = &response
	for i := range negative {
		negative[i].ID, negative[i].Phase = maxID+1, "workload"
	}
	events := []history.Op{original[first], original[first+1], negative[0], negative[1]}
	for _, tt := range []struct {
		name  string
		order []int
		want  Verdict
	}{
		{"new then missing", []int{0, 1, 2, 3}, Invalid},
		{"overlapping", []int{0, 2, 1, 3}, Inconclusive},
		{"old then new", []int{2, 3, 0, 1}, Valid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := append([]history.Op(nil), original[:first]...)
			for _, i := range tt.order {
				ops = append(ops, events[i])
			}
			ops = append(ops, original[first+2:]...)
			res := requireAtomic(t, stampAtomic(ops), tt.want)
			if tt.want == Invalid && (len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "single-table-regression" ||
				res.Anomalies[0].Basis != "fresh-read-model" || len(res.Anomalies[0].Tables) != 1 || len(res.Anomalies[0].EvidenceIDs) != 3) {
				t.Fatal(res)
			}
		})
	}
}
