package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

func atomicHistory(t *testing.T, bugs fakecatalog.Bugs, clients, perClient int) []history.Op {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	rec := history.NewRecorder()
	client := rest.NewClient(fake.Server.URL, "", "atomic-private-token", time.Second)
	if _, err := workload.RunAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 3, Clients: clients, PerClient: perClient}, rec); err != nil {
		t.Fatal(err)
	}
	fake.Close()
	var data bytes.Buffer
	if err := history.WriteJSONL(&data, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(data.String(), "atomic-private-token") {
		t.Fatal("credential leaked")
	}
	ops, err := history.ReadJSONL(&data)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func requireAtomic(t *testing.T, ops []history.Op, want Verdict) AtomicResult {
	t.Helper()
	result, err := CheckAtomic(ops)
	if err != nil || result.Verdict != want {
		t.Fatalf("want %s; got %+v; error %v", want, result, err)
	}
	return result
}

func TestAtomicCheckerPlantedEffects(t *testing.T) {
	for _, tt := range []struct {
		name        string
		bugs        fakecatalog.Bugs
		want        Verdict
		kind, basis string
	}{
		{"correct", fakecatalog.Bugs{}, Valid, "", ""},
		{"lost", fakecatalog.Bugs{LoseTransactions: true}, Invalid, "lost", "catalog-contract"},
		{"partial acknowledged", fakecatalog.Bugs{PartialTransactions: true}, Invalid, "lost", "catalog-contract"},
		{"rejected applied", fakecatalog.Bugs{RejectAppliedTransactions: true}, Invalid, "phantom-failed", "catalog-contract"},
		{"rejected partial", fakecatalog.Bugs{RejectAppliedTransactions: true, PartialTransactions: true}, Invalid, "phantom-failed", "catalog-contract"},
		{"unknown applied", fakecatalog.Bugs{UnknownAppliedTransactions: true}, Valid, "", ""},
		{"unknown absent", fakecatalog.Bugs{UnknownUnappliedTransactions: true}, Inconclusive, "", ""},
		{"unknown partial", fakecatalog.Bugs{UnknownAppliedTransactions: true, PartialTransactions: true}, Invalid, "non-atomic-visibility", "atomic-visibility-model"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := atomicHistory(t, tt.bugs, 3, 4)
			res := requireAtomic(t, ops, tt.want)
			if res.OK+res.Failed+res.Unknown != 12 || len(res.Transactions) != 12 {
				t.Fatal(res)
			}
			if tt.kind != "" {
				if len(res.Anomalies) != 12 {
					t.Fatal(res)
				}
				for _, finding := range res.Anomalies {
					if finding.Kind != tt.kind || finding.Basis != tt.basis || len(finding.EvidenceIDs) < 2 {
						t.Fatal(finding)
					}
				}
			} else if len(res.Anomalies) != 0 {
				t.Fatal(res)
			}
			for _, txn := range res.Transactions {
				if tt.bugs.UnknownAppliedTransactions && txn.Outcome != history.Info {
					t.Fatal("source outcome rewritten", txn)
				}
			}
			if tt.name == "unknown applied" && (res.ResolvedCommitted != 12 || res.Unknown != 12 || res.Unresolved != 0) {
				t.Fatal(res)
			}
			if tt.name == "unknown absent" && (res.Unresolved != 12 || len(res.Anomalies) != 0) {
				t.Fatal(res)
			}
		})
	}
}

// These helpers construct synthetic alternative schedules from HTTP records;
// they are checker fixtures, not claimed live-catalog reproduction evidence.
func stampAtomic(ops []history.Op) []history.Op {
	for i := range ops {
		ops[i].Index = i
		ops[i].Time = int64(i)
	}
	return ops
}

func appendAtomicObservation(ops []history.Op, pair []history.Op, phase string) []history.Op {
	maxID := 0
	for _, op := range ops {
		if op.ID > maxID {
			maxID = op.ID
		}
	}
	for _, source := range pair {
		source.ID = maxID + 1
		source.Phase = phase
		ops = append(ops, source)
	}
	return stampAtomic(ops)
}

func TestAtomicUnknownNegativeBeforePositiveIsNotPartial(t *testing.T) {
	ops := atomicHistory(t, fakecatalog.Bugs{UnknownAppliedTransactions: true, PartialTransactions: true}, 1, 1)
	var prefix []history.Op
	var pairs [][]history.Op
	for i := 0; i < len(ops); i++ {
		if ops[i].Phase == "final" {
			pairs = append(pairs, append([]history.Op(nil), ops[i:i+2]...))
			i++
		} else {
			prefix = append(prefix, ops[i])
		}
	}
	// Only the first table has the effect. Observe missing tables first.
	for _, i := range []int{1, 2, 0} {
		prefix = append(prefix, pairs[i]...)
	}
	prefix = stampAtomic(prefix)
	res := requireAtomic(t, prefix, Inconclusive)
	if len(res.Anomalies) != 0 || res.Unresolved != 1 {
		t.Fatal(res)
	}
	// A later missing-table read after that positive evidence is a model lead.
	prefix = appendAtomicObservation(prefix, pairs[1], "recovery")
	res = requireAtomic(t, prefix, Invalid)
	if res.Anomalies[0].Basis != "atomic-visibility-model" {
		t.Fatal(res)
	}
}

func TestAtomicFailedReadRecoveryAndIdentity(t *testing.T) {
	t.Run("failed final recovers", func(t *testing.T) {
		ops := atomicHistory(t, fakecatalog.Bugs{UnknownAppliedTransactions: true}, 1, 1)
		var last int
		for i, op := range ops {
			if op.Type == history.Invoke && op.Phase == "final" {
				last = i
			}
		}
		pair := append([]history.Op(nil), ops[last:last+2]...)
		done := &ops[last+1]
		done.Type, done.Status, done.Error = history.Info, 503, "unavailable"
		done.Response = &history.Response{Status: 503, Body: []byte("{}")}
		requireAtomic(t, ops, Inconclusive)
		ops = appendAtomicObservation(ops, pair, "recovery")
		res := requireAtomic(t, ops, Valid)
		if res.Unknown != 1 || res.ResolvedCommitted != 1 {
			t.Fatal(res)
		}
	})
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed UUID/lost=%v", lost), func(t *testing.T) {
			ops := atomicHistory(t, fakecatalog.Bugs{LoseTransactions: lost}, 1, 1)
			last := len(ops) - 1
			var body map[string]any
			json.Unmarshal(ops[last].Response.Body, &body)
			body["metadata"].(map[string]any)["table-uuid"] = "recreated"
			ops[last].Response.Body, _ = json.Marshal(body)
			want := Inconclusive
			if lost {
				want = Invalid
			}
			res := requireAtomic(t, ops, want)
			for _, finding := range res.Anomalies {
				for _, id := range finding.EvidenceIDs {
					if id == ops[last].ID {
						t.Fatal("new incarnation cited", finding)
					}
				}
			}
		})
	}
}

func TestAtomicRejectsUnmodeledRequests(t *testing.T) {
	for _, mode := range []string{"missing table", "schema update", "wrong guard", "duplicate table", "wrong marker", "duplicate marker", "origin", "status", "reader settings"} {
		t.Run(mode, func(t *testing.T) {
			ops := atomicHistory(t, fakecatalog.Bugs{}, 1, 2)
			var indexes []int
			for i, op := range ops {
				if op.Type == history.Invoke && op.F == "atomic-txn" {
					indexes = append(indexes, i)
				}
			}
			index := indexes[0]
			var body struct {
				Changes []rest.TableChange `json:"table-changes"`
			}
			json.Unmarshal(ops[index].Request.Body, &body)
			switch mode {
			case "missing table":
				body.Changes = body.Changes[:2]
			case "schema update":
				body.Changes[0].Updates[0] = map[string]any{"action": "set-current-schema", "schema-id": 0}
			case "wrong guard":
				body.Changes[0].Requirements[0]["uuid"] = "wrong"
			case "duplicate table":
				body.Changes[1] = body.Changes[0]
			case "wrong marker":
				body.Changes[0].Updates[0]["updates"] = map[string]string{"different": "1"}
			case "duplicate marker":
				id := ops[indexes[1]].ID
				for i := range ops {
					if ops[i].ID == id {
						ops[i].Value = ops[index].Value
					}
				}
				ops[indexes[1]].Request.Body = ops[index].Request.Body
			case "origin":
				ops[index].Request.URL = strings.Replace(ops[index].Request.URL, "http://", "http://other.", 1)
			case "status":
				ops[index+1].Response.Status = 502
			case "reader settings":
				var m atomicManifest
				json.Unmarshal(ops[0].Value, &m)
				m.Readers = 1 // Enabled readers require a positive interval.
				ops[0].Value = snapshotRaw(m)
			}
			ops[index].Request.Body, _ = json.Marshal(body)
			if _, err := CheckAtomic(ops); err == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

type atomicTransport func(*http.Request) (*http.Response, error)

func (f atomicTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAtomicRealTimeoutReadsStraddleWholeCommit(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	release, applied := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var mu sync.Mutex
	loads := map[string]int{}
	posts := 0
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		isTxn := r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/transactions/commit")
		if isTxn {
			mu.Lock()
			posts++
			mu.Unlock()
			<-release
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tables/") {
			mu.Lock()
			loads[r.URL.Path]++
			n := loads[r.URL.Path]
			mu.Unlock()
			// On the second table's final GET, publish both tables together.
			// The first final GET has already observed the old state.
			if strings.HasSuffix(r.URL.Path, "_t1") && n == 2 {
				unblock()
				select {
				case <-applied:
				case <-time.After(2 * time.Second):
					http.Error(w, "not applied", 503)
					return
				}
			}
		}
		req, _ := http.NewRequest(r.Method, fake.Server.URL+r.URL.RequestURI(), bytes.NewReader(body))
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer response.Body.Close()
		w.WriteHeader(response.StatusCode)
		io.Copy(w, response.Body)
		if isTxn {
			close(applied)
		}
	}))
	defer front.Close()
	defer unblock()
	client := rest.NewClient(front.URL, "", "", 3*time.Second)
	client.HTTP.Transport = atomicTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/transactions/commit") {
			ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
			defer cancel()
			r = r.Clone(ctx)
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	rec := history.NewRecorder()
	if _, err := workload.RunAtomic(context.Background(), client, workload.AtomicConfig{Namespace: "test", Tables: 2, Clients: 1, PerClient: 1}, rec); err != nil {
		t.Fatal(err)
	}
	res := requireAtomic(t, rec.Ops(), Inconclusive)
	if res.Unknown != 1 || len(res.Anomalies) != 0 {
		t.Fatal(res)
	}
	target := ""
	for _, op := range rec.Ops() {
		if op.Type == history.Invoke && op.Phase == "baseline" {
			target = op.Request.URL
			break
		}
	}
	ctx := history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "recovery"})
	client.WithRecorder(rec).ReloadTable(ctx, target)
	front.Close()
	fake.Close()
	res = requireAtomic(t, rec.Ops(), Valid)
	if res.Unknown != 1 || res.ResolvedCommitted != 1 || posts != 1 {
		t.Fatal(res, posts)
	}
}

func TestAtomicEvidenceIDsAreStable(t *testing.T) {
	ops := atomicHistory(t, fakecatalog.Bugs{UnknownAppliedTransactions: true, PartialTransactions: true}, 1, 1)
	first := requireAtomic(t, ops, Invalid)
	second := requireAtomic(t, ops, Invalid)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatal("nondeterministic offline result")
	}
	for _, finding := range first.Anomalies {
		if !sort.IntsAreSorted(finding.EvidenceIDs) {
			t.Fatal(finding)
		}
	}
}

func TestAtomicUnknownCompletionFormsAndDisappearingEffects(t *testing.T) {
	for _, status := range []int{0, 200, 500, 502, 503, 504, -1} {
		t.Run(fmt.Sprintf("status=%d", status), func(t *testing.T) {
			ops := atomicHistory(t, fakecatalog.Bugs{UnknownAppliedTransactions: true}, 1, 1)
			index := -1
			var firstFinal []history.Op
			for i, op := range ops {
				if op.F == "atomic-txn" && op.Type == history.Info {
					index = i
				}
				if len(firstFinal) == 0 && op.Phase == "final" && op.Type == history.Invoke {
					firstFinal = append(firstFinal, ops[i:i+2]...)
				}
			}
			if status == -1 {
				ops = stampAtomic(append(ops[:index], ops[index+1:]...))
			} else {
				ops[index].Status = status
				ops[index].Error = "uncertain response"
				if status == 0 {
					ops[index].Response = nil
				} else {
					ops[index].Response = &history.Response{Status: status, Body: []byte("{")}
				}
			}
			res := requireAtomic(t, ops, Valid)
			if res.Unknown != 1 || res.ResolvedCommitted != 1 || res.Transactions[0].Outcome != history.Info {
				t.Fatal(res)
			}
			// Synthetic later read loses the previously established effect.
			var body map[string]any
			json.Unmarshal(firstFinal[1].Response.Body, &body)
			body["metadata"].(map[string]any)["properties"] = map[string]string{}
			firstFinal[1].Response = &history.Response{Status: 200, Body: snapshotRaw(body)}
			ops = appendAtomicObservation(ops, firstFinal, "recovery")
			res = requireAtomic(t, ops, Invalid)
			if res.Anomalies[0].Kind != "lost" {
				t.Fatal(res)
			}
			for _, proof := range res.Transactions[0].EvidenceIDs {
				found := false
				for _, id := range res.Anomalies[0].EvidenceIDs {
					found = found || proof == id
				}
				if !found {
					t.Fatal("lost finding omitted commitment proof", res)
				}
			}
		})
	}
}

func TestAtomicUnknownPeerCannotHideAcknowledgedLoss(t *testing.T) {
	ops := atomicHistory(t, fakecatalog.Bugs{LoseTransactions: true}, 1, 2)
	changed := false
	for i := range ops {
		if ops[i].F == "atomic-txn" && ops[i].Type == history.OK && !changed {
			ops[i].Type, ops[i].Status, ops[i].Error = history.Info, 503, "unknown peer"
			ops[i].Response = &history.Response{Status: 503, Body: []byte("{}")}
			changed = true
		}
	}
	res := requireAtomic(t, ops, Invalid)
	if res.Unknown != 1 || res.Unresolved != 1 || len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "lost" {
		t.Fatal(res)
	}
}

func TestAtomicRejectedPresenceAndUnexpectedValues(t *testing.T) {
	for _, value := range []string{"0", "", "arbitrary"} {
		t.Run("rejected/"+value, func(t *testing.T) {
			ops := atomicHistory(t, fakecatalog.Bugs{RejectAppliedTransactions: true}, 1, 1)
			var key string
			for _, op := range ops {
				if op.F == "atomic-txn" && op.Type == history.Invoke {
					var label struct{ Key string }
					json.Unmarshal(op.Value, &label)
					key = label.Key
				}
			}
			for i := range ops {
				if ops[i].Phase == "final" && ops[i].Type == history.OK {
					var body map[string]any
					json.Unmarshal(ops[i].Response.Body, &body)
					body["metadata"].(map[string]any)["properties"] = map[string]string{key: value}
					ops[i].Response.Body = snapshotRaw(body)
				}
			}
			res := requireAtomic(t, ops, Invalid)
			if len(res.Anomalies) != 1 || res.Anomalies[0].Kind != "phantom-failed" {
				t.Fatal(res)
			}
		})
	}
	ops := atomicHistory(t, fakecatalog.Bugs{}, 1, 1)
	var manifest atomicManifest
	json.Unmarshal(ops[0].Value, &manifest)
	for i := range ops {
		if ops[i].Phase == "final" && ops[i].Type == history.OK {
			var body map[string]any
			json.Unmarshal(ops[i].Response.Body, &body)
			properties := body["metadata"].(map[string]any)["properties"].(map[string]any)
			properties[atomicPrefix+manifest.RunID+".unexpected-z"] = "0"
			properties[atomicPrefix+manifest.RunID+".unexpected-a"] = ""
			body["config"] = map[string]string{"s3.secret-access-key": "[REDACTED]"}
			ops[i].Response.Body = snapshotRaw(body)
			break
		}
	}
	a := requireAtomic(t, ops, Invalid)
	b := requireAtomic(t, ops, Invalid)
	rawA, _ := json.Marshal(a)
	rawB, _ := json.Marshal(b)
	if !bytes.Equal(rawA, rawB) || len(a.Anomalies) != 2 {
		t.Fatal(a, b)
	}
}
