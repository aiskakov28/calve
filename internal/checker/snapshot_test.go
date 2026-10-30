package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
	"github.com/anishathalye/porcupine"
)

func snapshotPtr(n int64) *int64 { return &n }
func snapshotRaw(v any) []byte   { b, _ := json.Marshal(v); return b }

func testSnapshot(id int64, parent *int64, sequence int64) map[string]any {
	s := map[string]any{
		"snapshot-id": id, "sequence-number": sequence, "timestamp-ms": int64(1700000000000) + sequence,
		"manifest-list": fmt.Sprintf("s3://test/snapshot-%d.avro", id),
		"summary":       map[string]string{"operation": "append"}, "schema-id": 0,
	}
	if parent != nil {
		s["parent-snapshot-id"] = *parent
	}
	return s
}

func testSnapshotChange(uuid string, id int64, parent *int64, sequence int64) rest.TableChange {
	return rest.TableChange{
		Requirements: []map[string]any{
			{"type": "assert-table-uuid", "uuid": uuid},
			{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent},
		},
		Updates: []map[string]any{
			{"action": "add-snapshot", "snapshot": testSnapshot(id, parent, sequence)},
			{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id},
		},
	}
}

func testSnapshotMetadata(current *int64, records ...map[string]any) map[string]any {
	sequence := int64(0)
	for _, record := range records {
		if seq := record["sequence-number"].(int64); seq > sequence {
			sequence = seq
		}
	}
	refs := map[string]any{}
	if current != nil {
		refs["main"] = map[string]any{"snapshot-id": *current, "type": "branch"}
	}
	return map[string]any{"metadata": map[string]any{
		"format-version": 2, "table-uuid": "u", "last-sequence-number": sequence,
		"current-snapshot-id": current, "refs": refs, "snapshots": records,
	}}
}

type snapshotTrace struct {
	ops   []history.Op
	calls map[int]history.Op
}

func newSnapshotTrace(commits int) *snapshotTrace {
	s := &snapshotTrace{calls: map[int]history.Op{}}
	s.record(history.Op{Type: history.Meta, F: "run", Value: snapshotRaw(SnapshotManifest{
		RunID: "snapshots", Workload: "snapshot-cas", Model: SnapshotModelVersion,
		Catalog: "https://catalog.test/catalog", Namespace: "group.ns", Table: "table", Commits: commits,
	})})
	s.call(1, "load-table", "baseline", nil)
	s.reply(1, history.OK, 200, testSnapshotMetadata(nil))
	return s
}

func (s *snapshotTrace) record(op history.Op) {
	op.Version, op.Index = history.Version, len(s.ops)
	// Intentionally tie all clock values; recorded call/return order still
	// carries real-time precedence and must not become artificial concurrency.
	op.Time = 0
	s.ops = append(s.ops, op)
}

func (s *snapshotTrace) call(id int, f, phase string, body any) {
	method := http.MethodGet
	var raw []byte
	if body != nil {
		method, raw = http.MethodPost, snapshotRaw(body)
	}
	op := history.Op{Type: history.Invoke, ID: id, F: f, Phase: phase, Process: id,
		Request: &history.Request{Method: method, URL: "https://catalog.test/catalog/v1/prefix/x/namespaces/group%1Fns/tables/table", Body: raw}}
	s.calls[id] = op
	s.record(op)
}

func (s *snapshotTrace) reply(id int, outcome history.Type, status int, body any) {
	op := s.calls[id]
	op.Type, op.Request, op.Status = outcome, nil, status
	if status != 0 {
		op.Response = &history.Response{Status: status, Body: snapshotRaw(body)}
	}
	if outcome != history.OK {
		op.Error = "test response"
	}
	s.record(op)
}

func (s *snapshotTrace) final(id int, body any) {
	s.call(id, "load-table", "final", nil)
	s.reply(id, history.OK, 200, body)
}

func checkSnapshotTrace(t *testing.T, ops []history.Op, want Verdict) SnapshotResult {
	t.Helper()
	var saved bytes.Buffer
	if err := history.WriteJSONL(&saved, ops); err != nil {
		t.Fatal(err)
	}
	replayed, err := history.ReadJSONL(&saved)
	if err != nil {
		t.Fatal(err)
	}
	res, err := CheckSnapshotHistory(replayed, time.Second)
	if err != nil || res.Verdict != want {
		t.Fatalf("want %s, got %+v, error %v", want, res, err)
	}
	return res
}

func TestSnapshotLinearizabilityCAS(t *testing.T) {
	first := testSnapshot(11, nil, 1)
	one := testSnapshotMetadata(snapshotPtr(11), first)
	t.Run("one winner in overlapping pair", func(t *testing.T) {
		s := newSnapshotTrace(2)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, nil, 1))
		s.reply(3, history.Fail, 409, nil)
		s.reply(2, history.OK, 200, one)
		s.final(4, one)
		checkSnapshotTrace(t, s.ops, Valid)
	})
	t.Run("both winners cannot be ordered", func(t *testing.T) {
		s := newSnapshotTrace(2)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, nil, 1))
		s.reply(2, history.OK, 200, one)
		two := testSnapshotMetadata(snapshotPtr(12), testSnapshot(12, nil, 1))
		s.reply(3, history.OK, 200, two)
		s.final(4, two)
		res := checkSnapshotTrace(t, s.ops, Invalid)
		if res.Search != porcupine.Illegal || len(res.Anomalies) != 1 {
			t.Fatal(res)
		}
	})
	t.Run("fresh CAS can follow winner", func(t *testing.T) {
		s := newSnapshotTrace(2)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.reply(2, history.OK, 200, one)
		s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 2))
		two := testSnapshotMetadata(snapshotPtr(12), first, testSnapshot(12, snapshotPtr(11), 2))
		s.reply(3, history.OK, 200, two)
		s.final(4, two)
		checkSnapshotTrace(t, s.ops, Valid)
	})
	t.Run("stale guard succeeds", func(t *testing.T) {
		s := newSnapshotTrace(2)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.reply(2, history.OK, 200, one)
		s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, nil, 2))
		two := testSnapshotMetadata(snapshotPtr(12), first, testSnapshot(12, nil, 2))
		s.reply(3, history.OK, 200, two)
		s.final(4, two)
		checkSnapshotTrace(t, s.ops, Invalid)
	})
}

func TestSnapshotLinearizabilityReads(t *testing.T) {
	one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
	t.Run("old read overlaps commit", func(t *testing.T) {
		s := newSnapshotTrace(1)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.call(3, "load-table", "workload", nil)
		s.reply(2, history.OK, 200, one)
		s.reply(3, history.OK, 200, testSnapshotMetadata(nil))
		s.final(4, one)
		checkSnapshotTrace(t, s.ops, Valid)
	})
	t.Run("old read follows completed commit", func(t *testing.T) {
		s := newSnapshotTrace(1)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.reply(2, history.OK, 200, one)
		s.final(3, testSnapshotMetadata(nil))
		checkSnapshotTrace(t, s.ops, Invalid)
	})
	t.Run("correct head but ancestor forgotten", func(t *testing.T) {
		s := newSnapshotTrace(2)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		s.reply(2, history.OK, 200, one)
		s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 2))
		second := testSnapshot(12, snapshotPtr(11), 2)
		s.reply(3, history.OK, 200, testSnapshotMetadata(snapshotPtr(12), testSnapshot(11, nil, 1), second))
		s.final(4, testSnapshotMetadata(snapshotPtr(12), second))
		checkSnapshotTrace(t, s.ops, Invalid)
	})
	t.Run("nonempty baseline", func(t *testing.T) {
		s := newSnapshotTrace(1)
		s.ops[2].Response.Body = snapshotRaw(one)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 2))
		two := testSnapshotMetadata(snapshotPtr(12), testSnapshot(11, nil, 1), testSnapshot(12, snapshotPtr(11), 2))
		s.reply(2, history.OK, 200, two)
		s.final(3, two)
		checkSnapshotTrace(t, s.ops, Valid)
	})
	t.Run("implicit main reference", func(t *testing.T) {
		s := newSnapshotTrace(1)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		implicit := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
		delete(implicit["metadata"].(map[string]any), "refs")
		s.reply(2, history.OK, 200, implicit)
		s.final(3, implicit)
		checkSnapshotTrace(t, s.ops, Valid)
	})
}

func TestSnapshotUnknownCommitMayApplyAfterClientReturn(t *testing.T) {
	one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			s := newSnapshotTrace(1)
			s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
			if !missing {
				s.reply(2, history.Info, 503, nil)
			}
			s.final(3, testSnapshotMetadata(nil)) // not evidence of rejection
			res := checkSnapshotTrace(t, s.ops, Inconclusive)
			if res.Unknown != 1 || res.Unresolved != 1 || len(res.Anomalies) != 0 {
				t.Fatal(res)
			}
			s.call(4, "load-table", "recovery", nil)
			s.reply(4, history.OK, 200, one) // server commits after early read
			res = checkSnapshotTrace(t, s.ops, Valid)
			if res.Unknown != 1 || res.ResolvedCommitted != 1 || res.Commits[0].Outcome != history.Info ||
				!reflect.DeepEqual(res.Commits[0].EvidenceIDs, []int{4}) {
				t.Fatal(res)
			}
			s.final(5, testSnapshotMetadata(nil)) // positive evidence cannot disappear
			checkSnapshotTrace(t, s.ops, Invalid)
		})
	}
}

func TestSnapshotUnknownDoesNotHideIndependentViolation(t *testing.T) {
	s := newSnapshotTrace(2)
	s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
	one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
	s.reply(2, history.OK, 200, one)
	s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 2))
	s.reply(3, history.Info, 0, nil)
	s.final(4, testSnapshotMetadata(nil))
	res := checkSnapshotTrace(t, s.ops, Invalid)
	if res.Unresolved != 1 {
		t.Fatal(res)
	}
}

func TestSnapshotOverlappingReadCanResolveUnknownCommit(t *testing.T) {
	s := newSnapshotTrace(2)
	s.call(2, "load-table", "workload", nil) // read starts first, observes later
	s.call(3, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
	s.reply(3, history.Info, 503, nil)
	first := testSnapshot(11, nil, 1)
	s.reply(2, history.OK, 200, testSnapshotMetadata(snapshotPtr(11), first))
	s.call(4, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 2))
	two := testSnapshotMetadata(snapshotPtr(12), first, testSnapshot(12, snapshotPtr(11), 2))
	s.reply(4, history.OK, 200, two)
	s.final(5, two)
	res := checkSnapshotTrace(t, s.ops, Valid)
	if res.ResolvedCommitted != 1 || !reflect.DeepEqual(res.Commits[0].EvidenceIDs, []int{2}) {
		t.Fatal(res)
	}
}

func TestSnapshotSearchTimeoutAndIncompleteCoverage(t *testing.T) {
	s := newSnapshotTrace(1)
	s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
	one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
	s.reply(2, history.OK, 200, one)
	s.final(3, one)
	res, err := checkSnapshotHistory(s.ops, time.Millisecond, func(_ porcupine.Model, _ []porcupine.Event, budget time.Duration) porcupine.CheckResult {
		if budget != time.Millisecond {
			t.Fatal("search lost its budget")
		}
		return porcupine.Unknown
	})
	if err != nil || res.Verdict != Inconclusive || !strings.Contains(strings.Join(res.Reasons, " "), "timed out") {
		t.Fatalf("%+v %v", res, err)
	}
	for _, budget := range []time.Duration{0, -time.Second} {
		if _, err := CheckSnapshotHistory(s.ops, budget); err == nil {
			t.Fatal("accepted unbounded timeout")
		}
	}
	checkSnapshotTrace(t, s.ops[:5], Inconclusive) // no final read
	s.ops[0].Value = snapshotRaw(SnapshotManifest{RunID: "snapshots", Workload: "snapshot-cas", Model: SnapshotModelVersion,
		Catalog: "https://catalog.test/catalog", Namespace: "group.ns", Table: "table", Commits: 2})
	checkSnapshotTrace(t, s.ops, Inconclusive)
}

func TestSnapshotHistoryRefusesUnmodeledOrMalformedEvidence(t *testing.T) {
	fresh := func() []history.Op {
		s := newSnapshotTrace(1)
		s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
		one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
		s.reply(2, history.OK, 200, one)
		s.final(3, one)
		return s.ops
	}
	tests := map[string]func([]history.Op){
		"property history": func(ops []history.Op) {
			ops[0].Value = bytes.ReplaceAll(ops[0].Value, []byte("snapshot-cas"), []byte("append"))
		},
		"multi-table endpoint": func(ops []history.Op) { ops[3].Request.URL = "https://catalog.test/catalog/v1/transactions/commit" },
		"different catalog": func(ops []history.Op) {
			ops[3].Request.URL = strings.Replace(ops[3].Request.URL, "catalog.test", "other.test", 1)
		},
		"different prefix": func(ops []history.Op) {
			ops[3].Request.URL = strings.Replace(ops[3].Request.URL, "prefix/x", "other", 1)
		},
		"different table":      func(ops []history.Op) { ops[3].Request.URL += "2" },
		"redirect":             func(ops []history.Op) { ops[4].Status, ops[4].Response.Status = 302, 302 },
		"status mismatch":      func(ops []history.Op) { ops[4].Response.Status = 503 },
		"bad version":          func(ops []history.Op) { ops[4].Version = 0 },
		"bad index":            func(ops []history.Op) { ops[4].Index++ },
		"backwards time":       func(ops []history.Op) { ops[3].Time = 10 },
		"duplicate completion": func(ops []history.Op) { ops[5] = ops[4] },
		"missing guard": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			c.Requirements = c.Requirements[:1]
			ops[3].Request.Body = snapshotRaw(c)
		},
		"missing null guard ID": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			delete(c.Requirements[1], "snapshot-id")
			ops[3].Request.Body = snapshotRaw(c)
		},
		"unsupported update": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			c.Updates = append(c.Updates, map[string]any{"action": "remove-snapshots", "snapshot-ids": []int64{1}})
			ops[3].Request.Body = snapshotRaw(c)
		},
		"unknown update field": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			c.Updates[1]["max-ref-age-ms"] = 1000
			ops[3].Request.Body = snapshotRaw(c)
		},
		"tag update": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			c.Updates[1]["type"] = "tag"
			ops[3].Request.Body = snapshotRaw(c)
		},
		"wrong parent": func(ops []history.Op) {
			c := testSnapshotChange("u", 11, nil, 1)
			c.Updates[0]["snapshot"] = testSnapshot(11, snapshotPtr(99), 1)
			ops[3].Request.Body = snapshotRaw(c)
		},
		"redacted request": func(ops []history.Op) {
			ops[3].Request.Body = bytes.ReplaceAll(ops[3].Request.Body, []byte("s3://test"), []byte("[REDACTED]"))
		},
		"redacted pending request": func(ops []history.Op) {
			ops[3].Request.Body = bytes.ReplaceAll(ops[3].Request.Body, []byte("s3://test"), []byte("[REDACTED]"))
			ops[4].Type, ops[4].Status, ops[4].Response = history.Info, 0, nil
		},
		"metadata format v3": func(ops []history.Op) {
			ops[2].Response.Body = bytes.Replace(ops[2].Response.Body, []byte(`"format-version":2`), []byte(`"format-version":3`), 1)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ops := fresh()
			mutate(ops)
			if res, err := CheckSnapshotHistory(ops, time.Second); err == nil {
				t.Fatalf("accepted unsupported evidence: %+v", res)
			}
		})
	}
	t.Run("UUID changes are outside experiment", func(t *testing.T) {
		ops := fresh()
		ops[6].Response.Body = bytes.ReplaceAll(ops[6].Response.Body, []byte(`"table-uuid":"u"`), []byte(`"table-uuid":"recreated"`))
		checkSnapshotTrace(t, ops, Inconclusive)
	})
	t.Run("failed final read is not empty state", func(t *testing.T) {
		ops := fresh()
		ops[6].Type, ops[6].Status, ops[6].Response.Status = history.Info, 503, 503
		ops[6].Response.Body = []byte("{}")
		ops[6].Error = "unavailable"
		checkSnapshotTrace(t, ops, Inconclusive)
	})
}

func TestSnapshotCheckerAgainstFakeCatalog(t *testing.T) {
	for _, tt := range []struct {
		name string
		bugs fakecatalog.Bugs
		want Verdict
	}{
		{"correct", fakecatalog.Bugs{}, Valid},
		{"lose acknowledged", fakecatalog.Bugs{LoseEvery: 1}, Invalid},
		{"partial publication", fakecatalog.Bugs{PartialSnapshots: true}, Invalid},
		{"rejected effect", fakecatalog.Bugs{RejectAppliedSnapshots: true}, Invalid},
		{"stale read", fakecatalog.Bugs{StaleSnapshotReads: true}, Invalid},
		{"unrequested snapshot", fakecatalog.Bugs{UnexpectedSnapshots: true}, Invalid},
		{"applied unknown", fakecatalog.Bugs{UnknownAppliedSnapshots: true}, Valid},
		{"absent unknown", fakecatalog.Bugs{UnknownUnappliedSnapshots: true}, Inconclusive},
		{"accepted stale guard", fakecatalog.Bugs{IgnoreSnapshotRequirements: true}, Invalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := fakecatalog.New(tt.bugs)
			defer fake.Close()
			client := rest.NewClient(fake.Server.URL, "", "", time.Second)
			if err := workload.SetupTable(context.Background(), client, "calve", "snapshots"); err != nil {
				t.Fatal(err)
			}
			commits := 1
			if tt.bugs.IgnoreSnapshotRequirements {
				commits = 2
			}
			rec := history.NewRecorder()
			rec.Record(history.Op{Type: history.Meta, F: "run", Value: snapshotRaw(SnapshotManifest{RunID: "fake",
				Workload: "snapshot-cas", Model: SnapshotModelVersion, Catalog: fake.Server.URL, Namespace: "calve", Table: "snapshots", Commits: commits})})
			client = client.WithRecorder(rec)
			ctx := history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "baseline"})
			meta, err := rest.ParseMetadata(client.LoadTable(ctx, "calve", "snapshots"))
			if err != nil {
				t.Fatal(err)
			}
			// Above 2^53, adjacent IDs must remain distinct in requests, reads,
			// redacted JSONL and the fake's own storage.
			for n := 0; n < commits; n++ {
				ctx = history.WithOperation(context.Background(), history.Op{Process: n, Phase: "workload"})
				client.CommitTable(ctx, "calve", "snapshots", testSnapshotChange(meta.TableUUID, int64(9007199254740993)+int64(n), nil, int64(n+1)*7))
			}
			ctx = history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "final"})
			client.LoadTable(ctx, "calve", "snapshots")
			fake.Close()
			res := checkSnapshotTrace(t, rec.Ops(), tt.want)
			if tt.name == "correct" && res.OK != commits {
				t.Fatal("correct fake rejected supported commits", res)
			}
			if tt.bugs.UnknownAppliedSnapshots && (res.Unknown != 1 || res.ResolvedCommitted != 1) {
				t.Fatal(res)
			}
		})
	}
}

func TestSnapshotRealHTTPTimeoutAndLateApplication(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	setup := rest.NewClient(fake.Server.URL, "", "", time.Second)
	if err := workload.SetupTable(context.Background(), setup, "calve", "snapshots"); err != nil {
		t.Fatal(err)
	}
	release, applied := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method == http.MethodPost {
			<-release
		}
		// Server work intentionally outlives the client's request context.
		req, _ := http.NewRequest(r.Method, fake.Server.URL+r.URL.RequestURI(), bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		if r.Method == http.MethodPost {
			close(applied)
		}
	}))
	defer front.Close()
	// On any early test failure release server work before Close waits for it.
	defer unblock()
	rec := history.NewRecorder()
	rec.Record(history.Op{Type: history.Meta, F: "run", Value: snapshotRaw(SnapshotManifest{RunID: "late",
		Workload: "snapshot-cas", Model: SnapshotModelVersion, Catalog: front.URL, Namespace: "calve", Table: "snapshots", Commits: 1})})
	client := rest.NewClient(front.URL, "", "", time.Second).WithRecorder(rec)
	ctx := history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "baseline"})
	meta, err := rest.ParseMetadata(client.LoadTable(ctx, "calve", "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	client.HTTP.Timeout = 100 * time.Millisecond
	ctx = history.WithOperation(context.Background(), history.Op{Process: 0, Phase: "workload"})
	response := client.CommitTable(ctx, "calve", "snapshots", testSnapshotChange(meta.TableUUID, 11, nil, 1))
	if response.Outcome() != "info" || response.Err == nil {
		t.Fatalf("request did not time out: %+v", response)
	}
	client.HTTP.Timeout = time.Second
	ctx = history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "final"})
	client.LoadTable(ctx, "calve", "snapshots")
	checkSnapshotTrace(t, rec.Ops(), Inconclusive)
	unblock()
	select {
	case <-applied:
	case <-time.After(3 * time.Second):
		t.Fatal("server work did not apply")
	}
	ctx = history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "recovery"})
	client.LoadTable(ctx, "calve", "snapshots")
	front.Close()
	fake.Close()
	res := checkSnapshotTrace(t, rec.Ops(), Valid)
	if res.Unknown != 1 || res.ResolvedCommitted != 1 || res.OK != 0 {
		t.Fatal(res)
	}
	attempts := 0
	for _, op := range rec.Ops() {
		if op.Type == history.Invoke && op.F == "commit-table" {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatalf("commit sent %d times", attempts)
	}
}

func TestSnapshotConcurrentCASOnCorrectFake(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	client := rest.NewClient(fake.Server.URL, "", "", 2*time.Second)
	if err := workload.SetupTable(context.Background(), client, "calve", "concurrent"); err != nil {
		t.Fatal(err)
	}
	rec := history.NewRecorder()
	rec.Record(history.Op{Type: history.Meta, F: "run", Value: snapshotRaw(SnapshotManifest{RunID: "concurrent",
		Workload: "snapshot-cas", Model: SnapshotModelVersion, Catalog: fake.Server.URL, Namespace: "calve", Table: "concurrent", Commits: 8})})
	client = client.WithRecorder(rec)
	ctx := history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "baseline"})
	meta, err := rest.ParseMetadata(client.LoadTable(ctx, "calve", "concurrent"))
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for p := 0; p < 4; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				<-start
				ctx := history.WithOperation(context.Background(), history.Op{Process: p, Phase: "workload"})
				client.CommitTable(ctx, "calve", "concurrent", testSnapshotChange(meta.TableUUID, int64(100+round*4+p), meta.CurrentSnapshotID, int64(round+1)*7))
			}(p)
		}
		close(start)
		wg.Wait()
		ctx = history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "workload"})
		meta, err = rest.ParseMetadata(client.LoadTable(ctx, "calve", "concurrent"))
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx = history.WithOperation(context.Background(), history.Op{Process: -1, Phase: "final"})
	client.LoadTable(ctx, "calve", "concurrent")
	fake.Close()
	res := checkSnapshotTrace(t, rec.Ops(), Valid)
	if res.OK != 2 || res.Failed != 6 {
		t.Fatal(res)
	}
}
