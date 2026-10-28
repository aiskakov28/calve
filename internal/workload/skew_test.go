package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
)

type skewTransport func(*http.Request) (*http.Response, error)

func (f skewTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func skewReply(status int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
}

func TestSkewRecordsConflictingPairs(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	c := rest.NewClient(fake.Server.URL, "", "", 5*time.Second)
	// Hold the first HTTP pair until both invocations have been recorded. This
	// proves overlap deterministically without assuming goroutine scheduling.
	var arrived atomic.Int32
	release := make(chan struct{})
	c.HTTP.Transport = skewTransport(func(r *http.Request) (*http.Response, error) {
		op := history.Operation(r.Context())
		if op.F == "skew-txn" && strings.HasSuffix(op.Round, "_r0") {
			if arrived.Add(1) == 2 {
				close(release)
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	rec := history.NewRecorder()
	run, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve.skew", Rounds: 3}, rec)
	fake.Close() // All assertions below use saved evidence, with no live catalog.
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Rounds) != 5 || !run.Rounds[0].Overlapped {
		t.Fatalf("missing rounds/overlap: %+v", run)
	}
	var buf bytes.Buffer
	if err := history.WriteJSONL(&buf, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	ops, err := history.ReadJSONL(&buf)
	if err != nil {
		t.Fatal(err)
	}
	assertSkewPairs(t, ops)
	baselines := map[string]rest.TableMetadata{}
	invocations := map[int]history.Op{}
	completions := map[int]history.Op{}
	verification := map[string]history.Op{}
	finals := map[string]rest.TableMetadata{}
	summaries := map[string]SkewRound{}
	counts := map[string][2]int{}
	for _, op := range ops {
		if op.Type == history.Invoke {
			invocations[op.ID] = op
			if op.F == "skew-txn" {
				pair := counts[op.Round]
				pair[op.Process]++
				counts[op.Round] = pair
			}
		}
		if op.Type == history.OK || op.Type == history.Fail || op.Type == history.Info {
			completions[op.ID] = op
		}
		if op.Type == history.OK && (op.Phase == "baseline" || op.Phase == "final" || op.Phase == "control-verify") {
			meta, err := rest.ParseMetadata(rest.Response{Status: op.Status, Body: op.Response.Body})
			if err != nil {
				t.Fatal(err)
			}
			if op.Phase == "baseline" {
				baselines[meta.TableUUID] = meta
			} else if op.Phase == "final" {
				finals[meta.TableUUID] = meta
			} else {
				verification[op.Round] = op
			}
		}
		if op.F == "skew-round-result" {
			var summary SkewRound
			if err := json.Unmarshal(op.Value, &summary); err != nil {
				t.Fatal(err)
			}
			summaries[op.Round] = summary
		}
	}
	seenTables := map[string]bool{}
	for _, round := range run.Rounds {
		if !reflect.DeepEqual(summaries[round.ID], round) || round.Unresolved || round.Error != "" {
			t.Fatalf("wrong or missing summary: %+v", round)
		}
		guards := map[string]int{}
		ok, failed := 0, 0
		for p, attempt := range round.Attempts {
			if !attempt.Invoked || counts[round.ID][p] != 1 {
				t.Fatal("missing or retried transaction")
			}
			invoke := invocations[attempt.OperationID]
			var label SkewTxn
			if err := json.Unmarshal(invoke.Value, &label); err != nil {
				t.Fatal(err)
			}
			if label.RunID != run.RunID || label.Mode != round.Mode || label.Target.Name != round.Tables[p].Name || label.Marker.Name == "" {
				t.Fatalf("lost transaction attribution: %+v", label)
			}
			var body struct {
				Changes []rest.TableChange `json:"table-changes"`
			}
			if err := json.Unmarshal(invoke.Request.Body, &body); err != nil || len(body.Changes) != 2 {
				t.Fatalf("malformed request: %s", invoke.Request.Body)
			}
			if len(body.Changes[0].Updates) != 2 || body.Changes[1].Updates == nil || len(body.Changes[1].Updates) != 0 {
				t.Fatal("requirement-only table must have literal [] updates")
			}
			if body.Changes[0].Identifier.Name != label.Target.Name || body.Changes[0].Updates[1]["schema-id"] != float64(-1) {
				t.Fatal("wrong target or current-schema update")
			}
			for _, change := range body.Changes {
				if len(change.Requirements) != 2 {
					t.Fatal("UUID and current-schema guards required on both tables")
				}
				uuid := change.Requirements[0]["uuid"].(string)
				id := int(change.Requirements[1]["current-schema-id"].(float64))
				baseline, found := baselines[uuid]
				if !found || baseline.CurrentSchemaID != id {
					t.Fatal("request guard does not match saved baseline")
				}
				if previous, found := guards[uuid]; found && previous != id {
					t.Fatal("pair used different baselines")
				}
				guards[uuid] = id
			}
			if attempt.Outcome == "ok" {
				ok++
				ownUUID := body.Changes[0].Requirements[0]["uuid"].(string)
				current, err := skewCurrentSchema(finals[ownUUID])
				if err != nil || len(current.Fields) != 2 || current.Fields[1] != label.Marker {
					t.Fatal("acknowledged update not visible in saved final schema")
				}
			} else if attempt.Outcome == "fail" && attempt.Status == 409 {
				failed++
				ownUUID := body.Changes[0].Requirements[0]["uuid"].(string)
				if finals[ownUUID].CurrentSchemaID != baselines[ownUUID].CurrentSchemaID || len(finals[ownUUID].Schemas) != 1 {
					t.Fatal("rejected transaction published a staged schema")
				}
			}
			if seenTables[label.Target.Name] {
				t.Fatal("table reused across rounds")
			}
			seenTables[label.Target.Name] = true
		}
		if ok != 1 || failed != 1 || len(guards) != 2 {
			t.Fatalf("correct serial catalog should have one winner: %+v", round)
		}
		if round.Mode != SkewConcurrent {
			first := 0
			if round.Mode == SkewBFirst {
				first = 1
			}
			verify := verification[round.ID]
			if !round.ControlEstablished || round.Overlapped || completions[round.Attempts[first].OperationID].Time >= invocations[verify.ID].Time || verify.Time >= invocations[round.Attempts[1-first].OperationID].Time {
				t.Fatal("control did not verify the first update before the stale attempt")
			}
		}
	}
}

func assertSkewPairs(t *testing.T, ops []history.Op) {
	t.Helper()
	pending := map[int]history.Op{}
	completed := map[int]bool{}
	for _, op := range ops {
		if op.Version != history.Version {
			t.Fatal("unversioned history")
		}
		if op.Type == history.Meta {
			continue
		}
		if op.Type == history.Invoke {
			if op.ID == 0 || pending[op.ID].ID != 0 || completed[op.ID] || op.Request == nil {
				t.Fatal("invalid invocation")
			}
			pending[op.ID] = op
		} else {
			invoke, found := pending[op.ID]
			if !found || op.Time < invoke.Time || op.Round != invoke.Round || op.Process != invoke.Process || op.Phase != invoke.Phase {
				t.Fatal("unpaired operation")
			}
			delete(pending, op.ID)
			completed[op.ID] = true
		}
	}
	if len(pending) != 0 {
		t.Fatal("unfinished HTTP calls")
	}
}

func TestSkewFailuresRetainEvidence(t *testing.T) {
	for _, scenario := range []string{"config", "create", "baseline", "missing-schema-id", "final", "malformed-final", "changed-uuid", "verification"} {
		t.Run(scenario, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			c.HTTP.Transport = skewTransport(func(r *http.Request) (*http.Response, error) {
				op := history.Operation(r.Context())
				if r.Method == "DELETE" {
					t.Error("workload must retain tables")
				}
				if scenario == "config" && strings.HasSuffix(r.URL.Path, "/config") || scenario == "create" && strings.HasSuffix(r.URL.Path, "/tables") || scenario == "baseline" && op.Phase == "baseline" || scenario == "final" && op.Phase == "final" || scenario == "verification" && op.Phase == "control-verify" {
					return skewReply(503, "unavailable")
				}
				if scenario == "malformed-final" && op.Phase == "final" {
					return skewReply(200, "<html>broken response</html>")
				}
				resp, err := http.DefaultTransport.RoundTrip(r)
				if err == nil && (scenario == "changed-uuid" && op.Phase == "final" || scenario == "missing-schema-id" && op.Phase == "baseline") {
					var body map[string]any
					json.NewDecoder(resp.Body).Decode(&body)
					resp.Body.Close()
					meta := body["metadata"].(map[string]any)
					if scenario == "changed-uuid" {
						meta["table-uuid"] = "replacement"
					} else {
						delete(meta, "current-schema-id")
					}
					data, _ := json.Marshal(body)
					resp.Body = io.NopCloser(bytes.NewReader(data))
				}
				return resp, err
			})
			rec := history.NewRecorder()
			run, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 1}, rec)
			if err == nil {
				t.Fatal("failed observation/setup must return an error")
			}
			assertSkewPairs(t, rec.Ops())
			if len(rec.Ops()) < 3 {
				t.Fatal("lost failed request evidence")
			}
			if scenario == "baseline" || scenario == "missing-schema-id" || scenario == "create" {
				if len(run.Rounds) != 1 || run.Rounds[0].Attempts[0].Invoked || run.Rounds[0].Attempts[1].Invoked {
					t.Fatal("transactions sent without a valid baseline")
				}
			}
			if scenario == "final" || scenario == "malformed-final" || scenario == "changed-uuid" {
				if len(run.Rounds) != 1 || !run.Rounds[0].Attempts[0].Invoked || !run.Rounds[0].Attempts[1].Invoked || run.Rounds[0].Error == "" {
					t.Fatal("lost committed pair on final-read error")
				}
				reads := 0
				for _, op := range rec.Ops() {
					if op.Phase == "final" && op.Type == history.Invoke {
						reads++
					}
				}
				if reads != 2 {
					t.Fatal("must preserve both final observations")
				}
			}
			if scenario == "verification" {
				control := run.Rounds[len(run.Rounds)-1]
				if control.ControlEstablished || control.Attempts[1].Invoked {
					t.Fatal("stale control proceeded after failed verification")
				}
			}
		})
	}
}

func TestSkewUnknownAndUnestablishedControls(t *testing.T) {
	for _, outcome := range []string{"unknown-applied", "unknown-unapplied", "acknowledged-unapplied", "unsupported"} {
		t.Run(outcome, func(t *testing.T) {
			fake := fakecatalog.New(fakecatalog.Bugs{})
			defer fake.Close()
			c := rest.NewClient(fake.Server.URL, "", "", time.Second)
			c.HTTP.Transport = skewTransport(func(r *http.Request) (*http.Response, error) {
				op := history.Operation(r.Context())
				var label SkewTxn
				json.Unmarshal(op.Value, &label)
				if op.F == "skew-txn" && label.Mode == SkewAFirst {
					if outcome == "unknown-applied" {
						resp, err := http.DefaultTransport.RoundTrip(r)
						if err != nil {
							return nil, err
						}
						resp.Body.Close()
					}
					switch outcome {
					case "unknown-applied", "unknown-unapplied":
						return skewReply(503, "unknown commit state")
					case "acknowledged-unapplied":
						return skewReply(204, "")
					case "unsupported":
						return skewReply(404, "not supported")
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})
			rec := history.NewRecorder()
			run, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 1}, rec)
			if err != nil {
				t.Fatal(err)
			}
			control := run.Rounds[1]
			if control.ControlEstablished || control.Attempts[1].Invoked || control.SkippedReason == "" {
				t.Fatalf("unestablished control was treated as ready: %+v", control)
			}
			wantUnknown := strings.HasPrefix(outcome, "unknown")
			if control.Unresolved != wantUnknown {
				t.Fatalf("unknown outcome changed by a final read: %+v", control)
			}
			assertSkewPairs(t, rec.Ops())
		})
	}
}

func TestSkewCancellation(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	c := rest.NewClient(fake.Server.URL, "", "", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.HTTP.Transport = skewTransport(func(r *http.Request) (*http.Response, error) {
		if history.Operation(r.Context()).F == "skew-txn" {
			cancel()
			return nil, context.Canceled
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	rec := history.NewRecorder()
	run, err := RunSkew(ctx, c, SkewConfig{Namespace: "calve", Rounds: 3}, rec)
	if err == nil || len(run.Rounds) != 1 || !run.Rounds[0].Unresolved {
		t.Fatalf("canceled run lost uncertainty: %+v, %v", run, err)
	}
	for _, attempt := range run.Rounds[0].Attempts {
		if !attempt.Invoked || attempt.Outcome != "info" {
			t.Fatal("canceled attempt must remain unknown")
		}
	}
	assertSkewPairs(t, rec.Ops())
}

func TestSkewChangesPreserveFieldMetadata(t *testing.T) {
	// Copy the existing field exactly, including attributes outside rest.Field.
	raw := json.RawMessage(`{"schema-id":7,"type":"struct","identifier-field-ids":[1],"fields":[{"id":1,"name":"id","required":true,"type":"long","doc":"keep me","initial-default":42}]}`)
	base := [2]rest.TableMetadata{{TableUUID: "a", CurrentSchemaID: 7, LastColumnID: 1, Schemas: []json.RawMessage{raw}}, {TableUUID: "b", CurrentSchemaID: 7}}
	tables := [2]rest.Identifier{{Namespace: []string{"test"}, Name: "a"}, {Namespace: []string{"test"}, Name: "b"}}
	marker := rest.Field{ID: 2, Name: "unique_marker", Type: "long"}
	changes := skewChanges(tables, base, 0, marker)
	data, err := json.Marshal(changes[0].Updates[0]["schema"])
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		ID          *int             `json:"schema-id"`
		Identifiers []int            `json:"identifier-field-ids"`
		Fields      []map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.ID != nil || len(schema.Fields) != 2 || schema.Fields[0]["doc"] != "keep me" || schema.Fields[0]["initial-default"] != float64(42) || !reflect.DeepEqual(schema.Identifiers, []int{1}) {
		t.Fatalf("schema update discarded existing metadata: %s", data)
	}
}

func TestSkewConfigAndUniqueRuns(t *testing.T) {
	fake := fakecatalog.New(fakecatalog.Bugs{})
	defer fake.Close()
	c := rest.NewClient(fake.Server.URL, "", "", time.Second)
	for _, cfg := range []SkewConfig{{Namespace: "calve"}, {Namespace: "calve", Rounds: -1}, {Rounds: 1}, {Namespace: "a..b", Rounds: 1}} {
		rec := history.NewRecorder()
		if _, err := RunSkew(context.Background(), c, cfg, rec); err == nil || len(rec.Ops()) != 0 {
			t.Fatal("invalid config sent requests")
		}
	}
	if _, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 1}, nil); err == nil {
		t.Fatal("nil recorder accepted")
	}
	a, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 1}, history.NewRecorder())
	if err != nil {
		t.Fatal(err)
	}
	b, err := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 1}, history.NewRecorder())
	if err != nil || a.RunID == b.RunID || a.Rounds[0].Tables[0].Name == b.Rounds[0].Tables[0].Name {
		t.Fatal("run identifiers/table names were reused")
	}
}

// Opt-in real-catalog exercise until issue #10 adds calve skew. It captures
// observations, not a serializability verdict; both acknowledgments may occur.
func TestSkewLiveCatalog(t *testing.T) {
	endpoint := os.Getenv("CALVE_SKEW_CATALOG")
	if endpoint == "" {
		t.Skip("set CALVE_SKEW_CATALOG to exercise a running catalog")
	}
	c := rest.NewClient(endpoint, os.Getenv("CALVE_SKEW_WAREHOUSE"), os.Getenv("CALVE_TOKEN"), 10*time.Second)
	rec := history.NewRecorder()
	run, runErr := RunSkew(context.Background(), c, SkewConfig{Namespace: "calve", Rounds: 10}, rec)
	path := os.Getenv("CALVE_SKEW_HISTORY")
	if path == "" {
		path = filepath.Join(t.TempDir(), "skew.jsonl")
	}
	if err := history.SaveFile(path, rec.Ops()); err != nil {
		t.Errorf("save history: %v", err)
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, round := range run.Rounds {
		t.Logf("%s %s: %s/%d, %s/%d; overlap=%v control-established=%v unresolved=%v", round.ID, round.Mode, round.Attempts[0].Outcome, round.Attempts[0].Status, round.Attempts[1].Outcome, round.Attempts[1].Status, round.Overlapped, round.ControlEstablished, round.Unresolved)
	}
	t.Logf("history saved to %s", path)
}
