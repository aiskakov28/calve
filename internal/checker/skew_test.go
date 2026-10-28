package checker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/checker"
	"github.com/aiskakov28/calve/internal/fakecatalog"
	"github.com/aiskakov28/calve/internal/history"
	"github.com/aiskakov28/calve/internal/rest"
	"github.com/aiskakov28/calve/internal/workload"
)

type skewTransport func(*http.Request) (*http.Response, error)

func (f skewTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func skewHistory(t *testing.T, bugs fakecatalog.Bugs, transport http.RoundTripper) ([]history.Op, workload.SkewRun, error) {
	t.Helper()
	fake := fakecatalog.New(bugs)
	defer fake.Close()
	if bugs.WriteSkew {
		var validated atomic.Int32
		release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		fake.SetAfterTransactionValidation(func(ctx context.Context) {
			n := validated.Add(1)
			if n <= 4 { // Two concurrent rounds; the later controls must not wait.
				gate := release[(n-1)/2]
				if n%2 == 0 {
					close(gate)
				}
				select {
				case <-gate:
				case <-ctx.Done():
				}
			}
		})
	}
	c := rest.NewClient(fake.Server.URL, "", "", 3*time.Second)
	if transport != nil {
		c.HTTP.Transport = transport
	}
	rec := history.NewRecorder()
	run, err := workload.RunSkew(context.Background(), c, workload.SkewConfig{Namespace: "calve.skew", Rounds: 2}, rec)
	return copySkewHistory(t, rec.Ops()), run, err
}

func copySkewHistory(t *testing.T, ops []history.Op) []history.Op {
	t.Helper()
	var buf bytes.Buffer
	if err := history.WriteJSONL(&buf, ops); err != nil {
		t.Fatal(err)
	}
	copy, err := history.ReadJSONL(&buf)
	if err != nil {
		t.Fatal(err)
	}
	return copy
}

func checkSkew(t *testing.T, ops []history.Op) checker.SkewResult {
	t.Helper()
	result, err := checker.CheckSkew(ops)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCheckSkewCorrectAndPlantedCatalogs(t *testing.T) {
	cases := []struct {
		name    string
		bugs    fakecatalog.Bugs
		verdict checker.SkewVerdict
		kinds   map[string]int
	}{
		{"correct", fakecatalog.Bugs{}, checker.SkewValid, nil},
		{"write-skew-race", fakecatalog.Bugs{WriteSkew: true}, checker.SkewInvalid, map[string]int{"write-skew": 2}},
		{"ignored-read-guards", fakecatalog.Bugs{IgnoreReadOnlyRequirements: true}, checker.SkewInvalid, map[string]int{"write-skew": 2, "stale-requirement": 2}},
		{"lost-acknowledgments", fakecatalog.Bugs{LoseTransactions: true}, checker.SkewInvalid, map[string]int{"write-skew": 2, "lost": 6}},
		{"rejected-effects", fakecatalog.Bugs{RejectAppliedTransactions: true}, checker.SkewInvalid, map[string]int{"phantom-failed": 4}},
		{"unknown-applied", fakecatalog.Bugs{UnknownAppliedTransactions: true}, checker.SkewInconclusive, nil},
		{"unknown-unapplied", fakecatalog.Bugs{UnknownUnappliedTransactions: true}, checker.SkewInconclusive, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops, _, err := skewHistory(t, tc.bugs, nil)
			if err != nil {
				t.Fatal(err)
			}
			// The fake has stopped; all checking is from the serialized history.
			res := checkSkew(t, ops)
			if res.Verdict != tc.verdict || res.Valid != (tc.verdict == checker.SkewValid) {
				t.Fatalf("wrong verdict: %+v", res)
			}
			got := map[string]int{}
			for _, anomaly := range res.Anomalies {
				got[anomaly.Kind]++
				wantBasis := "catalog-contract"
				if anomaly.Kind == "write-skew" {
					wantBasis = "serializable-model"
				}
				if anomaly.Basis != wantBasis {
					t.Fatal("finding claims the wrong guarantee")
				}
			}
			if len(got) != len(tc.kinds) {
				t.Fatalf("wrong findings: %+v", res.Anomalies)
			}
			for kind, count := range tc.kinds {
				if got[kind] != count {
					t.Fatalf("want %d %s findings, got %+v", count, kind, got)
				}
			}
			if tc.name == "correct" && (res.OK != 4 || res.Failed != 4 || res.Unknown != 0 || res.InconclusiveRounds != 0) {
				t.Fatalf("wrong counts: %+v", res)
			}
			if tc.name == "write-skew-race" && (!res.Rounds[0].Overlapped || !res.Rounds[1].Overlapped || res.Rounds[2].Verdict != checker.SkewValid || res.Rounds[3].Verdict != checker.SkewValid) {
				t.Fatal("planted race must overlap without breaking sequential controls")
			}
			if tc.name == "lost-acknowledgments" && (res.OK != 6 || res.Rounds[0].Transactions[0].CommitState != "committed") {
				t.Fatal("missing effects erased acknowledgments")
			}
			if tc.name == "unknown-applied" && (res.ResolvedCommitted != 4 || res.Unknown != 4 || res.Rounds[0].Verdict != checker.SkewValid || res.Rounds[0].Transactions[0].Outcome != history.Info && res.Rounds[0].Transactions[1].Outcome != history.Info) {
				t.Fatalf("visible complete update did not resolve uncertainty separately: %+v", res)
			}
			if tc.name == "unknown-unapplied" && (res.ResolvedCommitted != 0 || res.Unknown != 6 || res.InconclusiveRounds != 4) {
				t.Fatalf("absence resolved an unknown without a recovery boundary: %+v", res)
			}
		})
	}
}

func setSkewUnknown(op *history.Op) {
	op.Type = history.Info
	op.Status = 503
	op.Response.Status = 503
	op.Error = "commit state unknown"
}

func TestCheckSkewUnknownPairs(t *testing.T) {
	correct, run, err := skewHistory(t, fakecatalog.Bugs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raced, raceRun, err := skewHistory(t, fakecatalog.Bugs{WriteSkew: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{false, true} {
		ops := copySkewHistory(t, correct)
		filtered := ops[:0]
		for _, op := range ops {
			if op.F == "skew-txn" && op.Round == run.Rounds[0].ID && op.Type == history.Fail {
				if missing {
					continue
				}
				setSkewUnknown(&op)
			}
			filtered = append(filtered, op)
		}
		res := checkSkew(t, filtered)
		if res.Verdict != checker.SkewInconclusive || len(res.Anomalies) != 0 || res.Unknown != 1 || res.ResolvedCommitted != 0 {
			t.Fatalf("unknown rejection became a definite result: %+v", res)
		}
	}
	for _, count := range []int{1, 2} {
		ops := copySkewHistory(t, raced)
		changed := 0
		for i := range ops {
			op := &ops[i]
			if changed < count && op.F == "skew-txn" && op.Round == raceRun.Rounds[0].ID && op.Type == history.OK {
				setSkewUnknown(op)
				changed++
			}
		}
		res := checkSkew(t, ops)
		if res.Rounds[0].Verdict != checker.SkewInvalid || res.ResolvedCommitted != count || len(res.Rounds[0].Anomalies) != 1 || res.Rounds[0].Anomalies[0].Kind != "write-skew" {
			t.Fatalf("complete visible updates failed to prove the pair: %+v", res)
		}
	}
}

func TestCheckSkewPositiveResolutionSurvivesFailedLaterRead(t *testing.T) {
	ops, run, err := skewHistory(t, fakecatalog.Bugs{WriteSkew: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var invoke, done history.Op
	maxID := 0
	for i := range ops {
		op := &ops[i]
		if op.ID > maxID {
			maxID = op.ID
		}
		if op.Round == run.Rounds[0].ID && op.F == "skew-txn" && op.Type == history.OK {
			setSkewUnknown(op)
		}
		if op.Round == run.Rounds[0].ID && op.Phase == "final" && op.F == "load-table" {
			if op.Type == history.Invoke {
				invoke = *op
			} else {
				done = *op
			}
		}
	}
	for _, op := range []history.Op{invoke, done} {
		op.ID, op.Phase = maxID+1, "recovery"
		op.Index, op.Time = ops[len(ops)-1].Index+1, ops[len(ops)-1].Time+1
		if op.Type == history.OK {
			response := *op.Response
			op.Response = &response
			op.Type, op.Status, op.Response.Status = history.Info, 503, 503
			op.Response.Body = []byte("unavailable")
		}
		ops = append(ops, op)
	}
	res := checkSkew(t, copySkewHistory(t, ops))
	if res.Verdict != checker.SkewInvalid || res.ResolvedCommitted != 2 || len(res.Anomalies) != 2 {
		t.Fatalf("failed later read erased prior commitment/violation evidence: %+v", res)
	}
	for _, txn := range res.Rounds[0].Transactions {
		if txn.Outcome != history.Info || txn.CommitState != "committed" || len(txn.EvidenceIDs) != 1 {
			t.Fatalf("raw outcome or supporting evidence changed: %+v", txn)
		}
	}
}

func TestUnknownResolutionSkewEffectDisappears(t *testing.T) {
	original, run, err := skewHistory(t, fakecatalog.Bugs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, missingCompletion := range []bool{false, true} {
		for _, uncertainPeer := range []bool{false, true} {
			t.Run(fmt.Sprintf("missing-completion=%v/uncertain-peer=%v", missingCompletion, uncertainPeer), func(t *testing.T) {
				ops := copySkewHistory(t, original)
				winner := -1
				maxID := 0
				var filtered []history.Op
				for i := range ops {
					op := &ops[i]
					if op.ID > maxID {
						maxID = op.ID
					}
					if op.Round == run.Rounds[0].ID && op.F == "skew-txn" {
						if op.Type == history.OK {
							winner = op.Process
							if missingCompletion {
								continue
							}
							setSkewUnknown(op)
						} else if op.Type == history.Fail && uncertainPeer {
							setSkewUnknown(op)
						}
					}
					filtered = append(filtered, *op)
				}
				if winner < 0 {
					t.Fatal("missing acknowledged winner in the correct history")
				}
				var baselineInvoke, baselineDone history.Op
				for _, op := range filtered {
					if op.Round == run.Rounds[0].ID && op.Phase == "baseline" && op.Type == history.Invoke &&
						strings.HasSuffix(op.Request.URL, "/"+run.Rounds[0].Tables[winner].Name) {
						baselineInvoke = op
					}
					if op.ID == baselineInvoke.ID && op.Type == history.OK {
						baselineDone = op
					}
				}
				if baselineDone.Response == nil {
					t.Fatal("missing baseline response")
				}
				// The complete update is visible in the existing final read.
				// A later successful read returns the old schema on the same UUID.
				for _, op := range []history.Op{baselineInvoke, baselineDone} {
					op.ID, op.Phase = maxID+1, "recovery"
					last := filtered[len(filtered)-1]
					op.Index, op.Time = last.Index+1, last.Time+1
					filtered = append(filtered, op)
				}
				res := checkSkew(t, copySkewHistory(t, filtered))
				round := res.Rounds[0]
				txn := round.Transactions[winner]
				if txn.Outcome != history.Info || txn.CommitState != "committed" || len(txn.EvidenceIDs) != 1 || res.ResolvedCommitted != 1 {
					t.Fatalf("disappearance erased the source outcome or positive proof: %+v", res)
				}
				if uncertainPeer {
					if res.Verdict != checker.SkewInconclusive || len(res.Anomalies) != 0 {
						t.Fatalf("unsettled reads became a definite loss/pass: %+v", res)
					}
				} else if res.Verdict != checker.SkewInvalid || len(round.Anomalies) != 1 ||
					round.Anomalies[0].Kind != "lost" || round.Anomalies[0].Basis != "catalog-contract" {
					t.Fatalf("resolved commitment disappeared without a finding: %+v", res)
				}
			})
		}
	}
}

func TestCheckSkewUnknownNeedsCompleteCurrentUpdate(t *testing.T) {
	for _, mode := range []string{"historical-only", "partial-current"} {
		t.Run(mode, func(t *testing.T) {
			ops, run, err := skewHistory(t, fakecatalog.Bugs{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			loser := -1
			var request history.Op
			for i := range ops {
				op := &ops[i]
				if op.F == "skew-txn" && op.Round == run.Rounds[0].ID && op.Type == history.Fail {
					loser = op.Process
					setSkewUnknown(op)
				}
			}
			for _, op := range ops {
				if op.Round == run.Rounds[0].ID && op.F == "skew-txn" && op.Process == loser && op.Type == history.Invoke {
					request = op
				}
			}
			var body struct {
				Changes []rest.TableChange `json:"table-changes"`
			}
			json.Unmarshal(request.Request.Body, &body)
			added := body.Changes[0].Updates[0]["schema"].(map[string]any)
			for i := range ops {
				op := &ops[i]
				if op.Round != run.Rounds[0].ID || op.Phase != "final" || op.Type != history.OK {
					continue
				}
				var response map[string]any
				json.Unmarshal(op.Response.Body, &response)
				meta := response["metadata"].(map[string]any)
				if meta["table-uuid"] != body.Changes[0].Requirements[0]["uuid"] {
					continue
				}
				if mode == "partial-current" {
					fields := added["fields"].([]any)
					fields[1].(map[string]any)["type"] = "string"
					meta["current-schema-id"] = 1
				}
				meta["schemas"] = append(meta["schemas"].([]any), added)
				meta["last-column-id"] = 2
				op.Response.Body, _ = json.Marshal(response)
			}
			res := checkSkew(t, ops)
			if res.Rounds[0].Verdict != checker.SkewInconclusive || res.ResolvedCommitted != 0 || len(res.Anomalies) != 0 {
				t.Fatalf("partial/historical effect was treated as complete commitment: %+v", res)
			}
		})
	}
}

func TestCheckSkewUnavailableObservations(t *testing.T) {
	for _, mode := range []string{"missing-final", "bad-final", "changed-uuid", "malformed-fields", "redacted-metadata", "config", "baseline"} {
		t.Run(mode, func(t *testing.T) {
			var transport http.RoundTripper
			if mode == "config" || mode == "baseline" {
				transport = skewTransport(func(r *http.Request) (*http.Response, error) {
					if mode == "config" && strings.HasSuffix(r.URL.Path, "/config") || mode == "baseline" && history.Operation(r.Context()).Phase == "baseline" {
						return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
					}
					return http.DefaultTransport.RoundTrip(r)
				})
			}
			ops, _, runErr := skewHistory(t, fakecatalog.Bugs{}, transport)
			if transport != nil && runErr == nil {
				t.Fatal("injected failure did not stop workload")
			}
			filtered := ops[:0]
			for _, op := range ops {
				if mode == "missing-final" && op.Phase == "final" {
					continue
				}
				if op.Phase == "final" && op.Type == history.OK {
					if mode == "bad-final" {
						op.Response.Body = []byte("<html>broken</html>")
					}
					if mode == "changed-uuid" {
						var body map[string]any
						json.Unmarshal(op.Response.Body, &body)
						body["metadata"].(map[string]any)["table-uuid"] = "replacement"
						op.Response.Body, _ = json.Marshal(body)
					}
					if mode == "malformed-fields" || mode == "redacted-metadata" {
						var body map[string]any
						json.Unmarshal(op.Response.Body, &body)
						meta := body["metadata"].(map[string]any)
						if mode == "redacted-metadata" {
							meta["table-uuid"] = "[REDACTED]"
						} else {
							for _, schema := range meta["schemas"].([]any) {
								for _, field := range schema.(map[string]any)["fields"].([]any) {
									delete(field.(map[string]any), "required")
								}
							}
						}
						op.Response.Body, _ = json.Marshal(body)
					}
				}
				filtered = append(filtered, op)
			}
			res := checkSkew(t, filtered)
			if res.Valid || res.Verdict != checker.SkewInconclusive || len(res.Anomalies) != 0 {
				t.Fatalf("unavailable observations manufactured a verdict: %+v", res)
			}
		})
	}
}

func TestCheckSkewRejectsMalformedHistories(t *testing.T) {
	original, _, err := skewHistory(t, fakecatalog.Bugs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"orphan", "duplicate", "labels", "different-baseline", "writes-other-table", "version", "time", "missing-verification", "wrong-endpoint", "different-catalog", "different-prefix"} {
		t.Run(mode, func(t *testing.T) {
			ops := copySkewHistory(t, original)
			if mode == "version" {
				ops[0].Version = 0
			}
			if mode == "time" {
				ops[len(ops)-1].Time = -1
			}
			filtered := ops[:0]
			changed := false
			for _, op := range ops {
				if mode == "missing-verification" && op.Phase == "control-verify" {
					continue
				}
				if !changed && op.F == "skew-txn" {
					switch mode {
					case "orphan":
						if op.Type == history.Invoke {
							changed = true
							continue
						}
					case "duplicate":
						if op.Type != history.Invoke {
							changed = true
							filtered = append(filtered, op)
						}
					case "labels":
						if op.Type != history.Invoke {
							changed = true
							op.Process = 7
						}
					case "wrong-endpoint":
						if op.Type == history.Invoke {
							changed = true
							op.Request.URL += "/different"
						}
					case "different-catalog":
						if op.Type == history.Invoke {
							changed = true
							op.Request.URL = "http://other-catalog/v1/transactions/commit"
						}
					case "different-prefix":
						if op.Type == history.Invoke {
							changed = true
							op.Request.URL = strings.Replace(op.Request.URL, "/v1/", "/v1/other/", 1)
						}
					case "different-baseline", "writes-other-table":
						if op.Type == history.Invoke {
							var body struct {
								Changes []rest.TableChange `json:"table-changes"`
							}
							json.Unmarshal(op.Request.Body, &body)
							if mode == "different-baseline" {
								body.Changes[1].Requirements[1]["current-schema-id"] = 99
							} else {
								body.Changes[1].Updates = body.Changes[0].Updates
							}
							op.Request.Body, _ = json.Marshal(body)
							changed = true
						}
					}
				}
				filtered = append(filtered, op)
			}
			if _, err := checker.CheckSkew(filtered); err == nil {
				t.Fatal("malformed evidence accepted")
			}
		})
	}
}

func TestCheckSkewDoesNotTrustExecutionSummaries(t *testing.T) {
	ops, _, err := skewHistory(t, fakecatalog.Bugs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := checkSkew(t, ops)
	for i := range ops {
		if ops[i].F == "skew-round-result" {
			ops[i].Value = json.RawMessage(`{"attempts":[{"outcome":"ok"},{"outcome":"ok"}],"unresolved":true,"control_established":false}`)
		}
	}
	if got := checkSkew(t, ops); !reflect.DeepEqual(got, want) {
		t.Fatal("execution summary overrode the actual evidence")
	}
}

func TestCheckSkewSavedHistory(t *testing.T) {
	path := os.Getenv("CALVE_SKEW_RECHECK")
	if path == "" {
		t.Skip("set CALVE_SKEW_RECHECK to replay a saved artifact")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ops, err := history.ReadJSONL(f)
	if err != nil {
		t.Fatal(err)
	}
	res := checkSkew(t, ops)
	if expected := os.Getenv("CALVE_SKEW_EXPECT"); expected != "" && string(res.Verdict) != expected {
		t.Fatalf("want %s, got %+v", expected, res)
	}
	kinds := map[string]int{}
	for _, anomaly := range res.Anomalies {
		kinds[anomaly.Kind]++
	}
	t.Logf("%s: %s; %d rounds, %d ok, %d rejected, %d unknown, %d inconclusive rounds; findings %v", path, res.Verdict, len(res.Rounds), res.OK, res.Failed, res.Unknown, res.InconclusiveRounds, kinds)
}

func TestCheckSkewMissingReadsDoNotEraseAcknowledgedConflict(t *testing.T) {
	ops, _, err := skewHistory(t, fakecatalog.Bugs{WriteSkew: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	filtered := ops[:0]
	for _, op := range ops {
		if op.Phase != "final" {
			filtered = append(filtered, op)
		}
	}
	res := checkSkew(t, filtered)
	if res.Verdict != checker.SkewInvalid || len(res.Anomalies) != 2 || res.InconclusiveRounds != 4 {
		t.Fatalf("missing state erased a definite acknowledged conflict: %+v", res)
	}
}

func TestCheckSkewCredentialHeaderRedactionIsHarmless(t *testing.T) {
	ops, _, err := skewHistory(t, fakecatalog.Bugs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ops {
		if ops[i].Request != nil {
			ops[i].Request.Redacted = true
			ops[i].Request.Headers.Set("Authorization", "[REDACTED]")
		}
		if ops[i].Response != nil {
			ops[i].Response.Redacted = true
			ops[i].Response.Headers.Set("Set-Cookie", "[REDACTED]")
		}
	}
	if res := checkSkew(t, ops); !res.Valid {
		t.Fatalf("header-only redaction invalidated usable evidence: %+v", res)
	}
}

func TestCheckSkewRejectedRoundsAreVisible(t *testing.T) {
	for _, status := range []int{409, 400} {
		transport := skewTransport(func(r *http.Request) (*http.Response, error) {
			op := history.Operation(r.Context())
			var label workload.SkewTxn
			json.Unmarshal(op.Value, &label)
			if op.F == "skew-txn" && label.Mode == workload.SkewConcurrent {
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("rejected"))}, nil
			}
			return http.DefaultTransport.RoundTrip(r)
		})
		ops, _, err := skewHistory(t, fakecatalog.Bugs{}, transport)
		if err != nil {
			t.Fatal(err)
		}
		res := checkSkew(t, ops)
		if res.RejectedRounds != 2 || len(res.Anomalies) != 0 {
			t.Fatalf("no-progress rounds were hidden: %+v", res)
		}
		if status == 409 && !res.Valid {
			t.Fatalf("safety requires at most one winner, not progress: %+v", res)
		}
		if status == 400 && res.Verdict != checker.SkewInconclusive {
			t.Fatalf("invalid/unsupported requests counted as a successful isolation test: %+v", res)
		}
	}
}
