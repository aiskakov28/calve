package checker

import (
	"fmt"
	"testing"
	"time"

	"github.com/aiskakov28/calve/internal/history"
)

func TestSnapshotReviewSequenceGaps(t *testing.T) {
	s := newSnapshotTrace(2)
	first := testSnapshot(11, nil, 7)
	one := testSnapshotMetadata(snapshotPtr(11), first)
	s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 7))
	s.reply(2, history.OK, 200, one)
	s.call(3, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), 13))
	two := testSnapshotMetadata(snapshotPtr(12), first, testSnapshot(12, snapshotPtr(11), 13))
	s.reply(3, history.OK, 200, two)
	s.final(4, two)
	checkSnapshotTrace(t, s.ops, Valid)
	for _, sequence := range []int64{6, 7} {
		t.Run(fmt.Sprintf("non-increasing=%d", sequence), func(t *testing.T) {
			s := newSnapshotTrace(1)
			s.ops[2].Response.Body = snapshotRaw(one)
			s.call(2, "commit-table", "workload", testSnapshotChange("u", 12, snapshotPtr(11), sequence))
			result := testSnapshotMetadata(snapshotPtr(12), first, testSnapshot(12, snapshotPtr(11), sequence))
			s.reply(2, history.OK, 200, result)
			s.final(3, result)
			checkSnapshotTrace(t, s.ops, Invalid)
		})
	}
}

func TestSnapshotReviewUnmodeledCredentialsCanBeRedacted(t *testing.T) {
	s := newSnapshotTrace(1)
	s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
	one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
	s.reply(2, history.OK, 200, one)
	s.final(3, one)
	for _, i := range []int{2, 4, 6} {
		var body map[string]any
		if err := snapshotJSON(s.ops[i].Response.Body, &body); err != nil {
			t.Fatal(err)
		}
		body["config"] = map[string]string{"s3.secret-access-key": "[REDACTED]"}
		body["metadata"].(map[string]any)["properties"] = map[string]string{"application.token": "[REDACTED]"}
		s.ops[i].Response.Body = snapshotRaw(body)
		s.ops[i].Response.Redacted = true
	}
	checkSnapshotTrace(t, s.ops, Valid)
}

func TestSnapshotReviewModeledRedactionRemainsUnusable(t *testing.T) {
	for _, field := range []string{"uuid", "manifest", "summary"} {
		t.Run(field, func(t *testing.T) {
			s := newSnapshotTrace(1)
			s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
			record := testSnapshot(11, nil, 1)
			one := testSnapshotMetadata(snapshotPtr(11), record)
			switch field {
			case "uuid":
				one["metadata"].(map[string]any)["table-uuid"] = "[REDACTED]"
			case "manifest":
				record["manifest-list"] = "[REDACTED]"
			case "summary":
				record["summary"] = map[string]string{"operation": "append", "custom": "[REDACTED]"}
			}
			s.reply(2, history.OK, 200, one)
			s.final(3, one)
			if _, err := CheckSnapshotHistory(s.ops, time.Second); err == nil {
				t.Fatal("accepted redacted modeled evidence")
			}
		})
	}
}

func TestSnapshotReviewReadFailureCanRecover(t *testing.T) {
	for _, outcome := range []history.Type{history.OK, history.Info} {
		t.Run(string(outcome), func(t *testing.T) {
			s := newSnapshotTrace(1)
			s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
			one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
			if outcome == history.OK {
				s.reply(2, outcome, 200, one)
			} else {
				s.reply(2, outcome, 503, nil)
			}
			s.call(3, "load-table", "final", nil)
			s.reply(3, history.Info, 503, nil)
			checkSnapshotTrace(t, s.ops, Inconclusive)
			s.call(4, "load-table", "recovery", nil)
			s.reply(4, history.OK, 200, one)
			checkSnapshotTrace(t, s.ops, Valid)
		})
	}
}

func TestSnapshotReviewIdentityBoundary(t *testing.T) {
	for _, earlierBug := range []bool{false, true} {
		for _, changeDuringCommit := range []bool{false, true} {
			t.Run(fmt.Sprintf("earlierBug=%v/commit=%v", earlierBug, changeDuringCommit), func(t *testing.T) {
				s := newSnapshotTrace(2)
				s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
				one := testSnapshotMetadata(snapshotPtr(11), testSnapshot(11, nil, 1))
				if earlierBug {
					one = testSnapshotMetadata(nil)
				}
				s.reply(2, history.OK, 200, one)
				if !changeDuringCommit {
					s.call(3, "load-table", "workload", nil)
					fresh := testSnapshotMetadata(nil)
					fresh["metadata"].(map[string]any)["table-uuid"] = "recreated"
					s.reply(3, history.OK, 200, fresh)
				}
				s.call(4, "commit-table", "workload", testSnapshotChange("recreated", 12, nil, 1))
				fresh := testSnapshotMetadata(snapshotPtr(12), testSnapshot(12, nil, 1))
				fresh["metadata"].(map[string]any)["table-uuid"] = "recreated"
				s.reply(4, history.OK, 200, fresh)
				s.final(5, fresh)
				want := Inconclusive
				if earlierBug {
					want = Invalid
				}
				res := checkSnapshotTrace(t, s.ops, want)
				if res.Verdict == Invalid {
					for _, id := range res.OperationIDs {
						if id >= 3 {
							t.Fatalf("identity-mismatched suffix cited as finding evidence: %+v", res)
						}
					}
				}
			})
		}
	}
}

func TestSnapshotReviewIdentityBoundaryLeavesCrossingCommitPending(t *testing.T) {
	s := newSnapshotTrace(1)
	s.call(2, "commit-table", "workload", testSnapshotChange("u", 11, nil, 1))
	s.call(3, "load-table", "workload", nil)
	// This response cannot be used to prove a violation in the old incarnation
	// once its operation overlaps the identity boundary.
	s.reply(2, history.OK, 200, testSnapshotMetadata(nil))
	fresh := testSnapshotMetadata(nil)
	fresh["metadata"].(map[string]any)["table-uuid"] = "recreated"
	// A new incarnation can even use another metadata format.
	fresh["metadata"].(map[string]any)["format-version"] = 3
	s.reply(3, history.OK, 200, fresh)
	s.final(4, fresh)
	checkSnapshotTrace(t, s.ops, Inconclusive)
}
