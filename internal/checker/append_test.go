package checker

import (
	"encoding/json"
	"testing"

	"github.com/aiskakov28/calve/internal/history"
)

func appendOps(t *testing.T, pairs ...any) []history.Op {
	t.Helper()
	var ops []history.Op
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i].(string)
		typ := pairs[i+1].(history.Type)
		v, _ := json.Marshal(AppendValue{Key: key})
		ops = append(ops,
			history.Op{Index: len(ops), F: "append", Type: history.Invoke, Value: v},
			history.Op{Index: len(ops) + 1, F: "append", Type: typ, Value: v},
		)
	}
	return ops
}

func kinds(r AppendResult) map[string]int {
	out := map[string]int{}
	for _, a := range r.Anomalies {
		out[a.Kind]++
	}
	return out
}

func TestCheckAppendValid(t *testing.T) {
	ops := appendOps(t, "a", history.OK, "b", history.Fail, "c", history.Info)
	// c is unknown, so it may or may not be present.
	for _, final := range [][]string{{"a"}, {"a", "c"}} {
		r, err := CheckAppend(ops, final)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Valid {
			t.Fatalf("final %v: want valid, got %+v", final, r.Anomalies)
		}
	}
}

func TestCheckAppendAnomalies(t *testing.T) {
	ops := appendOps(t, "a", history.OK, "b", history.Fail, "c", history.OK)
	r, err := CheckAppend(ops, []string{"b", "c", "c", "zzz"})
	if err != nil {
		t.Fatal(err)
	}
	got := kinds(r)
	want := map[string]int{"lost": 1, "phantom-failed": 1, "duplicate": 1, "unexpected": 1}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("anomaly %q: want %d, got %d (all: %+v)", k, n, got[k], r.Anomalies)
		}
	}
	if r.Valid {
		t.Error("want invalid")
	}
}

func TestCheckAppendInvokeWithoutCompletionIsUnknown(t *testing.T) {
	v, _ := json.Marshal(AppendValue{Key: "x"})
	ops := []history.Op{{F: "append", Type: history.Invoke, Value: v}}
	for _, final := range [][]string{{}, {"x"}} {
		r, err := CheckAppend(ops, final)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Valid || r.Unknown != 1 {
			t.Fatalf("final %v: want valid with 1 unknown, got %+v", final, r)
		}
	}
}
