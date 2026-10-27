package history

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
)

func TestRecorderConcurrentAndRoundTrip(t *testing.T) {
	r := NewRecorder()
	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				r.Record(Op{Process: p, Type: Invoke, F: "append", Value: json.RawMessage(`{"key":"k"}`)})
			}
		}(p)
	}
	wg.Wait()

	ops := r.Ops()
	if len(ops) != 400 {
		t.Fatalf("want 400 ops, got %d", len(ops))
	}
	for i, op := range ops {
		if op.Index != i {
			t.Fatalf("op %d has index %d", i, op.Index)
		}
	}

	var buf bytes.Buffer
	if err := WriteJSONL(&buf, ops); err != nil {
		t.Fatal(err)
	}
	back, err := ReadJSONL(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(ops) || back[399].Index != 399 {
		t.Fatalf("round trip lost records: %d", len(back))
	}
}
