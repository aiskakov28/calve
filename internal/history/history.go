// Package history records every operation a Calve run makes against a
// catalog, in the invoke/complete style Jepsen uses.
//
// Each operation produces two records: an "invoke" when the client sends the
// request, and a completion ("ok", "fail" or "info") when it gets an answer.
// "info" means the outcome is unknown: a timeout or a 5xx where the commit
// may or may not have happened. Checkers must treat "info" as "maybe".
package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Type is the kind of record.
type Type string

const (
	Invoke Type = "invoke"
	OK     Type = "ok"
	Fail   Type = "fail"
	// Info means the outcome is unknown (timeout, connection reset, 5xx).
	Info Type = "info"
)

// Op is one record in a history.
type Op struct {
	// Index is the record's position in the history, assigned by the recorder.
	Index int `json:"index"`
	// Process is the logical client that issued the operation.
	Process int `json:"process"`
	// Type is invoke, ok, fail or info.
	Type Type `json:"type"`
	// F names the operation, e.g. "append", "commit-txn", "read".
	F string `json:"f"`
	// Value is workload-specific: the file appended, the tables read, etc.
	Value json.RawMessage `json:"value,omitempty"`
	// Time is nanoseconds since the run started.
	Time int64 `json:"time"`
	// Status is the HTTP status for completions, 0 if no response arrived.
	Status int `json:"status,omitempty"`
	// Error holds the error text for fail and info records.
	Error string `json:"error,omitempty"`
}

// Recorder appends operations to a history safely from many goroutines.
type Recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []Op
}

// NewRecorder starts a history whose clock begins now.
func NewRecorder() *Recorder {
	return &Recorder{start: time.Now()}
}

// Record stamps the operation with its index and time, then stores it.
func (r *Recorder) Record(op Op) Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	op.Index = len(r.ops)
	op.Time = time.Since(r.start).Nanoseconds()
	r.ops = append(r.ops, op)
	return op
}

// Ops returns a copy of every record so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// WriteJSONL writes one JSON record per line.
func WriteJSONL(w io.Writer, ops []Op) error {
	enc := json.NewEncoder(w)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			return err
		}
	}
	return nil
}

// SaveFile writes the history to path as JSON Lines.
func SaveFile(path string, ops []Op) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	if err := WriteJSONL(bw, ops); err != nil {
		return err
	}
	return bw.Flush()
}

// ReadJSONL parses a history written by WriteJSONL.
func ReadJSONL(r io.Reader) ([]Op, error) {
	var ops []Op
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		if len(sc.Bytes()) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			return nil, fmt.Errorf("history line %d: %w", line, err)
		}
		ops = append(ops, op)
	}
	return ops, sc.Err()
}
