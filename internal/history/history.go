// Package history records every operation a Calve run makes against a
// catalog, in the invoke/complete style Jepsen uses.
//
// The append CLI starts with a "meta" run record. Each REST call produces an
// "invoke" when the client sends the
// request, and a completion ("ok", "fail" or "info") when it gets an answer.
// "info" means the outcome is unknown: a timeout or a 5xx where the commit
// may or may not have happened. Checkers must treat "info" as "maybe".
package history

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Type is the kind of record.
type Type string

const (
	// Meta describes the run, not an operation or an uncertain commit.
	Meta   Type = "meta"
	Invoke Type = "invoke"
	OK     Type = "ok"
	Fail   Type = "fail"
	// Info means the outcome is unknown (timeout, connection reset, 5xx).
	Info Type = "info"
)

// Version is the current JSONL record format. Records without a version are legacy.
const Version = 1

type operationKey struct{}

// WithOperation labels a REST call. Only Process, F, Phase, Round and Value are used.
func WithOperation(ctx context.Context, op Op) context.Context {
	op.Value = append(json.RawMessage(nil), op.Value...)
	return context.WithValue(ctx, operationKey{}, op)
}

// Operation returns the call's labels, or the coordinator process (-1).
func Operation(ctx context.Context) Op {
	if op, ok := ctx.Value(operationKey{}).(Op); ok {
		return op
	}
	return Op{Process: -1}
}

// Op is one record in a history.
type Op struct {
	Version int `json:"version,omitempty"`
	// ID pairs an invocation with its completion within this run.
	ID       int       `json:"id,omitempty"`
	Phase    string    `json:"phase,omitempty"`
	Round    string    `json:"round,omitempty"`
	Request  *Request  `json:"request,omitempty"`
	Response *Response `json:"response,omitempty"`
	// Index is the record's position in the history, assigned by the recorder.
	Index int `json:"index"`
	// Process is the logical client that issued the operation.
	Process int `json:"process"`
	// Type is meta, invoke, ok, fail or info.
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
	mu     sync.Mutex
	start  time.Time
	ops    []Op
	nextID int
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
	op.Version = Version
	if op.Type == Invoke && op.ID == 0 {
		r.nextID++
		op.ID = r.nextID
	}
	op.Time = time.Since(r.start).Nanoseconds()
	r.ops = append(r.ops, clone(op))
	return op
}

// Ops returns a copy of every record so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	for i, op := range r.ops {
		out[i] = clone(op)
	}
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
	bw := bufio.NewWriter(f)
	if err := WriteJSONL(bw, ops); err != nil {
		return errors.Join(err, f.Close())
	}
	return errors.Join(bw.Flush(), f.Close())
}

// ReadJSONL parses a history written by WriteJSONL.
func ReadJSONL(r io.Reader) ([]Op, error) {
	var ops []Op
	reader := bufio.NewReader(r)
	line := 0
	for {
		data, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if len(data) == 0 && readErr == io.EOF {
			break
		}
		line++
		if len(bytes.TrimSpace(data)) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(data, &op); err != nil {
			return nil, fmt.Errorf("history line %d: %w", line, err)
		}
		if op.Version != 0 && op.Version != Version {
			return nil, fmt.Errorf("history line %d: unsupported version %d", line, op.Version)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func clone(op Op) Op {
	op.Value = append(json.RawMessage(nil), op.Value...)
	if op.Request != nil {
		v := *op.Request
		v.Headers = v.Headers.Clone()
		v.Body = append([]byte(nil), v.Body...)
		op.Request = &v
	}
	if op.Response != nil {
		v := *op.Response
		v.Headers = v.Headers.Clone()
		v.Body = append([]byte(nil), v.Body...)
		op.Response = &v
	}
	return op
}
