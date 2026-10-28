package history

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
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
		if op.ID != i+1 || op.Version != Version || (i > 0 && op.Time < ops[i-1].Time) {
			t.Fatalf("invalid identity/version/time at record %d: %+v", i, op)
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
	if !reflect.DeepEqual(back, ops) {
		t.Fatal("round trip changed history")
	}
}

func TestRecorderSnapshotsAndRawBodyRoundTrip(t *testing.T) {
	r := NewRecorder()
	value := json.RawMessage(`{"key":"a"}`)
	request := &Request{Method: "POST", URL: "http://example/v1/table", Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"updates":[]}`)}
	invoke := r.Record(Op{Type: Invoke, Process: 2, F: "append", Value: value, Request: request})
	response := &Response{Status: 502, Body: []byte{0xff, '<', 'h', 't', 'm', 'l', '>'}}
	r.Record(Op{ID: invoke.ID, Type: Info, Process: 2, F: "append", Response: response, Status: 502, Error: "gateway failed"})
	before := r.Ops()
	value[0] = 'x'
	request.Body[0] = 'x'
	request.Headers.Set("Content-Type", "changed")
	response.Body[0] = 0
	copy := r.Ops()
	copy[0].Request.Body[0] = 'x'
	copy[1].Response.Body[0] = 0
	if !reflect.DeepEqual(before, r.Ops()) {
		t.Fatal("recorder exposed mutable evidence")
	}
	var buf bytes.Buffer
	if err := WriteJSONL(&buf, before); err != nil {
		t.Fatal(err)
	}
	back, err := ReadJSONL(&buf)
	if err != nil || !reflect.DeepEqual(back, before) {
		t.Fatalf("raw evidence did not round trip: %v", err)
	}
}

func TestReadVersionsAndLargeRecord(t *testing.T) {
	if _, err := ReadJSONL(strings.NewReader(`{"version":2,"type":"meta"}`)); err == nil {
		t.Fatal("accepted unsupported format")
	}
	legacy, err := ReadJSONL(strings.NewReader("\n{\"index\":0,\"type\":\"invoke\",\"f\":\"append\"}"))
	if err != nil || len(legacy) != 1 || legacy[0].Version != 0 {
		t.Fatalf("legacy history rejected: %v", err)
	}
	// Base64 makes this line exceed the old 16 MiB scanner limit.
	body := bytes.Repeat([]byte("x"), 13<<20)
	ops := []Op{{Version: Version, Type: Info, Response: &Response{Status: 500, Body: body}}}
	var buf bytes.Buffer
	if err := WriteJSONL(&buf, ops); err != nil {
		t.Fatal(err)
	}
	back, err := ReadJSONL(&buf)
	if err != nil || !bytes.Equal(back[0].Response.Body, body) {
		t.Fatalf("large response lost: %v", err)
	}
}

func TestCredentialRedactionKeepsNumbersAndOrdinaryBytes(t *testing.T) {
	body := []byte(`{ "snapshot-id":9223372036854775806, "config":{"s3.secret-access-key":"server-secret"}}`)
	r := CaptureResponse(500, http.Header{"Set-Cookie": {"session=secret"}}, body)
	if !r.Redacted || bytes.Contains(r.Body, []byte("server-secret")) || !bytes.Contains(r.Body, []byte("9223372036854775806")) {
		t.Fatalf("unsafe/redacted response: %s", r.Body)
	}
	q := CaptureRequest("GET", "http://user:pass@host/v1/config?warehouse=test&token=url-secret", http.Header{"Authorization": {"Bearer client-secret"}}, nil, "client-secret")
	encoded, _ := json.Marshal(q)
	for _, secret := range []string{"user:pass", "url-secret", "client-secret"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("credential leaked: %s", encoded)
		}
	}
	ordinary := []byte(" <html>\xff\x00broken")
	if got := CaptureResponse(502, nil, ordinary); !bytes.Equal(got.Body, ordinary) || got.Redacted {
		t.Fatal("changed ordinary response bytes")
	}
	// A short configured token must not corrupt JSON numbers or syntax.
	short := CaptureResponse(200, nil, []byte(`{"id":11,"text":"1"}`), "1")
	if !json.Valid(short.Body) || !bytes.Contains(short.Body, []byte(`"id":11`)) || !bytes.Contains(short.Body, []byte(`"text":"[REDACTED]"`)) {
		t.Fatalf("short token corrupted JSON: %s", short.Body)
	}
}
