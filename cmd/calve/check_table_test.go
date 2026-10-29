package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotCheckCommandOfflineExitCodes(t *testing.T) {
	for _, tt := range []struct {
		fixture string
		code    int
		word    string
	}{
		{"valid", 0, "VALID:"},
		{"invalid", 3, "INVALID:"},
		{"inconclusive", 4, "INCONCLUSIVE:"},
	} {
		t.Run(tt.fixture, func(t *testing.T) {
			// These are synthetic, committed examples, not live catalog results.
			path := filepath.Join("..", "..", "internal", "checker", "testdata", "snapshot-"+tt.fixture+".jsonl")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			output, code := dispatch(t, "check-table", "--timeout", "1s", path)
			if code != tt.code || !strings.Contains(output, tt.word) {
				t.Fatalf("exit %d, output %s", code, output)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("offline check changed the source artifact")
			}
		})
	}
	for _, args := range [][]string{
		{"check-table", "does-not-exist.jsonl"},
		{"check-table", "--timeout", "0", "does-not-exist.jsonl"},
		{"check-table"},
	} {
		output, code := dispatch(t, args...)
		if code != 1 || strings.Contains(output, "VALID:") {
			t.Fatalf("exit %d, output %s", code, output)
		}
	}
}
