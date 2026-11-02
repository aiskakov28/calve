package history

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestSaveFileIsPrivateAndNeverReplacesEvidence(t *testing.T) {
	rec := NewRecorder()
	rec.Record(Op{Type: Invoke, F: "commit-table", Process: 1})
	path := filepath.Join(t.TempDir(), "history.jsonl")
	if err := SaveFile(path, rec.Ops()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Windows keeps no Unix permission bits.
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("history readable beyond its owner: %v", info.Mode().Perm())
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := ReadJSONL(f)
	f.Close()
	if err != nil || !reflect.DeepEqual(saved, rec.Ops()) {
		t.Fatalf("saved history differs: %v", err)
	}

	before, _ := os.ReadFile(path)
	if err := SaveFile(path, nil); !errors.Is(err, os.ErrExist) {
		t.Fatalf("an existing history was not refused: %v", err)
	}
	if after, _ := os.ReadFile(path); string(after) != string(before) {
		t.Fatal("an existing history was changed")
	}
}
