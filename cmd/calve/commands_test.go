package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aiskakov28/calve/internal/fakecatalog"
)

// Run the actual command dispatcher in a subprocess so exit codes and command
// coexistence are covered, including skew's intentional nonzero result.
func TestCommandDispatchProcess(t *testing.T) {
	if os.Getenv("CALVE_TEST_DISPATCH") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"calve"}, os.Args[i+1:]...)
			main()
			return
		}
	}
	t.Fatal("missing command arguments")
}

func dispatch(t *testing.T, args ...string) (string, int) {
	t.Helper()
	command := exec.Command(os.Args[0], append([]string{"-test.run=^TestCommandDispatchProcess$", "--"}, args...)...)
	command.Env = append(os.Environ(), "CALVE_TEST_DISPATCH=1")
	output, err := command.CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	if exited, ok := err.(*exec.ExitError); ok {
		return string(output), exited.ExitCode()
	}
	t.Fatal(err)
	return "", -1
}

func TestSkewAndPlanCommandsCoexist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if output, code := dispatch(t, "run", "--seed", "42", "--plan-only", "--plan", path); code != 0 || !strings.Contains(output, "PLAN ONLY:") || strings.Contains(output, "VALID:") {
		t.Fatalf("plan dispatch: exit %d, %s", code, output)
	}
	for _, buggy := range []bool{false, true} {
		fake := skewFake(t, fakecatalog.Bugs{WriteSkew: buggy}, 1)
		path := filepath.Join(t.TempDir(), "history.jsonl")
		output, code := dispatch(t, "skew", "--catalog", fake.Server.URL, "--rounds", "1", "--out", path)
		wantCode, wantText := 0, "VALID:"
		if buggy {
			wantCode, wantText = 3, "INVALID: 1 write-skew"
		}
		if code != wantCode || !strings.Contains(output, wantText) {
			t.Fatalf("skew dispatch: exit %d, %s", code, output)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatal("skew command failed to save evidence")
		}
	}
}
