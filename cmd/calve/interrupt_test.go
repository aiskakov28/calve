//go:build unix

package main

import (
	"bytes"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFirstSignalCancelsTheRun(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			var out bytes.Buffer
			ctx, stop := interruptible(&out)
			if err := syscall.Kill(os.Getpid(), sig); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("signal did not cancel the run")
			}
			if !stop() {
				t.Fatal("stop did not report the interrupt")
			}
			if !strings.Contains(out.String(), "healing its faults") {
				t.Fatalf("no notice of the cleanup: %q", out.String())
			}
		})
	}
}
