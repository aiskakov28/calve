package main

import (
	"bytes"
	"testing"
)

// Ending a run cancels its context too; that must not read as an interrupt,
// or every ordinary error would be reported as one.
func TestStopWithoutSignalIsNotAnInterrupt(t *testing.T) {
	var out bytes.Buffer
	ctx, stop := interruptible(&out)
	if stop() {
		t.Fatal("stop reported an interrupt that never came")
	}
	if ctx.Err() == nil {
		t.Fatal("stop left the context running")
	}
}
