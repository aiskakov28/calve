package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// interruptible returns a context cancelled by the first SIGINT or SIGTERM, so
// an interrupted campaign stops its load, heals every fault it may have left
// active and saves its run directory. Only the first signal is caught: a second
// one gets the default behavior and ends the process at once, skipping that
// cleanup. stop releases the signals and reports whether one arrived; call it
// before exiting.
func interruptible(w io.Writer) (ctx context.Context, stop func() (interrupted bool)) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case sig := <-signals:
			signal.Stop(signals)
			fmt.Fprintf(w, "calve: %v: stopping the run and healing its faults; interrupt again to exit without cleanup\n", sig)
			cancel(errInterrupted)
		case <-done:
		}
	}()
	return ctx, func() bool {
		signal.Stop(signals)
		close(done)
		cancel(nil)
		return errors.Is(context.Cause(ctx), errInterrupted)
	}
}

var errInterrupted = errors.New("interrupted")
