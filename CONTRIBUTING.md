# Contributing to Calve

Thanks for your interest. Calve is only useful if its verdicts can be trusted:
a false anomaly wastes a catalog team's time, and a missed one hides a real bug.
Most of what follows comes down to that.

## Setup

You need Go 1.24 or later, Docker and `jq`.

```sh
go vet ./...
go test -race ./...
```

The unit tests need no catalogs. To run against real catalogs, start the
Compose stack with `scripts/up.sh` (see the [README](README.md#quick-start)).

Some tests talk to the Compose stack and only run when asked:

| Test | Enable with |
| --- | --- |
| Toxiproxy faults take effect and heal | `CALVE_TOXIPROXY=http://localhost:8474 go test -run ToxiproxyLive ./internal/fault` |
| Process faults keep catalog data | `CALVE_DOCKER_FAULTS=1 go test -run DockerLive ./internal/fault` |
| Skew workload against a live catalog | `CALVE_SKEW_CATALOG=<url> go test -run TestSkewLiveCatalog ./internal/workload`; other variables are in `skew_test.go` |

## Rules for changes

**Unknown means unknown.** A timeout, reset, 5xx or any status the REST spec
doesn't document is recorded as `info`, and checkers must allow it to have
either committed or not. Absence of an effect never proves a rejection.

**The REST client records, nothing more.** It never retries a request and never
reinterprets a response.

**New checkers need controls.** A checker is trusted only after it catches bugs
planted in `internal/fakecatalog` and passes a correct fake with no findings.
Add both tests with the checker, and add a row to
[checker controls](docs/checker-controls.md).

**Valid is narrow.** `VALID` means the named invariant held in the rounds that
were fully checked. Report inconclusive rounds separately.

**No sleeps in tests.** Use barriers, channels or explicit waits.

Go code is `gofmt`-formatted, returns errors with context
(`fmt.Errorf("load table: %w", err)`) and never drops one silently. New
dependencies need a reason in the pull request. Docker images are pinned to
exact tags.

## Reporting a catalog bug

Calve's findings go to other projects, so the bar is high. Before filing
anything upstream:

1. **Reproduce it** from a seed or a saved history, and run the controls (for
   example the sequential stale-requirement check) so the catalog is shown to
   handle the simple case correctly.
2. **Shrink it** to a standalone script that doesn't need Calve.
3. **Quote the promise it breaks**: the spec text or the catalog's own
   documentation, at a pinned version. If you can't quote one, it's a lead, not
   a bug.
4. **Find the cause** in the catalog's source at the same version, if you can.
5. **Search** the project's existing issues and pull requests.

Write it up as a triage note in `docs/` (the existing `triage-*.md` files show
the shape) and open it as a pull request for review before filing upstream.

## Pull requests

- One change per pull request, with a clear description of what and why.
- Include the commands you ran and their output: at least `go vet ./...` and
  `go test -race ./...`, plus any live runs.
- Update the docs when behavior changes, and only describe what the code does
  today.
- CI must pass. It runs the unit tests on Linux and Windows and a smoke job
  that starts every catalog and runs the gates.

For a bug in Calve, open an issue with the **Wrong verdict** or **Other
problem** form. Security problems go through the private report described in
[SECURITY.md](SECURITY.md).
