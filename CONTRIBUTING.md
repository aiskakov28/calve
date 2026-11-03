# Contributing to Calve

Thanks for your interest. Calve is only useful if its verdicts can be trusted:
a false anomaly wastes a catalog team's time, and a missed one hides a real bug.
Most of what follows comes down to that.

## Setup

You need Go 1.24 or later, Docker and `jq`. The Compose stack needs about 2 GB of
memory when idle and 2.2 GB of disk for images; give Docker at least 4 GB.

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

## Extending Calve

Most contributions add a catalog, a workload or a reproduction. For a first
contribution, look at the issues labelled
[good first issue](https://github.com/aiskakov28/calve/labels/good%20first%20issue). Earlier pull
requests are the best guide; the lists below name every place to touch.

### Adding a catalog

[#98](https://github.com/aiskakov28/calve/pull/98) added Gravitino this way.

1. **`docker-compose.yml`:** the catalog service with an exact image tag (or an
   inline build on a pinned base image), its database if it has one, a
   healthcheck that dependents wait on, a port published on `127.0.0.1` (the
   next free `818x`), and a Toxiproxy proxy on the matching `2818x` port in the
   `toxiproxy.json` config. Any one-shot setup job must be safe to re-run.
2. **`cmd/calve/campaign.go`:** an entry in `composeTargets` (proxy name and
   Compose service), so fault campaigns can reach it.
3. **Smoke tests:** a line in `scripts/up.sh`, and a smoke step through
   Toxiproxy in `.github/workflows/ci.yml`.
4. **Sweeps:** the catalog list, the URLs in `run_one` and `run_campaign`, and
   `skipped` for any workload it can't run, in `scripts/sweep.sh`; the `all`
   list in `.github/workflows/sweep.yml`; and the catalog loops in
   `scripts/check-snapshots.sh` and `scripts/check-maintenance.sh`.
5. **README:** the catalog endpoints table, and the result tables after a sweep.

Before opening the pull request, run `scripts/up.sh` and one sweep cell, for
example `CATALOGS=<name> WORKLOADS=append scripts/sweep.sh 1 results/try`.

### Adding a workload and its checker

[#71](https://github.com/aiskakov28/calve/pull/71) (schema) and
[#72](https://github.com/aiskakov28/calve/pull/72) (maintenance) are the models.

1. **`internal/workload/<name>.go`:** sends every request once through
   `internal/rest`, with no retries, and records each one in the history.
2. **`internal/checker/<name>.go`** and its tests.
3. **Controls:** plant the bug the checker exists to catch in
   `internal/fakecatalog`, show that the checker finds it and passes the
   correct fake, and add a row to
   [checker controls](docs/checker-controls.md).
4. **`cmd/calve`:** the subcommand (`<name>.go`, registered in `main.go`); the
   history dispatch in `atomic.go`, where `calve check <history>` picks a
   checker by the workload name the history records; and `recordedWorkloads` in
   `results.go`, so `calve results` includes it.
5. **Under faults (optional):** a case in `run.go` and an entry in
   `campaignCheckers` in `campaign.go`, with a campaign test. [#80](https://github.com/aiskakov28/calve/pull/80) did this for the
   maintenance workload.
6. **CI and sweeps:** a gate in `.github/workflows/ci.yml`, and the workload in
   the `case` and default `workloads` list of `scripts/sweep.sh`.
7. **Docs:** `docs/<name>-workload.md`, `docs/design.md`, and the README's
   workload table.

### Adding a reproduction

Every finding gets a standalone script in `scripts/` that doesn't need Calve:
bash, curl and jq, or a Go file with `//go:build ignore` using only the
standard library. `scripts/repro-transaction-lost-update.sh` is a good
template. Like it, a repro should:

- send every request once, with a time limit (`TIMEOUT`), and count a failed
  transfer as no answer rather than as the status it saw;
- read the table back to check what each answer claimed;
- exit 3 when it reproduces, 0 when it doesn't, 4 when a run can't decide and
  1 on a setup error;
- remove its temporary files, and never print or save a token.

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
