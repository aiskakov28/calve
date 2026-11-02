# Changelog

## Unreleased

- **Lakekeeper schema 503:** concurrent schema changes on Lakekeeper answer the
  losing commit with HTTP 503 instead of 409. Filed as
  [lakekeeper/lakekeeper#2216](https://github.com/lakekeeper/lakekeeper/issues/2216),
  with a [triage note](docs/triage-lakekeeper-schema-cas.md) and a standalone
  repro, `scripts/repro-lakekeeper-schema-cas.go`. The fix offered there also
  stops the property deadlocks in
  [lakekeeper/lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201).
- **Lost transaction changes:** two concurrent transactions on one table are
  both acknowledged, but one change is lost, on the reference fixture and on
  Apache Hive 4.2.0 and 4.2.1's REST catalog. Minimized to
  `scripts/repro-transaction-lost-update.sh`, with the cause traced in the
  [triage note](docs/triage-reference-atomic.md). Not filed yet.

## v0.1.0

First public release.

Calve is at 0.x: the CLI flags, the history record format (version 1) and the
run directory format (`calve-run-v4`) can still change. `calve check` keeps
reading the run directory formats it has written before.

### What's included

- **Workloads:** append, skew, atomic, snapshot, schema and maintenance. Each
  saves the raw HTTP evidence of every request and is checked offline with
  `calve check` or `calve check-table`.
- **Checkers:** each is tested against bugs planted in an in-memory fake catalog
  before it is trusted ([controls](docs/checker-controls.md)). Snapshot
  linearizability uses [Porcupine](https://github.com/anishathalye/porcupine).
- **Fault campaigns** (`calve run`): seeded, reproducible plans of latency,
  reset and partition through Toxiproxy, and kill, pause and restart through
  Docker. A run directory holds the plan, the fault log, the history, the
  result and its provenance. `calve shrink` reduces a failing run.
- **Catalogs:** a Docker Compose stack with the Iceberg REST fixture 1.10.1,
  Lakekeeper v0.14.2, Nessie 0.109.0, Polaris 1.8.0 and Gravitino 1.3.1.
- **Sweeps:** `scripts/sweep.sh` and the `sweep` workflow run every applicable
  workload on every catalog; `calve results` turns them into the README tables.
- **GitHub Action:** runs the REST-only workloads against a catalog in another
  project's CI.

### Outcome rules

- Only statuses the Iceberg REST spec documents for the endpoints Calve
  calls are definite: 200 and 204 mean applied, and 400, 401, 403, 404, 406,
  409 and 419 mean rejected. Anything else (a timeout, a reset, a 5xx, or an
  undocumented status such as 202 or 429) is unknown.
- An unknown commit is never counted as rejected.

### Safety

- Ctrl+C or SIGTERM heals every fault before Calve exits.
- Histories, plans, results and provenance are written as new files readable
  only by their owner, and never overwrite an earlier run.
