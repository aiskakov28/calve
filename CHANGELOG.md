# Changelog

## Unreleased

## v0.1.1 (2026-11-03)

Findings, reproductions, sweep fixes and documentation since v0.1.0. The CLI,
workloads, checkers and history format are unchanged. The README's result
tables come from fresh sweeps of this release's code, and their raw evidence
is attached to the release.

### Findings

One new finding, two v0.1.0 leads minimized and reported, and fix PRs for
three. As of 2026-11-03 every upstream issue and fix PR below is open; none is
merged.

- **Lost transaction changes** (a v0.1.0 lead, now minimized and reported):
  two concurrent transactions on one table are
  both acknowledged, but one change is lost, on the Iceberg REST fixture and on
  Apache Hive 4.2.0 and 4.2.1's REST catalog. Reported in
  [apache/iceberg#18758](https://github.com/apache/iceberg/issues/18758)
  ([triage](docs/triage-reference-atomic.md)).
- **JDBC namespace creates** (a v0.1.0 lead, now reported with a fix):
  concurrent creates of one namespace through
  Iceberg's `JdbcCatalog` get HTTP 500 instead of 409. Reported in
  [apache/iceberg#18759](https://github.com/apache/iceberg/issues/18759), with
  our fix PR [apache/iceberg#18760](https://github.com/apache/iceberg/pull/18760)
  ([triage](docs/triage-jdbc-namespace-create.md)).
- **Lakekeeper schema 503** (new): concurrent schema changes answer the losing commit
  with HTTP 503 instead of 409. Reported in
  [lakekeeper/lakekeeper#2216](https://github.com/lakekeeper/lakekeeper/issues/2216)
  ([triage](docs/triage-lakekeeper-schema-cas.md)). The maintainers' fix PR,
  [lakekeeper/lakekeeper#2219](https://github.com/lakekeeper/lakekeeper/pull/2219),
  covers it and the property deadlocks in
  [lakekeeper/lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201);
  we validated it against our scripts and workloads.
- **Nessie request context** (reported before v0.1.0, now triaged): concurrent
  commits on Nessie 0.109.0 and `main` sometimes get HTTP 500 from an inactive
  request context. Reported in
  [projectnessie/nessie#13271](https://github.com/projectnessie/nessie/issues/13271)
  ([triage](docs/triage-nessie-request-context.md)); the maintainers' fix PR is
  [projectnessie/nessie#13275](https://github.com/projectnessie/nessie/pull/13275).

### Reproductions

- A standalone repro for every finding, in `scripts/`: bash/curl/jq or Go
  standard library. Each sends every request once with a time limit, reads the
  table back, exits 3 when it reproduces the finding, and never counts a failed
  request as evidence. The README's
  [Reproducing findings](README.md#reproducing-findings) section maps each
  finding to its script.

### Sweeps

- Each sweep run and fault campaign uses its own namespace, so Polaris's
  per-namespace location overlap check no longer slows setup past the request
  timeout in long sweeps. The v0.1.1 fault sweep had no errors, against 10
  Polaris setup errors in v0.1.0's.
- The Compose reference fixture runs SQLite in WAL mode, as newer fixture
  images do, to avoid `SQLITE_BUSY`. Both are triaged in
  [the sweep environment note](docs/triage-sweep-environment.md).

### Public readiness

- The Compose stack publishes every catalog port on `127.0.0.1` only, since the
  catalogs run without authentication or with test-only credentials.
- The `ci` workflow has read-only `contents` permissions.
- Comments about the reference fixture describe what was observed instead of
  an unconfirmed lock-up mechanism.

### Documentation

- The README opens with the selected findings and their evidence, then how it
  works and the quick start; the full findings table separates who reported
  each finding from the state of its fix.
- Triage notes for every finding, each with its evidence, the code involved and
  how to reproduce it.

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
