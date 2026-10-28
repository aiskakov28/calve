# Calve

Jepsen-style correctness testing for Apache Iceberg REST catalogs.

Calve runs concurrent commits against open-source Iceberg catalogs while injecting faults, records every request and response, and then checks the history for lost commits, duplicates, and transactions that should have been rejected.

> **Status: early development.** Smoke tests run against the reference catalog, Lakekeeper, and Nessie. Append and write-skew workloads have checkers; write skew is available as a library, with its CLI pending. Fault injection remains planned.

## Why

Every lakehouse vendor now ships an Iceberg REST catalog, and existing test suites check whether endpoints exist and return the right fields. Few check what happens when many writers commit at once while the network or the catalog fails. Concurrency bugs are still found by hand, for example [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552), where two conflicting multi-table transactions both commit on the reference catalog.

## Quick start

Requires Go 1.24+ and Docker.

```sh
docker compose up -d --wait && docker compose run --rm lakekeeper-init
go run ./cmd/calve smoke  --catalog http://localhost:8181
go run ./cmd/calve smoke  --catalog http://localhost:8182/catalog --warehouse calve
go run ./cmd/calve smoke  --catalog http://localhost:8183/iceberg
go run ./cmd/calve append --catalog http://localhost:8181 --clients 8 --per-client 50
```

`append` saves a versioned `history.jsonl` with run settings, paired HTTP evidence, and baseline/final table observations, then prints `VALID` or the anomalies it found. History is also saved on normal error returns, including a failed final read. HTTP bodies are base64 bytes with credential redaction; see the [history format](docs/design.md#history-format). A missing or failed final observation cannot produce `VALID`.

## What Calve checks

| Workload | Anomaly | Status |
| --- | --- | --- |
| append | acknowledged commit missing, rejected commit present, duplicates | working |
| multi-table write skew (#18552) | both conflicting transactions commit | workload and checker implemented; CLI pending |
| compare-and-set schema | more than one winner per round | M4 |
| maintenance races | snapshot expiration racing appends | M4 |

A commit answered with a timeout or 5xx is recorded as **unknown**, never as a failure: the Iceberg REST spec allows such a commit to have been applied. Checkers accept either outcome for unknown operations.

## Catalogs

| Catalog | Status |
| --- | --- |
| Reference (apache/iceberg-rest-fixture 1.10.1) | in docker-compose |
| Apache Polaris | M1 |
| Lakekeeper v0.14.2 (Postgres, Silo for S3) | in docker-compose |
| Nessie 0.109.0 (Postgres, Silo for S3) | in docker-compose |
| Apache Gravitino | M1 |

Calve only tests catalogs you run yourself. It does not load-test managed cloud catalogs.

## Layout

```
cmd/calve/            CLI
internal/rest/        raw Iceberg REST client (no retries, sees every status)
internal/history/     invoke/ok/fail/info records, JSON Lines
internal/workload/    workload generators
internal/checker/     history checkers
internal/fakecatalog/ in-memory catalog with planted bugs, for testing checkers
docs/design.md        design doc
```

## Development

```sh
go test -race ./...
```

Every checker must catch the bugs planted in `internal/fakecatalog` before it is run against a real catalog.

The write-skew library (`workload.RunSkew`) runs concurrent pairs and two sequential stale-requirement controls, recording HTTP evidence and execution summaries. `checker.CheckSkew` checks those records offline; `calve skew` remains issue #10. To capture a real-catalog run while that command is pending, use the opt-in integration test:

```sh
CALVE_SKEW_CATALOG=http://localhost:8181 CALVE_SKEW_HISTORY="$PWD/skew.jsonl" \
  go test -v ./internal/workload -run '^TestSkewLiveCatalog$' -count=1
```

On PowerShell, set `$env:CALVE_SKEW_CATALOG` and `$env:CALVE_SKEW_HISTORY` before running the same `go test` command. Use an absolute history path; Go runs this test from `internal/workload`. Without a history path it uses a temporary file. `CALVE_SKEW_WAREHOUSE` and `CALVE_TOKEN` are optional. This runs ten concurrent rounds and one control in each direction, saves history even on workload errors, and retains the fresh tables for inspection. Test success means the workload completed; it does not mean the catalog passed the skew checker.

Replay a saved artifact without contacting the catalog:

```sh
CALVE_SKEW_RECHECK="$PWD/skew.jsonl" \
  go test -v ./internal/checker -run '^TestCheckSkewSavedHistory$' -count=1
```

The test prints the verdict and counts; optional `CALVE_SKEW_EXPECT=valid` (or `invalid`/`inconclusive`) asserts a particular result. In Go, use `history.ReadJSONL` followed by `checker.CheckSkew`. The result is `valid` only with complete usable evidence, `invalid` for a proven finding, or `inconclusive` when evidence cannot decide. A write-skew finding is an isolation lead under the serializable model, with direct contract findings reported separately. Full schema evidence can resolve an unknown as committed; absence alone leaves it uncertain. See [checker rules](docs/design.md#checker-rules-and-unknown-outcomes).

## Reporting findings

Before filing anything upstream: reproduce it from a seed, shrink it to a standalone script, confirm the spec actually promises the violated behavior, and search existing issues.

## License

Apache-2.0

Done by [aiskakov28](https://github.com/aiskakov28) and [aldusialn](https://github.com/aldusialn).
