# Calve

Jepsen-style correctness testing for Apache Iceberg REST catalogs.

Calve runs concurrent commits against open-source Iceberg catalogs while injecting faults, records every request and response, and then checks the history for lost commits, duplicates, and transactions that should have been rejected.

> **Status: early development.** The append and write-skew workloads run from the CLI with checkers. CI checks the skew gate on reference, Lakekeeper, and Nessie. `calve run` runs the append workload under a seeded fault plan against one catalog and saves a run directory that `calve check` re-checks offline; CI runs a seeded campaign on Lakekeeper.

## Why

Every lakehouse vendor now ships an Iceberg REST catalog, and existing test suites check whether endpoints exist and return the right fields. Few check what happens when many writers commit at once while the network or the catalog fails. Concurrency bugs are still found by hand, for example [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552), where two conflicting multi-table transactions both commit on the reference catalog.

## Quick start

Requires Go 1.24+ and Docker.

```sh
docker compose up -d --wait
docker compose run --rm lakekeeper-init && docker compose run --rm polaris-init
go run ./cmd/calve smoke  --catalog http://localhost:8181
go run ./cmd/calve smoke  --catalog http://localhost:8182/catalog --warehouse calve
go run ./cmd/calve smoke  --catalog http://localhost:8183/iceberg
go run ./cmd/calve smoke  --catalog http://localhost:8184/api/catalog --warehouse calve --token "$POLARIS_TOKEN"
go run ./cmd/calve append --catalog http://localhost:8181 --clients 8 --per-client 50
go run ./cmd/calve skew   --catalog http://localhost:8181 --rounds 10
```

Set `POLARIS_TOKEN` before the Polaris smoke: Polaris needs an OAuth token for its root principal (`root` / `calve-secret`, test-only).

```sh
export POLARIS_TOKEN=$(curl -s -X POST http://localhost:8184/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=calve-secret \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
```

Tokens last an hour and survive Polaris restarts (it signs them with a fixed test key).

`skew` runs conflicting two-table transaction pairs plus two sequential stale-requirement controls, saves `history-skew.jsonl`, and prints `VALID`, `INVALID` with the findings, or `INCONCLUSIVE` with the reasons. Exit status is 0, 3 or 4 respectively, and 1 on error.

`append` saves a versioned `history.jsonl` with run settings, paired HTTP evidence, and baseline/final table observations, then prints `VALID`, `INVALID` (exit 3), or `INCONCLUSIVE` (exit 4). History is also saved on normal error returns, including a failed final read. HTTP bodies are base64 bytes with credential redaction; see the [history format](docs/design.md#history-format). A missing or failed final observation cannot produce `VALID`.

## What Calve checks

| Workload | Anomaly | Status |
| --- | --- | --- |
| property append | committed property missing, rejected property present | working; property maps cannot expose duplicate occurrences |
| multi-table write skew (#18552) | both conflicting transactions commit | working |
| multi-table marker transactions | acknowledged effects lost, rejected effects visible, non-atomic visibility leads | working; `calve atomic` and offline `calve check` |
| snapshot compare-and-set | non-linearizable snapshot commits and reads | offline Porcupine checker; live workload pending |
| compare-and-set schema | more than one winner per round | M4 |
| maintenance races | snapshot expiration racing appends | M4 |

A commit answered with a timeout or 5xx is recorded as **unknown**, never as a rejection. Complete attributable effects can resolve it as committed while preserving the original outcome. Absent effects leave it inconclusive. Both workload commands use up to 5 seconds of extra table GETs for unresolved commits; `--readback-timeout 0` disables them, and `--readback-interval` defaults to 100 ms. These reads never resubmit a commit. A read-back deadline is not proof that server work has finished. See [unknown outcomes](docs/unknown-outcomes.md).

## Catalogs

| Catalog | Status |
| --- | --- |
| Reference (apache/iceberg-rest-fixture 1.10.1) | in docker-compose |
| Apache Polaris 1.8.0 (Postgres, Silo for S3) | in docker-compose; needs an OAuth token; known write skew ([triage](docs/triage-polaris-skew.md)); commit retries skipped, filed as apache/polaris#5907 ([triage](docs/triage-409-without-requirements.md)) |
| Lakekeeper v0.14.2 (Postgres, Silo for S3) | in docker-compose; concurrent commits deadlock, filed as lakekeeper/lakekeeper#2201 with a fix proposed ([triage](docs/triage-409-without-requirements.md)) |
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

Replay a saved `calve skew` history without contacting the catalog:

```sh
CALVE_SKEW_RECHECK="$PWD/history-skew.jsonl" \
  go test -v ./internal/checker -run '^TestCheckSkewSavedHistory$' -count=1
```

The test prints the verdict and counts; optional `CALVE_SKEW_EXPECT=valid` (or `invalid`/`inconclusive`) asserts a particular result. In Go, use `history.ReadJSONL` followed by `checker.CheckSkew`. The result is `valid` only with complete usable evidence, `invalid` for a proven finding, or `inconclusive` when evidence cannot decide. A write-skew finding is an isolation lead under the serializable model, with direct contract findings reported separately. Full schema evidence can resolve an unknown as committed; absence alone leaves it uncertain. See [checker rules](docs/design.md#checker-rules-and-unknown-outcomes).

For property-append histories, use `history.ReadJSONL` followed by `checker.CheckAppendHistory`. It validates the recorded table identity, append requests, paired outcomes, and baseline/final/recovery reads. Each operation retains its raw outcome and separate commitment resolution with the supporting observation IDs. The lower-level `CheckAppend(ops, finalKeys)` remains available when the caller supplies a trusted final state.

For single-table snapshot CAS histories, `calve check-table --timeout 5s <history.jsonl>`
runs Porcupine against an explicit atomic snapshot model. Unknown commits may
apply after their client timeout; absent effects remain inconclusive. See the
[snapshot checker contract and synthetic examples](docs/single-table-checker.md).
The current property-append commands do not generate snapshot histories; a live
snapshot/file workload remains separate work.

Run transactions that update every participating table, then replay their
evidence offline:

```sh
go run ./cmd/calve atomic --catalog http://localhost:8182/catalog --warehouse calve \
  --tables 2 --clients 4 --per-client 10 --out history-atomic.jsonl
go run ./cmd/calve check history-atomic.jsonl
```

The [multi-table checker](docs/multi-table-checker.md) uses unique properties,
keeps absent unknown effects inconclusive, and distinguishes same-table
regressions and cross-table visibility leads from definite-outcome effect
findings. `calve atomic` reads during commits by default; `--readers` and
`--read-interval` control sampling. With readers enabled, missing usable
concurrent reads leaves the run inconclusive. The reference fixture has a
[reproduced missing-marker lead](docs/triage-reference-atomic.md); Lakekeeper,
Nessie and Polaris passed the initial 40-transaction runs.

## Seeded fault plans

```sh
go run ./cmd/calve run --seed 42 --plan-only \
  --targets reference,lakekeeper,nessie --plan fault-plan-42.json
```

The same seed, normalized configuration, and scheduler version produce the same complete plan. Targets and fault kinds are sorted before selection, with a private RNG independent of workload randomness. All JSON durations are nanoseconds. Each fault has an ID, target, kind, parameters, start offset, duration, and healing action. V1 schedules one fault at a time and reserves time for verified healing. The seed reproduces the planned schedule; it cannot reproduce thread timing or a failure.

With `--plan-only` it saves the plan and reports planning mode; it does not run a workload or print a correctness verdict. Choose another output path for a subsequent plan: existing artifacts are not overwritten. `calve run -h` lists timing options. See [the scheduler and controller contract](docs/fault-scheduler.md) for the library executor, cleanup behavior, and integration work remaining.

For network faults, Toxiproxy (in `docker-compose.yml`) proxies each catalog on the same paths: reference `:28181`, Lakekeeper `:28182`, Nessie `:28183`, Polaris `:28184`, with its API on `:8474`. `fault.Toxiproxy` adds and removes latency, reset and partition toxics there; point `--catalog` at a proxy port to run a workload through them.

## Fault campaigns

```sh
go run ./cmd/calve run --seed 7 --targets reference --catalog http://localhost:28181
go run ./cmd/calve run --seed 7 --targets lakekeeper --catalog http://localhost:28182/catalog --warehouse calve
```

Without `--plan-only`, `calve run` sets up a fresh table, executes the plan while `--clients` append every `--interval`, heals every fault, waits for the catalog to answer (`--ready-timeout`), then does the final read, read-back and append check. Exit codes match `append`: 0 valid, 3 invalid, 4 inconclusive, 1 when a fault could not be applied or healed. Network faults need `--catalog` on the target's proxy port; `calve run` refuses a direct URL. `--proxy` and `--container` override the docker-compose names. Each run saves a new directory (`--out-dir`, default `runs/<run ID>`) with `plan.json`, `faults.jsonl` (controller actions), `history.jsonl` (workload HTTP evidence) and `result.json` (the verdict). `calve check <dir>` re-checks it without a catalog: the plan must match its seed, the fault history must record that plan and every fault applied and healed, the workload history must name both, and the recomputed append verdict must match `result.json`. A run that ended in an error stays an error. It returns the same exit codes as the run.

For process faults, `fault.Docker` kills (SIGKILL), pauses or stops (SIGTERM with a grace period) a catalog container through the docker CLI and heals by starting or unpausing the same container, so catalog data survives. All three compose catalogs keep their tables across each fault.

## Reporting findings

Before filing anything upstream: reproduce it from a seed, shrink it to a standalone script, confirm the spec actually promises the violated behavior, and search existing issues.

## License

Apache-2.0

Done by [aiskakov28](https://github.com/aiskakov28) and [aldusialn](https://github.com/aldusialn).
