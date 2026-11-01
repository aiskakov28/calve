# Calve

Jepsen-style correctness testing for Apache Iceberg REST catalogs.

Calve runs concurrent commits against open-source Iceberg catalogs while injecting faults, records every request and response, and then checks the history for lost commits, duplicates, and transactions that should have been rejected.

## Results

Every workload on every Compose catalog, on GitHub's Linux runners. Each cell
counts runs by verdict, then names any anomaly with its rate per 1,000 commit
requests.

Without faults, 5 runs per cell ([sweep run 40992074092](https://github.com/aiskakov28/calve/actions/runs/40992074092), 2026-10-30). Gravitino joined later and ran on its own ([sweep run 41276069932](https://github.com/aiskakov28/calve/actions/runs/41276069932), 2026-11-01). It has no multi-table transaction endpoint, so skew and atomic do not apply to it:

| Workload | Reference | Lakekeeper | Nessie | Polaris | Gravitino |
| --- | --- | --- | --- | --- | --- |
| append | 5 valid | 5 inconclusive | 5 inconclusive | 5 valid | 5 valid |
| atomic | **5 invalid**<br>lost, non-atomic-visibility · 720 per 1k | 5 valid | 3 valid, 2 inconclusive | 5 valid | not supported |
| maintenance | 5 valid | 5 valid | 5 inconclusive | 5 valid | 5 valid |
| schema | 5 valid | 5 inconclusive | 5 valid | 5 valid | 5 valid |
| skew | **5 invalid**<br>write-skew · 400 per 1k | 5 valid | 5 valid | **5 invalid**<br>write-skew · 391.7 per 1k | not supported |
| snapshot | 5 valid | 5 valid | 5 valid | 5 valid | 5 valid |

Under faults, one 60-second run per fault kind, each plan seeded with 1 and
holding one kind: latency, reset, partition, kill, pause and restart. The
reference fixture runs only the three network kinds ([sweep run 41246628323](https://github.com/aiskakov28/calve/actions/runs/41246628323), 2026-10-31):

| Workload | Reference | Lakekeeper | Nessie | Polaris | Gravitino |
| --- | --- | --- | --- | --- | --- |
| append | 1 valid, 2 inconclusive | 1 valid, 5 inconclusive | 1 valid, 5 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive |
| atomic | **3 invalid**<br>lost, non-atomic-visibility · 655.5 per 1k | 1 valid, 5 inconclusive | 6 inconclusive | 2 valid, 4 inconclusive | not supported |
| maintenance | 1 valid, 2 inconclusive | 2 valid, 4 inconclusive | 6 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive |
| schema | 1 valid, 2 inconclusive | 6 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive |
| skew | **3 invalid**<br>write-skew · 486.2 per 1k | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | **6 invalid**<br>write-skew · 394.3 per 1k | not supported |
| snapshot | 1 valid, 2 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive |

An earlier sweep of the same plans without Gravitino ([run 41222078924](https://github.com/aiskakov28/calve/actions/runs/41222078924)) gave the same 12 invalid cells with the same anomaly kinds. Only 4 of its 126 cells changed, each between valid and inconclusive, depending on whether a fault left a commit unknown.

The faults found nothing the no-fault runs did not, and Gravitino showed no anomaly:

- **Reference skew:** both conflicting multi-table transactions commit, the
  lead in [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552).
- **Reference atomic:** acknowledged transactions lose their effects, a
  reproduced lead still being minimized ([triage](docs/triage-reference-atomic.md)).
- **Polaris skew:** the same write skew, a known upstream bug,
  [apache/polaris#5807](https://github.com/apache/polaris/issues/5807)
  ([triage](docs/triage-polaris-skew.md)).

A timeout or 5xx leaves a commit's outcome unknown, and Calve never counts an
absent effect as a rejection, so such a run is inconclusive rather than valid.
Under faults that is expected. Without faults it comes from the catalog:

- **Lakekeeper append:** concurrent commits to one table deadlock and time out, filed as
  [lakekeeper/lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201).
- **Lakekeeper schema:** concurrent schema changes get HTTP 503 from a
  duplicate-key database error.
- **Nessie append and atomic:** some commits get HTTP 500 from an inactive
  request context.
- **Nessie maintenance:** Nessie returns only the current snapshot, so no
  expiration can be checked.

[Sweeps](#sweeps) describes how to rerun them.

## Why

Every lakehouse vendor now ships an Iceberg REST catalog, and existing test suites check whether endpoints exist and return the right fields. Few check what happens when many writers commit at once while the network or the catalog fails. Concurrency bugs are still found by hand, for example [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552), where two conflicting multi-table transactions both commit on the reference catalog.

## Quick start

Requires Go 1.24+, Docker and jq.

```sh
scripts/up.sh
```

It starts the five catalogs, runs their setup jobs, builds `bin/calve` and
smoke-tests each catalog. It is safe to re-run.

Then reproduce apache/iceberg#18552 on the reference catalog, and check that
Lakekeeper does not have it:

```console
$ bin/calve skew --catalog http://localhost:8181 --rounds 10
skew on http://localhost:8181: 12 rounds, 22 ok, 2 rejected, 0 unknown (0 resolved as committed)
...
INVALID: 10 write-skew
write-skew is an isolation lead under the serializable model, not a confirmed catalog bug
$ bin/calve skew --catalog http://localhost:8182/catalog --warehouse calve --rounds 10
skew on http://localhost:8182/catalog: 12 rounds, 12 ok, 12 rejected, 0 unknown (0 resolved as committed)
VALID: at most one transaction committed in every round; stale controls were rejected
```

Each round starts two fresh tables, A and B, at schema 0. Two clients send
their multi-table transactions at the same moment, from that same starting
point. One adds a schema to A and asserts that B is still at schema 0. The
other adds a schema to B and asserts the same of A. Whichever commits second
asserts something that is no longer true. A catalog that runs each
transaction as one serializable step therefore commits at most one of them.

The last two rounds are controls. They send the same pair one after the
other, so the catalog must reject the second. That shows it checks the
requirement at all.

On the reference catalog, both transactions committed in all ten concurrent
rounds (20 ok). The controls went one each way (2 ok, 2 rejected). So the
requirement is checked, but not against a transaction that commits alongside
it. Calve reports `INVALID` and exits 3. It calls this an isolation lead
rather than a confirmed bug, because the REST spec does not state how these
requirements are isolated. Lakekeeper committed exactly one transaction per
round and rejected the other, so it is `VALID`.

Either way, `history-skew.jsonl` keeps every request and response, so the
verdict can be checked again without the catalog (see
[Development](#development)).

Commands against Polaris need an OAuth token for its root principal (`root` / `calve-secret`, test-only):

```sh
export POLARIS_TOKEN=$(curl -s -X POST http://localhost:8184/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=calve-secret \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
```

Tokens last an hour and survive Polaris restarts (it signs them with a fixed test key).

`skew` runs conflicting two-table transaction pairs plus two sequential stale-requirement controls, saves `history-skew.jsonl`, and prints `VALID`, `INVALID` with the findings, or `INCONCLUSIVE` with the reasons. Exit status is 0, 3 or 4 respectively, and 1 on error.

`append` saves a versioned `history.jsonl` with run settings, paired HTTP evidence, and baseline/final table observations, then prints `VALID`, `INVALID` (exit 3), or `INCONCLUSIVE` (exit 4). History is also saved on normal error returns, including a failed final read. HTTP bodies are base64 bytes with credential redaction; see the [history format](docs/design.md#history-format). A missing or failed final observation cannot produce `VALID`.

## Use in CI

A catalog project can run Calve on every push with the GitHub Action in this
repository. Start the catalog in an earlier step, then:

```yaml
- uses: aiskakov28/calve@main
  id: calve
  with:
    catalog: http://localhost:8181
    warehouse: ""                         # if the catalog needs one
    token: ${{ secrets.CATALOG_TOKEN }}   # if it needs a bearer token
- if: always()
  uses: actions/upload-artifact@v4
  with:
    name: calve-results
    path: calve-results/
```

It runs the append, skew, atomic and schema workloads, which need only the REST
API, and puts a verdict table in the job summary. It fails the job on a finding
(`INVALID`) or an error. With `fail-on: inconclusive` it also fails when a
run cannot be decided. `workloads` narrows the set, `out-dir` moves the
histories and logs, and the `verdict` output gives the worst verdict. Tokens
are passed in the environment, and Calve redacts them from the saved
histories. The same check runs locally with
`CATALOG=<url> scripts/check-catalog.sh`.

## What Calve checks

| Workload | Anomaly | Status |
| --- | --- | --- |
| property append | committed property missing, rejected property present | working; property maps cannot expose duplicate occurrences |
| multi-table write skew (#18552) | both conflicting transactions commit | working |
| multi-table marker transactions | acknowledged effects lost, rejected effects visible, non-atomic visibility leads | working; `calve atomic` and offline `calve check` |
| snapshot compare-and-set | non-linearizable snapshot commits and reads | `calve snapshot`, real empty manifest lists, offline Porcupine replay |
| compare-and-set schema | more than one winner per round, lost or wrongly rejected schema changes | `calve schema` and offline `calve check`; see [schema workload](docs/schema-workload.md) |
| maintenance races | expiration removing unlisted snapshots, dangling or regressed main, rejected effects applied | `calve maintenance` and offline `calve check`; see [maintenance workload](docs/maintenance-workload.md) |

A commit answered with a timeout or 5xx is recorded as **unknown**, never as a rejection. Complete attributable effects can resolve it as committed while preserving the original outcome. Absent effects leave it inconclusive. Both workload commands use up to 5 seconds of extra table GETs for unresolved commits; `--readback-timeout 0` disables them, and `--readback-interval` defaults to 100 ms. These reads never resubmit a commit. A read-back deadline is not proof that server work has finished. See [unknown outcomes](docs/unknown-outcomes.md).

## Catalogs

| Catalog | Status |
| --- | --- |
| Reference (apache/iceberg-rest-fixture 1.10.1) | in docker-compose |
| Apache Polaris 1.8.0 (Postgres, Silo for S3) | in docker-compose; needs an OAuth token; known write skew ([triage](docs/triage-polaris-skew.md)); commit retries skipped, filed as apache/polaris#5907 with a fix PR open ([triage](docs/triage-409-without-requirements.md)) |
| Lakekeeper v0.14.2 (Postgres, Silo for S3) | in docker-compose; concurrent commits deadlock, filed as lakekeeper/lakekeeper#2201 with a fix proposed ([triage](docs/triage-409-without-requirements.md)) |
| Nessie 0.109.0 (Postgres, Silo for S3) | in docker-compose |
| Apache Gravitino 1.3.1 Iceberg REST server (Postgres through Iceberg's JDBC catalog, Silo for S3) | in docker-compose; its image adds the Postgres JDBC driver. It has no multi-table transaction endpoint (`/v1/config` does not list one and the route answers 404), so skew and atomic do not apply and sweeps skip them |

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

Every checker must catch the bugs planted in `internal/fakecatalog` before it is run against a real catalog. The [control coverage matrix](docs/checker-controls.md) maps server faults to findings and records the model limits.

Replay a saved `calve skew` history without contacting the catalog:

```sh
CALVE_SKEW_RECHECK="$PWD/history-skew.jsonl" \
  go test -v ./internal/checker -run '^TestCheckSkewSavedHistory$' -count=1
```

The test prints the verdict and counts; optional `CALVE_SKEW_EXPECT=valid` (or `invalid`/`inconclusive`) asserts a particular result. In Go, use `history.ReadJSONL` followed by `checker.CheckSkew`. The result is `valid` only with complete usable evidence, `invalid` for a proven finding, or `inconclusive` when evidence cannot decide. A write-skew finding is an isolation lead under the serializable model, with direct contract findings reported separately. Full schema evidence can resolve an unknown as committed; absence alone leaves it uncertain. See [checker rules](docs/design.md#checker-rules-and-unknown-outcomes).

For property-append histories, use `history.ReadJSONL` followed by `checker.CheckAppendHistory`. It validates the recorded table identity, append requests, paired outcomes, and baseline/final/recovery reads. Each operation retains its raw outcome and separate commitment resolution with the supporting observation IDs. The lower-level `CheckAppend(ops, finalKeys)` remains available when the caller supplies a trusted final state.

`calve snapshot` creates a fresh table, writes real empty v2 Avro manifest lists,
and records concurrent guarded snapshot commits for `calve check-table`.
It checks metadata publication; it does not write or validate data files.
The [snapshot workload guide](docs/single-table-checker.md#live-snapshot-workload-65)
includes the Compose runner for all five catalogs and its storage configuration.
Unknown commits may apply after their client timeout; absent effects remain
inconclusive. Nessie uses an explicit current-snapshot model because it returns
only the current snapshot; other catalogs use the default retained-history model.
See the [initial live results](docs/snapshot-workload-results.md).

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
Nessie and Polaris passed the initial 40-transaction runs. Gravitino has no
transaction endpoint, so the workload does not apply to it.

## Sweeps

`scripts/sweep.sh RUNS OUTPUT` runs every workload on every Compose catalog
`RUNS` times into a new or empty `OUTPUT`. Without faults it saves each history
as `OUTPUT/<catalog>/<workload>/<run>.jsonl`. With `FAULTS` (space-separated
kinds) each workload runs under one-kind fault plans of `DURATION` (default
60s), seeded by the run number, and each run directory lands at
`OUTPUT/<catalog>/<workload>/<kind>/<run>/`. Process faults (kill, pause,
restart) skip the reference fixture. Exit statuses go to `OUTPUT/exits.tsv`; a
run's exit status never stops the sweep. `CATALOGS` and `WORKLOADS` narrow the
matrix. It ends with `calve results OUTPUT`, which re-checks every history and
run directory offline (run directories as `calve check` does, load coverage
included) and prints one row per catalog, workload and fault; `--json` prints
the same rows as JSON. Runs that saved nothing count as errors. Each row counts
the commit requests its checked runs sent, whatever their outcome, and gives
its anomalies per 1,000 of them.

```sh
scripts/sweep.sh 5 results/baseline
FAULTS="latency reset partition kill pause restart" scripts/sweep.sh 1 results/faults
go run ./cmd/calve results results/faults
```

The `sweep` workflow runs it on GitHub's Linux runners, one job per catalog:
dispatch it with the number of runs and, for a fault sweep, the fault kinds and
duration. `catalogs` narrows it to some catalogs, which costs fewer runner
minutes; empty means all five. Each catalog's evidence is uploaded as `sweep-<catalog>` and kept for 30
days; the results tables go to the job summary and, as Markdown and JSON, to
the `sweep-results` artifact.

## Seeded fault plans

```sh
go run ./cmd/calve run --seed 42 --plan-only \
  --targets reference,lakekeeper,nessie --plan fault-plan-42.json
```

The same seed, normalized configuration, and scheduler version produce the same complete plan. Targets and fault kinds are sorted before selection, with a private RNG independent of workload randomness. All JSON durations are nanoseconds. Each fault has an ID, target, kind, parameters, start offset, duration, and healing action. V1 schedules one fault at a time and reserves time for verified healing. The seed reproduces the planned schedule; it cannot reproduce thread timing or a failure.

With `--plan-only` it saves the plan and reports planning mode; it does not run a workload or print a correctness verdict. Choose another output path for a subsequent plan: existing artifacts are not overwritten. `calve run -h` lists timing options. See [the scheduler and controller contract](docs/fault-scheduler.md) for the library executor, cleanup behavior, and integration work remaining.

For network faults, Toxiproxy (in `docker-compose.yml`) proxies each catalog on the same paths: reference `:28181`, Lakekeeper `:28182`, Nessie `:28183`, Polaris `:28184`, Gravitino `:28185`, with its API on `:8474`. `fault.Toxiproxy` adds and removes latency, reset and partition toxics there; point `--catalog` at a proxy port to run a workload through them.

## Fault campaigns

```sh
go run ./cmd/calve run --seed 7 --targets reference --catalog http://localhost:28181
go run ./cmd/calve run --seed 7 --targets lakekeeper --catalog http://localhost:28182/catalog --warehouse calve
```

Without `--plan-only`, `calve run` sets up a fresh table, executes the plan while `--clients` append every `--interval`, heals every fault, waits for the catalog to answer (`--ready-timeout`), then does the final read, read-back and append check. Exit codes match `append`: 0 valid, 3 invalid, 4 inconclusive, 1 when a fault could not be applied or healed. Network faults need `--catalog` on the target's proxy port; `calve run` refuses a direct URL. `--proxy` and `--container` override the docker-compose names. Each run saves a new directory (`--out-dir`, default `runs/<run ID>`) with `plan.json`, `faults.jsonl` (controller actions), `history.jsonl` (workload HTTP evidence) and `result.json` (`calve-run-v4`: the verdict, load coverage and the checker's complete result). `calve check <dir>` re-checks it without a catalog: the plan must match its seed, the fault history must record that plan and every fault applied and healed, the workload history must name both, and the result recomputed with the workload's checker must match `result.json` in full. Directories saved as `calve-run-v1`, `calve-run-v2` or `calve-run-v3` still check as they were written. A run that ended in an error stays an error. It returns the same exit codes as the run.

`--workload schema`, `maintenance`, `snapshot` or `atomic` runs that workload under the plan instead: it loads from the plan's start to its horizon, a failed guard read or reload is retried rather than ending the run, and the final read follows healing. Atomic clients, which send transactions without a read before each, wait for the catalog to answer again after a transaction gets no response, so an outage does not fill the history with doomed unknown commits; `--tables`, `--readers` and `--read-interval` size it. Maintenance and snapshot write manifest lists to the catalog's storage, so they need `--s3-endpoint` (Silo is published on `http://localhost:19000`), `--s3-region local-01` and `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`; the reference fixture keeps files in its own container, so their campaigns are not run from the host there. On Nessie use `--model calve-maintenance-current-v1` or `--model calve-snapshot-current-v1`. For every workload the result records load coverage, the commits in flight during each applied fault; a fault that met no commit, counted from the start of its apply, makes an otherwise valid run inconclusive, naming the fault. `calve check` recomputes the coverage too. See [fault campaigns](docs/fault-campaigns.md).

For process faults, `fault.Docker` kills (SIGKILL), pauses or stops (SIGTERM with a grace period) a catalog container through the docker CLI and heals by starting or unpausing the same container, so catalog data survives. The live test (`CALVE_DOCKER_FAULTS=1 go test -run DockerLive ./internal/fault`) checks that the reference fixture, Lakekeeper and Nessie keep their tables across each fault; Polaris and Gravitino are not in it yet.

## Shrinking a failing run

```console
$ calve shrink --attempts 3 -- --workload skew --targets reference \
    --catalog http://localhost:28181 --seed 1 --faults latency,reset --duration 20s
attempt 1: original: invalid (write-skew ×79), reproduced
attempt 2: --duration 18s: invalid (write-skew ×72), reproduced
attempt 3: --duration 18s --faults latency: invalid (write-skew ×72), reproduced
shrunk in 3 attempts: calve run --workload skew ... --seed 1 --duration 18s --faults latency
```

`calve shrink` takes the flags of an invalid `calve run` after `--` and reruns
it with smaller settings. It shortens the plan, which means fewer faults, down
to the shortest plan that still fits one fault. It tries one fault kind at a
time and halves the clients, tables, readers or per-client appends, whichever
the workload has. A smaller run counts as reproducing when one of its
`--attempts` runs (default 3) is invalid with an anomaly kind the original
showed, since failures under concurrency do not repeat every time. Each
setting is halved until that stops reproducing, so the result is small but not
necessarily minimal.

The seed stays fixed and every plan is regenerated from it, so each attempt is
an ordinary run directory that `calve check` accepts. They all land under
`--out-dir` (default `shrinks/<shrink ID>`) with their logs, alongside
`shrink.jsonl`, which holds every attempt, and `shrink.json`, which holds the
result and the command that reproduces it. A `--token` value is redacted from
both; prefer `CALVE_TOKEN`.

## Reporting findings

Before filing anything upstream: reproduce it from a seed, shrink it to a standalone script, confirm the spec actually promises the violated behavior, and search existing issues.

## License

Apache-2.0

Done by [aiskakov28](https://github.com/aiskakov28) and [aldusialn](https://github.com/aldusialn).
