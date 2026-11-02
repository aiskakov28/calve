# Calve

Jepsen-style correctness testing for Apache Iceberg REST catalogs.

Calve sends concurrent commits to an Iceberg REST catalog, sometimes while it
breaks the network or kills the catalog process. It records the raw HTTP
evidence of every request, then replays it offline to check conditional
commits, multi-table atomicity, write skew, snapshot linearizability and
durability, without ever treating an unknown outcome as a known one.

It runs against five open-source catalogs in Docker Compose: the Iceberg
reference fixture, Apache Polaris, Lakekeeper, Nessie and Apache Gravitino.

## What it found

| Catalog | Finding | Status |
| --- | --- | --- |
| Iceberg reference fixture 1.10.1 | Two conflicting multi-table transactions both commit | Known: [apache/iceberg#18552](https://github.com/apache/iceberg/issues/18552) |
| Iceberg reference fixture 1.10.1 | Two concurrent transactions on one table are both acknowledged, but one change is lost | Filed: [apache/iceberg#18758](https://github.com/apache/iceberg/issues/18758) ([triage](docs/triage-reference-atomic.md)) |
| Apache Hive 4.2.0, 4.2.1 REST catalog | The same lost change: Hive ships the fixture's transaction code | Reproduced; reported in [apache/iceberg#18758](https://github.com/apache/iceberg/issues/18758) ([triage](docs/triage-reference-atomic.md#who-is-affected)) |
| Apache Polaris 1.8.0 | The same multi-table write skew | Known: [apache/polaris#5807](https://github.com/apache/polaris/issues/5807), confirmed by us on Postgres ([triage](docs/triage-polaris-skew.md)) |
| Apache Polaris 1.8.0 | A 1.1.0 refactor stopped the server from retrying most commit conflicts, so concurrent commits mostly get a 409 | Filed: [apache/polaris#5907](https://github.com/apache/polaris/issues/5907), fix in [apache/polaris#5921](https://github.com/apache/polaris/pull/5921) ([triage](docs/triage-409-without-requirements.md)) |
| Lakekeeper v0.14.2 | Concurrent commits to one table deadlock in Postgres and time out | Filed: [lakekeeper/lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201), fixed in [lakekeeper/lakekeeper#2219](https://github.com/lakekeeper/lakekeeper/pull/2219) (not yet merged) ([triage](docs/triage-409-without-requirements.md)) |
| Lakekeeper v0.14.2 | Concurrent schema changes: the losing commit gets HTTP 503 instead of 409 | Filed: [lakekeeper/lakekeeper#2216](https://github.com/lakekeeper/lakekeeper/issues/2216), fixed in [lakekeeper/lakekeeper#2219](https://github.com/lakekeeper/lakekeeper/pull/2219) (not yet merged) ([triage](docs/triage-lakekeeper-schema-cas.md)) |
| Nessie 0.109.0 | Some concurrent commits get HTTP 500 because a request context is no longer active | Filed: [projectnessie/nessie#13271](https://github.com/projectnessie/nessie/issues/13271) |
| Iceberg `JdbcCatalog` (reference fixture, Gravitino 1.3.1) | Concurrent creates of one namespace get HTTP 500 instead of 409 | Reproduced, not filed ([triage](docs/triage-jdbc-namespace-create.md)) |
| Apache Gravitino 1.3.1 | No anomaly in any workload it supports | |

Calve treats a write-skew result as a lead, not a confirmed bug, because the
REST spec does not say how multi-table requirements are isolated. Each triage
note gives the evidence, the part of the spec or catalog code involved, and how
to reproduce it.

## Results

Each run gets one of three verdicts:

- **Valid:** no violation was found in that recorded run, under the selected
  checker and model.
- **Invalid:** the recorded run breaks the checker's rule. Findings that depend
  on a model, such as write skew, still need triage before anyone calls them a
  catalog bug.
- **Inconclusive:** the evidence can't decide, usually because some commits
  have unknown outcomes or a needed read failed.

Every applicable workload on every catalog, on GitHub's Linux runners. Each
cell counts runs by verdict and names any anomaly with its rate per 1,000
commit requests. The rates make these runs comparable with each other; they
are not estimates of how often a catalog fails in production. Gravitino has no
multi-table transaction endpoint, so skew and atomic do not apply to it.
The raw evidence behind both tables, with checksums, is attached to the
[v0.1.0 release](https://github.com/aiskakov28/calve/releases/tag/v0.1.0).

Without faults, 5 runs per cell ([sweep run 41377550641](https://github.com/aiskakov28/calve/actions/runs/41377550641), 2026-11-02, Calve `c23f94e`):

| Workload | Reference | Lakekeeper | Nessie | Polaris | Gravitino |
| --- | --- | --- | --- | --- | --- |
| append | 5 valid | 5 inconclusive | 5 inconclusive | 5 valid | 5 valid |
| atomic | **5 invalid**<br>lost, non-atomic-visibility · 715 per 1k | 5 valid | 1 valid, 4 inconclusive | 5 valid | not supported |
| maintenance | 5 valid | 4 valid, 1 inconclusive | 5 inconclusive | 5 valid | 5 valid |
| schema | 5 valid | 5 inconclusive | 5 valid | 5 valid | 5 valid |
| skew | **5 invalid**<br>write-skew · 391.7 per 1k | 5 valid | 5 valid | **5 invalid**<br>write-skew · 350 per 1k | not supported |
| snapshot | 5 valid | 5 valid | 5 valid | 5 valid | 5 valid |

With faults, a single-seed sweep: one 60-second run per fault kind (latency,
reset, partition, kill, pause and restart), each plan seeded with 1. The
reference fixture only gets the three network faults, because it cannot write
after a restart ([sweep run 41377560231](https://github.com/aiskakov28/calve/actions/runs/41377560231), 2026-11-02, Calve `c23f94e`):

| Workload | Reference | Lakekeeper | Nessie | Polaris | Gravitino |
| --- | --- | --- | --- | --- | --- |
| append | 1 valid, 2 inconclusive | 1 valid, 5 inconclusive | 6 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive |
| atomic | **3 invalid**<br>lost, non-atomic-visibility · 570.9 per 1k | 6 inconclusive | 6 inconclusive | 4 inconclusive, 2 errors | not supported |
| maintenance | 2 valid, 1 inconclusive | 3 valid, 3 inconclusive | 6 inconclusive | 4 inconclusive, 2 errors | 3 valid, 3 inconclusive |
| schema | 1 valid, 2 inconclusive | 6 inconclusive | 2 valid, 4 inconclusive | 3 inconclusive, 3 errors | 2 valid, 4 inconclusive |
| skew | **3 invalid**<br>write-skew · 486.9 per 1k | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | **6 invalid**<br>write-skew · 441 per 1k | not supported |
| snapshot | 1 valid, 2 inconclusive | 2 valid, 4 inconclusive | 2 valid, 4 inconclusive | 3 inconclusive, 3 errors | 2 valid, 4 inconclusive |

Faults turned up nothing the fault-free runs had not, and Gravitino showed no
anomaly. The 12 invalid runs are the same as in the two earlier fault sweeps
([run 41246628323](https://github.com/aiskakov28/calve/actions/runs/41246628323) and [run 41222078924](https://github.com/aiskakov28/calve/actions/runs/41222078924); in both, every catalog job passed and only the final summary job failed).

**Polaris errors.** 10 Polaris runs ended in an error before their first fault:
the setup `create table` got no answer within the sweep's 5-second request
timeout. Over the job, Polaris's first `create table` in each run slowed from
about 10 ms to 4 or 5 seconds as tables piled up in the shared namespace. An
error is not a verdict, so those runs say nothing about Polaris under faults.

**Why so many runs are inconclusive.** When a commit times out or gets a 5xx,
nobody knows whether it applied. Calve never treats a missing effect as proof
of rejection, so such a run can't be called valid. Under faults that's
expected. Without faults it points at the catalog:

- **Lakekeeper append:** the deadlock above makes commits time out.
- **Lakekeeper schema:** losing schema changes get HTTP 503 from a duplicate-key
  database error instead of a 409 ([lakekeeper/lakekeeper#2216](https://github.com/lakekeeper/lakekeeper/issues/2216),
  [details](docs/schema-workload.md#lakekeeper-losing-schema-commits-answer-503)).
- **Nessie append and atomic:** some commits get HTTP 500 from an inactive
  request context ([projectnessie/nessie#13271](https://github.com/projectnessie/nessie/issues/13271)).
- **Nessie maintenance:** Nessie returns only the current snapshot, so there is
  nothing to expire and nothing to check.
- **Lakekeeper maintenance (1 run):** the run's only expiration lost a race with an
  append and got a clean 409, so no expiration was acknowledged and there was
  nothing to check. This one is not a catalog problem.

## Quick start

You need Go 1.24 or later, Docker and `jq`.

```sh
scripts/up.sh
```

This starts all five catalogs, runs their setup jobs, builds `bin/calve` and
smoke-tests each catalog. It's safe to run again.

Then reproduce apache/iceberg#18552 on the reference catalog, and check that
Lakekeeper doesn't have it:

```console
$ bin/calve skew --catalog http://localhost:8181 --rounds 10 --out history-skew-reference.jsonl
skew on http://localhost:8181: 12 rounds, 22 ok, 2 rejected, 0 unknown (0 resolved as committed)
...
INVALID: 10 write-skew
write-skew is an isolation lead under the serializable model, not a confirmed catalog bug
$ bin/calve skew --catalog http://localhost:8182/catalog --warehouse calve --rounds 10 --out history-skew-lakekeeper.jsonl
skew on http://localhost:8182/catalog: 12 rounds, 12 ok, 12 rejected, 0 unknown (0 resolved as committed)
VALID: at most one transaction committed in every round; stale controls were rejected
```

Each round creates two fresh tables, A and B. Two clients send multi-table
transactions at the same moment. One changes A's schema and asserts that B's
schema hasn't changed; the other does the opposite. Whichever commits second is
asserting something that's no longer true, so a catalog that runs each
transaction as one serializable step commits at most one of them.

The last two rounds are controls: the same pair sent one after the other. The
catalog must reject the second, which shows it checks the requirement at all.

The reference catalog committed both transactions in all ten concurrent rounds
but passed both controls. It checks the requirement, just not against a
transaction committing at the same time. Lakekeeper committed exactly one per
round.

Every run saves its history to a new file (`--out`), so the verdict can be
checked again later without the catalog. Calve never overwrites a history, so
each run needs its own file.

### Catalog endpoints

| Catalog | URL | Extra flags |
| --- | --- | --- |
| Reference fixture 1.10.1 | `http://localhost:8181` | |
| Lakekeeper v0.14.2 | `http://localhost:8182/catalog` | `--warehouse calve` |
| Nessie 0.109.0 | `http://localhost:8183/iceberg` | |
| Polaris 1.8.0 | `http://localhost:8184/api/catalog` | `--warehouse calve --token "$POLARIS_TOKEN"` |
| Gravitino 1.3.1 | `http://localhost:8185/iceberg` | |

Polaris needs an OAuth token for its root principal. The credentials are for
local testing only:

```sh
export POLARIS_TOKEN=$(curl -s -X POST http://localhost:8184/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=calve-secret \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
```

Tokens last an hour and survive Polaris restarts.

Lakekeeper, Nessie, Polaris and Gravitino store metadata in Postgres and table
files in Silo, an S3-compatible store. Gravitino runs its standalone Iceberg
REST server on Iceberg's JDBC catalog; its image is extended with the Postgres
driver. Calve only tests catalogs you run yourself. It does not test managed
cloud catalogs.

## Safety

Run Calve only against test catalogs you control. Fault campaigns deliberately
cut network traffic and kill, pause and stop catalog containers.

Ctrl+C or SIGTERM stops `calve run` (and `calve shrink`) cleanly: it heals
every fault the plan may have applied, saves the run directory with an `error`
verdict, and exits 1. A second Ctrl+C exits at once without that cleanup. If
Calve exits that way or is killed (`kill -9`, a crashed runner), a fault can stay
active. To clean up by hand:

```sh
docker compose restart toxiproxy    # drops every toxic; proxies come back from its config
docker compose unpause <service>    # if a catalog was paused
docker compose start <service>      # if a catalog was killed or stopped
```

`docker compose ps` shows which catalogs are paused or stopped.

Saved histories redact configured credentials, but they hold raw catalog
responses, which can include metadata or anything else the server sent back.
Calve saves them readable only by you and never overwrites an earlier one, so
a rerun needs a new `--out`. Look through them before sharing.

## How it works

1. **Run a workload.** Each workload creates fresh tables and sends concurrent
   commits designed so that a correct catalog can only produce certain results.
   Every request is sent exactly once, with no client retries.
2. **Record everything.** The raw HTTP request and response of every call goes
   into a JSON Lines history, with credentials redacted.
3. **Check offline.** A checker replays the history and returns one of three
   verdicts:
   - `VALID` (exit 0): the invariant held in every round it could check.
   - `INVALID` (exit 3): at least one anomaly, with the operations that prove it.
   - `INCONCLUSIVE` (exit 4): the evidence can't decide, usually because a
     commit's outcome is unknown.

   Errors exit 1.

A 200 or 204 means the commit applied. A 409 or another rejection the spec
documents (400, 401, 403, 404, 406, 419) means it didn't. Anything else, such as
a timeout, reset, 5xx or a status the spec doesn't document like 202 or 429,
means its outcome is unknown. An unknown commit counts as committed only if a later
read shows its effect; if the effect is missing, the commit stays unknown,
because it could still apply later.

Each checker is tested against an in-memory fake catalog with planted bugs. It
has to catch every planted bug, and pass a correct fake without false alarms,
before it is used on a real catalog ([controls](docs/checker-controls.md)).

## Workloads

| Command | What it does | What counts as an anomaly |
| --- | --- | --- |
| `calve append` | Concurrent property commits, each adding its own key | An acknowledged key is missing; a rejected key is present |
| `calve skew` | Conflicting two-table transactions, plus sequential controls | Both transactions commit; a stale requirement is accepted |
| `calve atomic` | Transactions that update every table, with concurrent readers | Lost or partial effects; effects of rejected transactions |
| `calve snapshot` | Guarded snapshot commits with real empty manifest lists | No legal sequential order exists (checked with [Porcupine](https://github.com/anishathalye/porcupine)) |
| `calve schema` | Guarded schema changes racing in rounds | More than one winner per round; lost or wrongly rejected changes |
| `calve maintenance` | Snapshot expiration racing a chain of appends | A committed snapshot disappears; `main` dangles or goes backwards |

Saved evidence is re-checked without a catalog:

- `calve check <dir>` re-checks a fault-campaign run directory, and
  `calve check <file>` an atomic, schema or maintenance history.
- `calve check-table <file>` re-checks a snapshot history.
- `calve results <dir>` re-checks every history in a sweep, append and skew
  included.

`calve version` prints the version and commit Calve was built from. Each run
directory records the same in `provenance.json`, with the catalog image it ran
against.

Run `calve <command> -h` for flags.

Workloads commit properties, schemas and snapshots whose manifest lists are
real but empty. They never write data files, so Calve says nothing about
data-file correctness.

## Fault campaigns

`calve run` runs one workload while it applies a seeded fault plan, then heals
every fault, waits for the catalog to answer and checks the result:

```sh
go run ./cmd/calve run --seed 7 --targets lakekeeper \
  --catalog http://localhost:28182/catalog --warehouse calve
```

- **Network faults** (latency, reset, partition) go through Toxiproxy, which
  proxies each catalog on the same path at ports `28181` to `28185`, in the
  order of the endpoint table above.
- **Process faults** (kill, pause, restart) act on the catalog's container
  through the Docker CLI and keep its data. Calve finds the container with
  `docker compose ps`, so run `calve run` from the repository directory.

The same seed and settings always produce the same plan, though not the same
thread timing. A run directory holds the plan, the fault log, the workload
history and the result, and `calve check <dir>` recomputes the verdict from
them. A fault that met no in-flight commit makes an otherwise valid run
inconclusive. Use `--workload` to pick something other than append. See
[fault campaigns](docs/fault-campaigns.md) for routing and storage settings.

To make a failing run smaller, pass its flags to `calve shrink`:

```sh
calve shrink --attempts 3 -- --workload skew --targets reference \
  --catalog http://localhost:28181 --seed 1 --faults latency,reset --duration 20s
```

It shortens the plan, tries one fault kind at a time and halves the clients,
tables or appends, keeping each change while some attempt still fails with the
same anomaly. The result is small, but not necessarily minimal.

## Sweeps

`scripts/sweep.sh` runs every workload on every catalog and ends with a results
table like the ones above:

```sh
scripts/sweep.sh 5 results/baseline
FAULTS="latency reset partition kill pause restart" scripts/sweep.sh 1 results/faults
go run ./cmd/calve results results/faults
```

`CATALOGS` and `WORKLOADS` narrow the matrix, and `DURATION` sets each fault
plan's length (default 60s). The `sweep` GitHub workflow does the same on
GitHub's runners, one job per catalog, and uploads the evidence.

## Use in CI

A catalog project can run Calve on every push with this repository's GitHub
Action. Start the catalog in an earlier step, then:

```yaml
- uses: aiskakov28/calve@v0.1.0
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

It runs the append, skew, atomic and schema workloads, which only need the REST
API, and writes a verdict table to the job summary. It fails the job on an
anomaly or an error; with `fail-on: inconclusive` it also fails when a run
can't be decided. `workloads` narrows the set and `out-dir` moves the output.
Locally, `CATALOG=<url> scripts/check-catalog.sh` runs the same check.

## Documentation

- [Design](docs/design.md): what the REST spec promises, how responses map to
  outcomes, and the history format.
- [Unknown outcomes](docs/unknown-outcomes.md): how timeouts and 5xx are
  handled and resolved.
- [Checker controls](docs/checker-controls.md): the planted bugs each checker
  must catch.
- Workloads: [snapshot](docs/single-table-checker.md),
  [multi-table](docs/multi-table-checker.md), [schema](docs/schema-workload.md),
  [maintenance](docs/maintenance-workload.md).
- Faults: [campaigns](docs/fault-campaigns.md) and the
  [seeded scheduler](docs/fault-scheduler.md).
- [Background on Elle and Porcupine](docs/checker-background.md).
- Triage notes: [reference atomic](docs/triage-reference-atomic.md),
  [Polaris skew](docs/triage-polaris-skew.md),
  [409s without requirements](docs/triage-409-without-requirements.md),
  [namespace creates](docs/triage-jdbc-namespace-create.md),
  [Lakekeeper schema CAS](docs/triage-lakekeeper-schema-cas.md).
- [Nessie schema IDs](docs/triage-nessie-schema-id.md): an interoperability
  difference Calve works around by always sending schema IDs, not a finding.

## Repository layout

```
cmd/calve/            command-line tool
internal/rest/        raw Iceberg REST client: no retries, records every response
internal/history/     JSON Lines history records
internal/workload/    workloads
internal/checker/     checkers
internal/fault/       fault planner, Toxiproxy and Docker controllers
internal/fakecatalog/ in-memory catalog with planted bugs, for testing checkers
internal/storage/     manifest-list writer and S3 uploads
scripts/              Compose setup, sweeps and standalone repro scripts
docs/                 design, workload and triage notes
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues in Calve itself go
through [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).

Built by [aiskakov28](https://github.com/aiskakov28) and [aldusialn](https://github.com/aldusialn).
