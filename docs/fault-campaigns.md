# Fault campaigns

`calve run` runs one workload against one catalog while a seeded fault plan
breaks the network or the catalog process. Then it heals every fault, waits for
the catalog to answer, reads the final state and checks the history. All six
workloads can run this way.

```sh
calve run --workload schema --seed 7 --targets lakekeeper \
  --catalog http://localhost:28182/catalog --warehouse calve
```

`--faults` picks the fault kinds (all six by default), `--duration` the plan's
length (default 60s), and `calve run -h` lists the timing options. Exit codes
match the workload commands: 0 valid, 3 invalid, 4 inconclusive, and 1 when a
fault could not be applied or healed.

## How a run is ordered

Each workload is split into three phases around the plan:

| Phase | When | What happens |
| --- | --- | --- |
| prepare | before the plan starts | setup, baseline reads, the run record |
| load | from the plan's start until its end | rounds or attempts keep starting |
| finish | after every fault is healed and the catalog answers | final reads and read-back |

Under a plan, the load phase runs until the plan ends, not for a fixed number
of rounds, so no workload can finish before the faults do. A cap still bounds
the history size, sized so the workload can't reach it before the plan ends.

Skew sets up all of its rounds' tables and baselines before the plan, so the
load phase only sends transactions, and its two controls run after healing.
Rounds the plan cut off are reported as not started.

Atomic clients send transactions without reading first. After a transaction
gets no response, they wait until the catalog answers again, using an
unrecorded readiness probe, so an outage doesn't fill the history with doomed
unknown commits.

## Reads during faults

A failed read during the load phase is recorded and tried again; it doesn't end
the run. Reads change nothing, so retrying them can't create an effect. Commits
are still sent once and never retried. A round whose guard read never succeeds
before the plan ends is not started. Every attempt is its own recorded
operation, so the checker sees all of them.

## Load coverage

A fault that met no traffic tested nothing. For each applied fault the result
records how many commits were in flight during its window, from the start of
its apply to the start of its heal, so a commit caught mid-flight by a kill
counts. A fault that met no commit makes an otherwise valid run inconclusive,
and the result names it. One commit per window is enough today.

## Run directories

Each run writes a new directory (`--out-dir`, default `runs/<run ID>`):

| File | Contents |
| --- | --- |
| `plan.json` | the complete fault plan and the seed that generates it |
| `faults.jsonl` | every controller action and what it observed |
| `history.jsonl` | the workload's HTTP evidence, linked to the plan and fault log |
| `result.json` | the verdict, load coverage and the checker's full result |
| `provenance.json` | the Calve version and commit, Go version and platform, and the catalog's image tag and image ID |

`calve check <dir>` re-checks a run without a catalog. The plan must
regenerate from its seed, the fault log must show every fault applied and
healed, the workload history must name both, and the recomputed checker result
must match `result.json` exactly. A run that ended in an error stays an error.
`provenance.json` describes the run and is not part of the check. The image is
looked up with `docker compose ps` and `docker inspect`; when that fails, the
reason is recorded instead and the run goes on. Directories from before it was
added have none.

`result.json` is versioned. `calve-run-v4` is current. Older directories are
checked as they were written: `calve-run-v1` holds append counters only,
`calve-run-v2` has no load coverage, and `calve-run-v3` measured each fault
window from the end of its apply.

## Routing

Toxiproxy proxies each catalog on the same path:

| Catalog | Direct | Through Toxiproxy |
| --- | --- | --- |
| Reference fixture | `:8181` | `:28181` |
| Lakekeeper | `:8182` | `:28182` |
| Nessie | `:8183` | `:28183` |
| Polaris | `:8184` | `:28184` |
| Gravitino | `:8185` | `:28185` |

For network faults, `calve run` refuses a `--catalog` URL that bypasses the
proxy. For process faults it finds the catalog's container with `docker compose
ps`, so run it from the Compose project directory; the project name doesn't
matter. `--proxy` and `--container` override both.

Snapshot and maintenance write manifest lists to the catalog's object store,
so they need `--s3-endpoint` (Silo is published at `http://localhost:19000`),
`--s3-region local-01` and `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
Manifest uploads to Silo are not faulted; the plan only targets the catalog.
On Nessie, pass `--model calve-maintenance-current-v1` or
`--model calve-snapshot-current-v1`, because it returns only the current
snapshot.

The reference fixture keeps its files inside its own container, so its
snapshot and maintenance campaigns run inside the fixture, with catalog traffic
through `toxiproxy:28181`. The sweep script handles that.

## Choices for the sweep

- **One fault kind per plan**, so any finding can be pinned on one kind of
  fault. Mixed plans are for follow-up on specific leads.
- **60 seconds per run**, which is roughly 10 to 15 faults.
- **No process faults on the reference fixture.** After a restart it keeps its
  SQLite file but fails nearly every write, so kill, pause and restart there
  would only produce inconclusive runs. It gets the three network faults.

`scripts/sweep.sh` with `FAULTS` set runs these plans across every catalog and
workload, seeding each plan with the run number.

## What is tested in CI

CI checks that Toxiproxy faults take effect and heal, and that process faults
keep catalog data on the reference fixture, Lakekeeper and Nessie
(`CALVE_DOCKER_FAULTS=1 go test -run DockerLive ./internal/fault`). Polaris
and Gravitino are not in that test yet. CI also runs short seeded campaigns:
append, maintenance and skew on Lakekeeper, and snapshot, atomic and schema on
Polaris.
