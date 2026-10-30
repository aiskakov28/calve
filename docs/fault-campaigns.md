# Workload-neutral fault campaigns (#76): design

Status: design with agreed decisions (below); nothing is implemented yet. It
is the prerequisite for the fault sweep (#23).

## Today

`calve run` executes a seeded fault plan while one workload runs, then heals,
waits for the catalog to answer, re-reads and checks. Only append can do this:

- `campaign.run` takes `workload.AppendConfig` and starts `workload.RunAppend`
  from the executor's `OnStart`.
- `executeAppendWith` owns setup, the baseline read before the plan, and the
  final read after it.
- Only the append run record carries `fault_seed`, `fault_plan` and
  `fault_history`, which tie the history to the plan and fault history.
- `result.json` stores append counters; `calve check <run directory>` requires
  `workload == "append"` and recomputes with `CheckAppendHistory`.

The other five workloads (skew, atomic, snapshot, schema, maintenance) run only
without faults, and two assumptions stop them working under one:

1. **A failed read ends the run.** Schema and snapshot return an error when a
   round's guard load fails, maintenance when the appender's reload does, and
   skew when a round's baseline or control verification read does. Under a
   fault, failed reads are expected. Atomic reads its baseline before the plan
   and its concurrent readers already ignore failed loads, so it is unaffected.
2. **Fixed amounts of work.** They run a set number of rounds or attempts, not
   until the plan's horizon, so a short workload can finish before the first
   fault is applied.

## Goals

- `calve run --workload <name>` for all six workloads, with the existing plans,
  controllers, readiness wait and read-back.
- One run directory layout and one `calve check <run directory>` for all of them.
- Workload code changes stay small: the campaign owns ordering, the workload
  owns its requests and its checker.
- No change to fault-free commands, histories or verdicts.

## Proposed design

### Phases

Each workload is split into the three phases the executor already expects:

| Phase | When | Owner |
| --- | --- | --- |
| prepare | before the plan starts: setup, baseline reads, run record | workload |
| load | from `OnStart` until the plan's horizon | workload, stopped by the campaign |
| finish | after every fault is healed and the catalog answers: final reads | workload |

A workload gets campaign hooks in its config, all nil without faults:

```go
type CampaignHooks struct {
    Link    map[string]any  // fault_seed, fault_plan, fault_history for the run record
    Ready   func()          // prepare is done; returns when the plan has started
    Stop    <-chan struct{} // closed at the plan's horizon: start no new rounds
    Healed  <-chan struct{} // closed after healing and readiness: run finish
}
```

Without hooks a workload behaves exactly as today. With hooks it records `Link`
in its run record, calls `Ready` after its baseline, starts new rounds or
attempts until `Stop` closes, waits for `Healed`, then does its final reads.
The campaign closes `Stop` when the executor returns and `Healed` after
`waitReady`, as `campaign.run` does now.

Under hooks the load is driven by the plan's horizon, not by the workload's
fault-free round or attempt counts, so no workload can finish before the faults
do. A cap stays only to bound history size, and the campaign refuses to start
if the cap could be reached before the horizon at the workload's pace.

### Load coverage

A completed fault schedule does not show that the faults met any load. The
campaign result records, for each applied fault, how many commits were in
flight during its window, from the start of its apply to the start of its heal,
so a commit caught mid-flight by a kill or stop counts. A run where an applied
fault met no commit
is inconclusive, with that fault named, rather than valid. This mirrors the
concurrency coverage the atomic and maintenance checkers already require.

### Reads under faults

During the load phase, a failed read is recorded and **retried**; it no longer
ends the run. Reads change nothing, so retrying them cannot create an effect.
Commits are still sent once and never retried. A round whose guard read never
succeeds before `Stop` is not started. Each retried read is a separate recorded
operation, so checkers see every attempt.

### Checkers

Checkers already treat timeouts and 5xx as unknown, resolve unknowns only from
positive evidence, and ignore failed loads as observations. Under hooks they
also need to:

- accept a run record with a campaign link and a variable number of completed
  rounds or attempts, up to the recorded maximum, instead of exactly that many;
- treat a round cut off by `Stop` as not started, not as missing evidence.

Every checker change ships with fake-catalog tests under a fake fault
controller: a correct catalog stays free of findings, and the existing planted
bugs are still caught when faults are interleaved.

### Run directory and `calve check`

The files stay the same (`plan.json`, `faults.jsonl`, `history.jsonl`,
`result.json`). `result.json` moves to `calve-run-v2`:

```json
{"version": "calve-run-v2", "run_id": "...", "workload": "schema", "target": "lakekeeper",
 "seed": 7, "verdict": "valid", "faults": {...}, "result": { full checker result }}
```

`calve check <run directory>` dispatches on `workload`, recomputes the checker
result from the history and compares the whole `result` object with the saved
one. That extends the anomaly comparison fixed in #74 to every field and every
workload. `calve-run-v1` append directories stay readable. Step 2 added load
coverage as `calve-run-v3`, and `calve-run-v4` opened each fault's window at the
start of its apply. Older directories are checked as they were written:
`calve-run-v2` without coverage, since its fault and workload histories do not
share a clock origin, and `calve-run-v3` with its original window.

### Routing network faults

Host workloads already go through the Toxiproxy ports (`:28181`–`:28184`), and
`checkProxied` refuses a URL that bypasses them. Snapshot and maintenance run
inside the Compose network so their manifests reach catalog storage; under a
campaign they use the proxy's in-network address (for example
`http://toxiproxy:28182/catalog`), and `checkProxied` keeps checking the port.
Manifest writes to Silo are not faulted: the plan targets the catalog.

### Process faults on the reference fixture

A restarted reference fixture keeps its SQLite file and then fails nearly every
write (found while building the sweep, #75). Kill and restart on the reference
fixture would therefore make its runs inconclusive rather than test anything.
Decision: run network faults on all four catalogs, and process faults on
Lakekeeper, Nessie and Polaris only, stated in the results.

## Rollout

Each step is its own PR with fake-catalog tests and a Codex review:

1. Generic campaign, `calve-run-v2`, `calve check` dispatch; port append with
   no change to its verdicts. The hooks and load coverage land in step 2 with
   their first users.
2. Schema under campaigns, with the hooks and load coverage (load coverage
   applies to append too), then maintenance. Maintenance writes manifests
   from the host through Silo's loopback port; the reference fixture's
   maintenance campaign needs an in-network runner, left to step 5.
3. Snapshot.
4. Atomic and skew.
5. Fault mode in `scripts/sweep.sh` and the sweep workflow; the results table's
   fault column names the plan's kinds and seed.

## Decisions

Agreed on 2026-10-30 for the first full fault sweep:

1. **One fault kind per plan**, so every finding is attributable to one kind
   of fault. Mixed plans are left for follow-up on specific leads.
2. **Workloads run until the plan ends** (see hooks and load coverage).
3. **No process faults on the reference fixture**; network faults run on all
   four catalogs.
4. **60 seconds per cell** (the default horizon, roughly 10 to 15 faults).

Still open: whether load coverage should require more than one commit per
fault window. The first implementation requires one.
