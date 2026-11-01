# Seeded fault scheduler

The planner, executor and both controllers live in `internal/fault`:
`fault.Toxiproxy` for latency, reset and partition, and `fault.Docker` for
kill, pause and restart. [Fault campaigns](fault-campaigns.md) explains how
`calve run` uses them with a workload. This page covers plans, the controller
contract and the executor.

To see a plan without running anything:

```sh
calve run --seed 42 --plan-only --targets reference,lakekeeper,nessie \
  --plan fault-plan-42.json
```

## Plans

`fault.Generate(seed, config)` returns a `Plan` with the scheduler version
(`calve-faults-v1-pcg`), the seed, the normalized configuration and the full
ordered list of events. Each event has an ID, target, kind, parameters, start
offset, duration and healing action.

The same seed, configuration and scheduler version always give the same plan.
Target and kind lists are sorted before use, and the planner draws from its own
[PCG generator](https://pkg.go.dev/math/rand/v2#NewPCG), so wall-clock time,
workload randomness and input order can't change it. A golden test catches
accidental changes to the algorithm; changing the rules means a new version.

A seed reproduces the schedule, not the thread timing, controller latency or
which operations fail.

Version 1 runs one fault at a time across all targets. For each fault it draws
a healthy gap and a fault window, then the target, kind and, where it matters,
the direction. The next fault starts after the previous heal, its control
budget and the next healthy gap. Late events keep their planned offsets; the
scheduler never redraws them.

| Setting | Default |
| --- | --- |
| plan length | 60s, after a 5s healthy warmup |
| healthy gap between faults | 2–5s |
| fault window | 1–3s |
| budget per control action | 5s |
| total cleanup budget | 30s |
| latency fault delay | 250ms |
| stop grace for restart | 1s |

Durations in JSON are nanoseconds, in fields ending in `_ns`. A configuration
that can't fit even one fault plus its healing budget is rejected, and plans
are capped at 10,000 events.

`fault.Save` writes a new plan file and syncs it before execution starts; it
never overwrites an existing file. `fault.Read` rejects unknown fields,
trailing data, unsupported versions, and any plan that doesn't regenerate from
its own seed and configuration. Plan files contain no timestamps, so identical
plans are byte-for-byte identical.

## Controllers

```go
type Controller interface {
    Apply(context.Context, fault.Injection) error
    Heal(context.Context, fault.Injection) error
    Observe(context.Context, fault.Injection) (fault.State, error)
}
```

`ExecuteOptions.Controllers` maps each fault kind to a controller. An
`Injection` carries the run ID and the planned event, and `ObjectName()` gives
a name unique to the run and fault (`runID-faultID`), so repeated plans don't
collide.

| Kind | Apply | Heal | Fixed parameters |
| --- | --- | --- | --- |
| latency | add a latency toxic | remove the run's toxic | chosen direction, configured delay, no jitter |
| reset | add `reset_peer` | remove the run's toxic | chosen direction |
| partition | add `timeout: 0` toxics | remove both | both directions |
| kill | `SIGKILL` the container | start the same container | storage kept |
| pause | pause the container | unpause it | |
| restart | stop with `SIGTERM` and a grace period | start the same container | storage kept |

All toxics use toxicity 1. The rules every controller follows:

- **Verify, don't trust.** A successful `Apply` call isn't proof. `Observe` has
  to confirm the fault is active with the requested parameters, and later that
  it's healed. "Healthy" means the fault is gone, not that the catalog is ready
  or that earlier commits have finished.
- **Heal safely.** `Heal` is idempotent, touches only this run's changes, and
  handles a fault that was planned but never applied. If an apply's result was
  uncertain, `Heal` must make sure a late apply can't bring the fault back.
  When it can't establish that within its budget, it returns an error.
- **Honor deadlines** and return errors without credentials.

`fault.Docker` refuses a container with an automatic restart policy, since that
would end an outage early.

## Executor

`fault.Execute(ctx, plan, options)` checks the plan and the controller set,
creates a new history file, and saves the plan before touching any backend or
starting load. It inspects every target first, then records `workload-start`
and calls `OnStart(loadCtx)`, which should start background writes and return.
The caller finishes table setup and baseline reads before this point.

Timing is relative to the workload start, using monotonic deadlines. Each apply
is followed by an `Observe` that must report `Active`, and each heal by one that
must report `Healthy`. Any error, a cancelled context or a missed healing
deadline stops new faults. A late action is recorded against its planned
offset, and a fault whose whole window and healing budget have passed is
skipped.

Controller actions are recorded as process -2, `f: "fault"`, in stages
`preflight`, `execution` and `cleanup`. Each record has the fault, the action,
its planned and actual offsets, and the state observed. Only a successful call
with the expected state is `ok`; anything uncertain is `info`. These outcomes
are about the controller, not catalog commits.

### Cleanup

On every exit after the plan is saved, the executor cancels `loadCtx` and runs
cleanup under a [context that ignores cancellation](https://pkg.go.dev/context#WithoutCancel),
with its own total budget and per-action limits:

1. Faults that were attempted but whose heal wasn't verified, newest first. An
   apply counts as attempted before the call, because a failed call can still
   leave the fault active.
2. Every remaining planned fault, in reverse order.

So a broken backend for a future fault can't use up the budget before an
active fault gets its repair attempt. Every planned fault is visited once, and
an expired budget is recorded as a failed cleanup. Execution, cleanup and
history-save errors are all returned together.

`ExecutionResult.Completed` means only that the schedule and cleanup finished.
The caller still joins its load, waits for the catalog, reads final state and
saves the complete history.

## Tests

Unit tests cover deterministic plans from reordered input, independent random
state, the golden plan, timing bounds, strict file round trips and refusing to
overwrite. A virtual clock tests sequencing without waiting through a full
campaign. Fake controllers check that the plan is saved before any backend
call, that heals are verified before the next fault, and how the executor
handles uncertain applies, cancellation, blocking calls, failed or late heals
and an exhausted cleanup budget.

Live tests run against the Compose stack in CI: `TestToxiproxyLiveEffects`
checks each toxic's effect and healing, and `TestDockerLiveEffectsAndSurvival`
checks that the reference fixture, Lakekeeper and Nessie keep their tables
through kill, pause and restart.
