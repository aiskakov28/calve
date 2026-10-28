# Seeded fault scheduler (issue #15)

The planner and controller-independent executor are implemented in `internal/fault`. Both controllers exist: `fault.Toxiproxy` for latency, reset and partition (#13) and `fault.Docker` for kill, pause and restart (#14). `calve run --seed N --plan-only` generates a saved plan now; execution without `--plan-only` returns an error until the controllers are wired into it. This is the scheduler foundation, not an end-to-end fault campaign or a checker verdict.

The existing `calve skew` command and three-catalog CI gate remain available alongside planning. They execute fault-free histories: reference must report a write-skew lead, while Lakekeeper and Nessie must pass. This scheduler does not alter that gate or supply fault records to its checker. The future campaign coordinator must prepare workloads/baselines before the scheduling origin and implement recovery before checking faulted runs.

## Plans and reproducibility

`fault.Generate(seed, config)` returns a `Plan` containing `calve-faults-v1-pcg`, the seed, normalized configuration, and the entire ordered event list. It sorts copies of target IDs and kinds, rejects duplicates, and uses a private [PCG generator](https://pkg.go.dev/math/rand/v2#NewPCG) with explicit 64-bit bounded draws. Wall-clock time, workload RNG calls, map iteration order, and caller input ordering do not affect generation. A golden fixture detects accidental algorithm changes; change the version when generation rules change.

The same seed needs the same configuration and scheduler version to reproduce the plan. It does not fix application thread timing, actual controller latency, or which operations fail. A run's unique execution ID is separate from the deterministic event IDs, so repeated plans use different backend object names (`runID-faultID`). Callers must choose a new execution ID each time. Logical target IDs are resolved by the controller, not interpreted as shell commands or credential-bearing URLs.

All serialized durations use explicit `_ns` field names. Configuration includes the workload horizon, healthy warmup, ranges for healthy gaps/fault windows, control timeout, total cleanup timeout, latency delay, and controlled-stop grace. CLI defaults are a 60-second horizon, 5-second warmup, 2–5-second healthy gaps, 1–3-second fault windows, 5-second control budget, 30-second cleanup budget, and 250 ms latency. Stop grace is 1 second. Inputs that cannot fit even one complete fault plus its healing budget are rejected; plans are capped at 10,000 events.

V1 serializes faults globally, a stricter rule than non-overlap on each target. It samples a healthy gap and fault duration, then selects the target/kind and any stream direction. The next window is after the previous planned heal plus its control budget and next healthy gap. Events retain their original offsets when execution is late; the scheduler never resamples them.

`fault.Save` validates and creates a new plan file, syncing and closing it before execution can proceed. It refuses to overwrite an existing artifact. `fault.Read` rejects unknown fields, trailing data, unsupported versions, or a plan that does not regenerate from its seed/configuration. The JSON is suitable for review and replay; it contains no creation timestamp that would change otherwise identical plan files.

## Controller contract for #13 and #14

```go
type Controller interface {
    Apply(context.Context, fault.Injection) error
    Heal(context.Context, fault.Injection) error
    Observe(context.Context, fault.Injection) (fault.State, error)
}
```

`ExecuteOptions.Controllers` maps each planned kind to a controller. `Injection` carries the run ID and complete planned event; `ObjectName()` provides the run-scoped fault name. Controllers must honor context deadlines and return credential-free observations/errors. `Apply` responses alone do not prove the fault took effect: `Observe` must verify the requested state and parameters. `Healthy` means the controller's fault state is healed, not that Iceberg is ready or previous commits have settled.

| Kind | Apply | Heal | Fixed parameters |
| --- | --- | --- | --- |
| latency | Add a latency toxic | Remove the run's toxic | Selected stream, configured delay, jitter 0, toxicity 1 |
| reset | Add `reset_peer` | Remove the run's toxic | Selected stream, toxicity 1 |
| partition | Add blocked-data timeout toxics | Remove both run-owned toxics | Both streams, timeout 0, toxicity 1 |
| kill | Kill the catalog process | Start the same container | SIGKILL; preserve storage |
| pause | Pause the container | Unpause it | Same container |
| restart | Controlled stop | Start the same container | SIGTERM and recorded grace; preserve storage |

`Heal` must be idempotent, act only on this run's changes, and safely handle an event that was planned but never applied. It must reconcile an uncertain apply before reporting `Healthy`: a late apply cannot be allowed to reactivate the fault after cleanup. Returning from a canceled client request alone does not establish that a control operation stopped. If a backend cannot reconcile that state within its budget, it must return an error/unknown state. Calls are sequential and get deadlines; the executor cannot forcibly terminate a backend that ignores its context contract.

Before process campaigns, #14 must validate storage survival, effective restart policies, container state transitions, and readiness; `TestDockerLiveEffectsAndSurvival` does this for all three catalogs in CI, and `fault.Docker` reports an automatic restart policy as an error. #13 must validate directional proxy effects and restore connectivity; `TestToxiproxyLiveEffects` does this against the compose Toxiproxy in CI. No real backend is silently replaced with a fake.

## Executor and history

`fault.Execute(ctx, plan, options)` validates the plan and complete controller registry, reserves a new history file, saves/syncs the complete plan, and records `fault-plan` metadata before any backend call or load callback. It inspects each distinct kind/target before load, then records `workload-start` and calls optional `OnStart(loadCtx)`. Callers must finish catalog/table setup and baseline reads before this stage; `OnStart` should start background writes and return promptly.

The executor uses monotonic deadlines relative to its workload-start origin. Apply is followed by observed `Active`; heal is followed by observed `Healthy`. An apply/observe/heal error, canceled context, or missed healing deadline stops new injections. A late action is recorded against its original offset. If an entire window plus healing budget has expired, the fault is not injected.

History uses controller process -2 and `f: "fault"` invocation/completion pairs, with stages `preflight`, `execution`, and `cleanup`. Payloads include execution/fault identity, complete planned event, action, planned/actual offsets, lateness, and observed state. The shared recorder's timestamps bound calls; offsets describe scheduler timing, not the instant a backend effect became active. Only a successful call with the expected observed state gets `ok`; errors or uncertainty get `info`. These outcomes describe controller actions, not catalog commits.

On every exit after plan persistence, the executor cancels `loadCtx` before cleanup. Cleanup uses a [cancellation-independent context](https://pkg.go.dev/context#WithoutCancel) with its own total budget and per-action limits. It first handles attempted faults whose healing was not verified, newest first, then all remaining planned IDs in reverse order. An apply is tracked before its call because an error may still leave the fault active; it leaves the priority group only after a verified scheduled heal. Thus unavailable backends for future, unstarted events cannot consume the budget before an active/uncertain fault gets a repair attempt. Every planned ID is visited once; expired budgets are recorded as failed/uncertain cleanup attempts. New faults do not resume. Original execution, cleanup, and history-save errors are returned together. A controller-stage history checkpoint is synced/closed on normal success or error return; hard-kill durability is not provided.

`ExecutionResult.Completed` means only that this scheduler and cleanup completed. Cancellation is a signal to background load, not proof it has joined. The caller still owns bounded joining, readiness checks, catalog recovery/unknown-commit resolution, final table observations, and extending/saving the shared recorder as a complete campaign artifact. Existing skew checking intentionally rejects fault histories; fault-aware checking is not introduced here.

## Validation and remaining integration

Tests cover deterministic/reordered inputs, independent RNG state, a golden plan hash, timing bounds/mechanisms, strict artifact round trips, and refusing overwrite. A virtual clock exercises sequencing without waiting through a full campaign. Fake controllers verify persistence before backend/load calls, verified healing before the next fault, uncertain applies, cancellation, blocking calls that honor deadlines, failed/late heals, cleanup-budget exhaustion, complete cleanup attempts, and separate object names for repeated seeds. Regression tests reproduce an early uncertain apply and a later failed heal while a future event's backend consumes the remaining cleanup budget; in both cases the active fault must be repaired first. A real-clock timeout test verifies bounded cancellation. Subprocess tests exercise both `calve run --plan-only` and the existing `calve skew` command, including skew's actual valid/invalid exit codes. No container was killed, paused, or network-faulted for this change.

Remaining work is to register both controllers in `calve run`, wire workload setup/start/join and run-level manifests (revision, workload, image/storage/restart configuration), and establish recovery boundaries under #16 before enabling real `calve run` campaigns. Issue #15 stays open for that integration; this draft delivers the independently testable scheduling core and plan command.
