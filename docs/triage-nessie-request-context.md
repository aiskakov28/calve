# Nessie: concurrent commits get HTTP 500 from an inactive request context

Status: filed as [projectnessie/nessie#13271][issue] (no reply yet). Reproduced
on Nessie 0.109.0 and on `main` built from source, without injected faults. A
trace narrows the cause to shared asynchronous task completion; it is not yet
confirmed by a controlled test, and there is no fix.

When concurrent writers commit to the same table through the Iceberg REST API,
some commits get HTTP 500 `Failed to write snapshot`. Nessie's log shows a
`ContextNotActiveException` when the commit writes table metadata through the
request-scoped `CatalogProducers.objectIO` proxy. A 500 is an unknown outcome,
not a rejection: a few of these updates were visible in later reads. No
acknowledged update was lost and no rejected one was applied, so this is an
availability bug, not an isolation or durability finding.

## What Calve saw

The `append` and `atomic` workloads hit it in the first sweeps: one fault-free
sweep ([run 40992074092](https://github.com/aiskakov28/calve/actions/runs/40992074092))
had 38 HTTP 500s across five Nessie append runs, and three of those updates
were visible in a later read. The README's result tables count these runs as
inconclusive. A two-table transaction in a reset-fault campaign also got this
500, after the last fault had healed; its outcome is unknown.

## Minimal reproduction

[`scripts/repro-nessie-request-context.sh`](../scripts/repro-nessie-request-context.sh)
(bash, curl, jq) creates a table and has several writers each send their own
`set-properties` commits with no requirements. Conflicts (409) are expected;
500s are the finding. Each request is sent once, and a final read checks that
every acknowledged property is present and no rejected one is.

```sh
docker compose up -d --wait nessie
scripts/repro-nessie-request-context.sh http://localhost:8183/iceberg        # 8 writers
scripts/repro-nessie-request-context.sh http://localhost:8183/iceberg 1 200  # control
```

It exits 3 when a commit got a 500. The failure is intermittent, especially
with few writers, so a run that exits 0 does not rule it out.

HTTP 500s per run of 200 commits, each on a fresh table:

| Setup | 1 writer | 2 writers | 4 writers | 8 writers |
| --- | --- | --- | --- | --- |
| 0.109.0, Calve `append` | 0 in 3 runs | | | 72 in 5 runs |
| 0.109.0, Python client, separate stack | 0 in 2 runs | | | 21 in 3 runs |
| 0.109.0, Python client, no redirects | 0 in 2 runs | 6 in 5 runs (4 with a 500) | 21 in 3 runs | 23 in 3 runs |
| 0.109.0, two fresh stacks ([as reported][issue]) | 0 in 4 runs | 12 in 10 runs (7 with a 500) | | |
| `main` `3a87338` built from source | 0 in 2 runs | 42 in 5 runs (all 5) | | |
| 0.109.0, the bash script (2026-11-02) | 0 in 1 run | 0 in 3 runs | 0 in 2 runs | 10 in 1 run |

Every metadata-write 500 that was checked against the server log matched a
`ContextNotActiveException` on `objectIO`, with `TasksServiceImpl.finalResult`
in the stack. A few other 500s named an inactive `SecurityIdentityProxy` or
`QuarkusResultCollectorFactory` context instead. Across all runs, every
acknowledged update was in the final read and no rejected one was.

## What happens

On an instrumented build of `main` (`3a87338`), two-writer runs gave 29 matching
metadata-write failures. In each one, the context the commit captured when it
started was still valid, but no request context was active on the thread that
did the metadata write. Each failing commit's snapshot load shared its task and
future with an earlier snapshot-store task from a different request.

That fits the source: [`TasksServiceImpl.submit`][submit] shares one base future
per task key, [`finalResult`][final] completes it, and the catalog attaches
continuations that eventually write through the request-scoped
[`objectIO` producer][objectio]. A continuation that runs when another
request's shared task completes can find no active context, or a different
one: in 55 other writes the active context belonged to the related store
task's request.

This is a strong explanation, not a proof. The instrumentation changes timing,
authentication was off, and identity hashes can collide in principle. Nessie
already wraps background tasks with [`ThreadContext`][threadcontext], so the
gap is specific, not a general lack of context propagation. A deterministic
test that controls when the shared task completes is the next step, before any
fix.

## History

The investigation ran in four stages: an audit of the saved evidence,
a clean reproduction on fresh stacks, a review against Nessie's latest release
and `main`, and the issue draft, followed by the source build and the context
trace. The stage write-ups, checked request/response witnesses and the Python
client are in the [branch history][history]; the full request histories and
server logs were kept locally, outside the repository.

[issue]: https://github.com/projectnessie/nessie/issues/13271
[submit]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L137-L156
[final]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L159-L165
[objectio]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L230
[threadcontext]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/async/src/main/java/org/projectnessie/nessie/tasks/async/wrapping/ThreadContextTasksAsync.java#L31-L39
[history]: https://github.com/aiskakov28/calve/tree/228d231/docs
