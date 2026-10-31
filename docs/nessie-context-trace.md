# Nessie issue #13271: request-context trace

On 2026-10-31, a locally instrumented build of Nessie `main` at [`3a8733847ae0`](https://github.com/projectnessie/nessie/commit/3a8733847ae0a1cf1c24ea694718fbf252912840) reproduced the metadata-write failure from [Nessie #13271](https://github.com/projectnessie/nessie/issues/13271). In all 29 matching failures, the context captured by the affected commit was still valid, but no request context was active on the thread when it attempted the metadata write. This narrows the failure to the context handoff; it does not by itself prove a fix.

The same tracing also observed metadata writes under a different active context from the one captured when that commit began. These observations were made with authentication and authorization disabled, so they do not establish what happens in an authenticated deployment.

## Experiment

We built the same source revision used in the [unmodified source-build baseline](nessie-source-baseline.md), in a separate checkout and Docker volume. Temporary diagnostics recorded request-context state and object identity, task/future identity hashes, callback thread names, and synthetic local test locations. The diagnostic code used weak references to the captured context state, so it did not intentionally keep a request context alive. No principals, credentials, or request bodies were recorded by the diagnostics. The changes were only for local observation and are not a proposed fix.

The instrumented server ran on a fresh PostgreSQL/Silo catalog with the same client and workload as the baseline: unique `set-properties` commits, no client retries, and no injected faults. A single-writer control ran before and after five two-writer runs. Each run used a fresh table and sent 200 requests.

| Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 400 | 400 | 0 | 0 | 0 |
| 2 | 5 | 1,000 | 583 | 386 | 31 | 5 |

Of the 31 HTTP 500s, 29 matched the original metadata-write exception and its `ContextNotActiveException`/`TasksServiceImpl.finalResult` stack; each was correlated by its unique metadata location and request timing. Two other 500s named an inactive `SecurityIdentityProxy` while obtaining the principal, also with `TasksServiceImpl.finalResult` in the server trace. These are separate observations from the 29 metadata-write failures; similar security-context errors had appeared in earlier release-image controls.

All 983 acknowledged updates appeared in the corresponding final table reads, and no rejected 409 update appeared. All 31 updates receiving 500 were absent from those reads; their outcomes remain **unknown**, not proven rejected or lost. Nessie, PostgreSQL, and Silo did not restart or run out of memory during the workload.

## What the trace shows

1. At the beginning of each of the 29 failing commits, the recorded request context was active and valid. At its metadata write, the same captured context was still valid, but `getStateIfActive()` returned no active request context on the callback thread. Each of those writes raised the matching exception and did not log a successful return from the write.
2. Each affected commit's snapshot load had an earlier snapshot-store task with the same task identifier and future identity hash, associated with a different captured context. The load callback ran with no active context. This is consistent with the source: [`TasksServiceImpl.submit`](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L137-L156) shares a base future by task key, and [`finalResult`](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L159-L165) completes it. The catalog attaches continuations to the snapshot task and eventually writes through the [request-scoped `objectIO` producer](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L230).
3. The earlier store task's captured context was invalid when its callback was subsequently logged in 28 of the 29 cases. In the remaining case, the weak reference had already been collected, so its validity could not be determined. This check happened **after** the failed write; it does not establish exactly when the earlier context expired.
4. In another 55 metadata-write attempts during the two-writer runs, the commit's captured context and the active context were both valid but were different objects. Each write returned without an `objectIO` exception. In all 55, the active context's identity hash matched the captured context of a related store task with the same task/future identifiers. A returned metadata write is not necessarily a successful HTTP commit. Neither single-writer control showed this different-context pattern.

The [checked witness](evidence/nessie-context-trace-witness.json) records a sequence containing both observations. The `same` field compares the actual context objects by reference while the captured context is still reachable. Task and future IDs, and the cross-probe context correlations, use Java identity hashes, which can theoretically collide. The repeated matching task IDs and timing, together with the source, make shared-task completion a strong explanation, but a controlled regression test is still needed to establish causality. Nessie's task executor already wraps background work with [`ThreadContext`](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/async/src/main/java/org/projectnessie/nessie/tasks/async/wrapping/ThreadContextTasksAsync.java#L31-L39); the evidence does not support simply saying that no context propagation exists anywhere.

The instrumentation adds logging and changes timing, so these counts are not a failure-rate estimate and cannot be compared directly with the unmodified baseline. That baseline independently reproduced 42 metadata-write 500s in five two-writer runs, with 400/400 successful single-writer requests.

## Retained evidence and next step

The local ignored directory `results/nessie-context-trace-20261031/` contains the diagnostic source and patch, exact input hashes, build and image verification, all seven request histories and final reads, the server log, and scripts that audit and correlate the trace. The server-log SHA-256 is `42a10ae96c42e02a84b0472d6912eb078b2a99d70b54acfc955edeb259fe1930`. The original source checkout, baseline data, and stopped test volumes are retained separately.

The next stage is a deterministic upstream regression test that controls the shared task's completion and checks which request context is active for each continuation, including when the earlier request context has ended. Only after that should a candidate fix be evaluated for preserving the correct context for each request.
