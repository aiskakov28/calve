# Nessie report: stage 3 upstream and scope review

Status: reviewed on 2026-10-31. This review distinguishes the reproduced
symptom from nearby reports and records a source-level explanation as a
hypothesis. It does not claim first discovery or a proven root cause.

## Version and source status

The [latest published Nessie release][release] was 0.109.0, published on
2026-10-08. The [clean reproduction][clean] ran that version from its immutable
image digest, so it covers the current published release as checked on
2026-10-31. The release tag resolves to commit
`261e936e2b72a130ebc5e3a10b8b1e9d55eb6872`. Upstream `main` was
`3a8733847ae0a1cf1c24ea694718fbf252912840` when checked.

The complete contents of these three files were byte-for-byte identical at
the release tag and that `main` commit:

- [`CatalogProducers.java`][producer], which produces `ObjectIO` with
  `@RequestScoped` at lines 222–230.
- [`CatalogServiceImpl.java`][commit], whose Iceberg table commit continuation
  calls `storeTableSnapshot` at lines 676–682. A later comment at lines
  568–577 says the returned snapshot stages must be awaited to keep the
  request context alive.
- [`TasksServiceImpl.java`][tasks], which shares a task result future by task
  key at lines 133–156 and calls `resultFuture.complete(result)` at lines
  159–164.

This comparison finds no change in those three files between 0.109.0 and the
checked `main` commit. Other code or dependency changes could still affect the
behavior. An unreleased build from `main` was not run.

## Existing-report check

The repository's open and closed issue/PR search was checked for
`ContextNotActiveException`, `RequestScoped context`, `objectIO`,
`Failed to write snapshot`, and broader concurrent-commit / HTTP 500 terms.
No report matching an inactive `CatalogProducers.objectIO` context during
concurrent Iceberg REST commits was found in those searches. This is a search
result, not proof that the symptom was never reported.

| Representative GitHub search on 2026-10-31 | Matches | Relevant result |
| --- | ---: | --- |
| [Issues: `ContextNotActiveException`][context-search] | 1 | #9852, startup health check |
| [Issues: `Failed to write snapshot`][write-search] | 0 | None |
| [Issues: `objectIO`][object-search] | 4 | Storage/health-check reports; no matching commit failure |
| [PRs: `RequestScoped`][scope-pr-search] | 4 | No matching Iceberg commit fix |

The closest exception match is [Nessie #9852][health]: a
`ContextNotActiveException` for `ConfigProducers` in an object-store **health
check** at startup on 0.99.1-SNAPSHOT. It explicitly says later read/write
operations worked. Its component, operation and timing differ from this
commit-time `objectIO` failure. [Nessie #8830][read] also contains an Iceberg
REST HTTP 500 and task-service frames, but its reported failure is an S3
**metadata read** (`IOException`), without the inactive request context seen
here. [Nessie #10013][ancestor] discusses parallel writers and snapshot
ancestor validation; it does not report this server exception. These are
related search results, not established duplicates.

The original metadata-write HTTP 500 symptom was already noted by
aiskakov28 in [Calve's 409 triage][collaborator]. This investigation adds the
two-writer, fault-free reproduction, exact client and runtime provenance, and
matched server traces. Any upstream report should credit that earlier
observation and present this work as diagnosis and stronger reproduction.

## Scope and explanation

The observed issue is intermittent availability loss for valid Iceberg REST
`set-properties` commits under two-writer contention on the 0.109.0
JDBC2/PostgreSQL/Silo setup. A single writer completed 800/800 requests in
the [clean controls][clean]. All 12 two-writer HTTP 500s matched distinct
inactive `objectIO` request-context traces; 409 conflicts were tracked
separately. No acknowledged update disappeared in the final reads. The 500
outcomes remain unknown and do not establish an Iceberg REST contract or
atomicity violation. Other version stores, object stores, authentication
settings and an unreleased `main` build have not been tested here.

**Hypothesis, not root-cause finding:** the catalog attaches metadata-write
continuations to asynchronous snapshot results while `objectIO` is request
scoped. The task service shares a result future and completes it from a task
completion path. A continuation could therefore reach the `objectIO` proxy
without an active request context. The observed stack contains
`TasksServiceImpl.finalResult` and the metadata-write continuation, which is
consistent with this path. The evidence does not identify which request
context boundary failed, or show that a specific future-sharing race is
necessary. A fix would require a targeted upstream test and code review.

[release]: https://github.com/projectnessie/nessie/releases/tag/nessie-0.109.0
[clean]: nessie-clean-reproduction.md
[producer]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L230
[commit]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/catalog/service/impl/src/main/java/org/projectnessie/catalog/service/impl/CatalogServiceImpl.java#L676-L682
[tasks]: https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L133-L164
[health]: https://github.com/projectnessie/nessie/issues/9852
[read]: https://github.com/projectnessie/nessie/issues/8830
[ancestor]: https://github.com/projectnessie/nessie/issues/10013
[collaborator]: triage-409-without-requirements.md#nessie
[context-search]: https://github.com/projectnessie/nessie/issues?q=is%3Aissue+ContextNotActiveException
[write-search]: https://github.com/projectnessie/nessie/issues?q=is%3Aissue+%22Failed+to+write+snapshot%22
[object-search]: https://github.com/projectnessie/nessie/issues?q=is%3Aissue+objectIO
[scope-pr-search]: https://github.com/projectnessie/nessie/pulls?q=is%3Apr+RequestScoped
