# Nessie issue #13271: source-build baseline

Stage 1 completed on 2026-10-31. The request-context issue reported in [Nessie #13271](https://github.com/projectnessie/nessie/issues/13271) also reproduces on a server built from Nessie `main` at commit [`3a8733847ae0a1cf1c24ea694718fbf252912840`](https://github.com/projectnessie/nessie/commit/3a8733847ae0a1cf1c24ea694718fbf252912840). That commit was still the head of `main` when the results were checked. The source build identifies itself as `0.109.1-SNAPSHOT`.

The experiment started a fresh catalog with separate PostgreSQL and Silo volumes. Each run created a new table and sent 200 unique Iceberg REST `set-properties` commits with empty requirements. One-writer controls ran before and after five two-writer runs. The client connected directly to Nessie, made no commit retries, and injected no faults.

| Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 400 | 400 | 0 | 0 | 0 |
| 2 | 5 | 1,000 | 557 | 401 | 42 | 5 |

All 42 HTTP 500 responses named different metadata locations. Each matched exactly one server HTTP/500 trace within the client request interval, containing `ContextNotActiveException`, `CatalogProducers.objectIO`'s `ClientProxy.writeObject`, and `TasksServiceImpl.finalResult`. Every failed request overlapped a request from the other writer on the client side; that does not prove simultaneous server execution. A [checked witness](evidence/nessie-source-main-context.json) includes a request, response, overlapping requests, and trace excerpt.

Every one of the 957 acknowledged updates remained in its matching final table read, and no update rejected with HTTP 409 appeared. The 42 updates that received HTTP 500 were absent from the final reads. Their outcomes remain **unknown**: absence alone does not establish rejection or data loss. These runs confirm the reported availability issue on the tested source revision; they do not establish its root cause or quantify a change in failure rate relative to the earlier release tests.

## Provenance and saved evidence

- The tracked source had no changes before or after the build at the recorded commit. Nessie's `:nessie-quarkus:quarkusBuild` succeeded with Java 21.0.13; the application manifest records `0.109.1-SNAPSHOT` but does not embed a Git SHA. The captured checkout, build log, clean artifact export, and verified image/JAR hashes link the runtime to the recorded source.
- The source-built runtime used Java 21.0.12.1, Quarkus 3.40.1.1, JDBC2 with PostgreSQL 17.6, and Silo `RELEASE.2026-09-03T13-18-01Z`. The catalog started empty. Nessie, PostgreSQL, and Silo had no restarts or out-of-memory kills during the workload.
- The exact client and configuration hashes, build and runtime image identities, build log, all seven raw histories, run commands, final reads, server logs, and audit output are retained locally under `results/nessie-source-baseline-20261031/`. This directory is ignored by Git. The source checkout, Docker build cache, and stopped test volumes are also retained for the next investigation stage.

Next, trace the request context and asynchronous task completion on this source build to determine why the callback reaches `objectIO` without an active context. The existing shared-future explanation remains a hypothesis until that is demonstrated.
