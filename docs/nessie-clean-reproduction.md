# Nessie report: stage 2 clean reproduction

Status: completed on 2026-10-31. This is a controlled reproduction of the
request-context availability issue on Nessie 0.109.0. A current-upstream and
duplicate check, followed by the final report, remain separate stages.

Two new Docker Compose projects, `calve-nessie-stage2-a-20261031` and
`calve-nessie-stage2-b-20261031`, each started with an empty namespace list and
their own new PostgreSQL and Silo volumes. The projects had separate networks,
published no host ports, and were stopped afterward without deleting their
containers or volumes. The client ran inside each project network and sent
direct HTTP requests to `http://nessie:19120/iceberg`, with redirects disabled,
no SDK retries, and no injected faults. The services recorded no restarts or
out-of-memory kills.

Before the first workload in each project, the runner saved the exact command,
client and Compose file hashes, container/image IDs, runtime versions,
configuration allowlist, and an empty-catalog check. It rechecked frozen input
hashes and saved the command before every run. Each run created a fresh table
and sent 200 unique `set-properties` commits with empty requirements. Each
logical writer sent its requests sequentially.

| Stack | Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 1 | 2 | 400 | 400 | 0 | 0 | 0 |
| A | 2 | 5 | 1,000 | 667 | 330 | 3 | 2 |
| B | 1 | 2 | 400 | 400 | 0 | 0 | 0 |
| B | 2 | 5 | 1,000 | 646 | 345 | 9 | 5 |
| **Total** | | **14** | **2,800** | **2,113** | **675** | **12** | **7** |

All 12 HTTP 500 responses reported failed metadata writes to distinct
locations. Each location matched exactly one server HTTP/500 block within the
request time window. Every matched block included
`ContextNotActiveException`, `CatalogProducers.objectIO` proxy access, and a
`TasksServiceImpl.finalResult` frame. All 12 failed client request intervals
overlapped a request from the other writer. This is client-side overlap, not
proof of simultaneous server execution. The [checked witness][witness] gives
one complete request/response and matching trace excerpt from each stack.

All 2,113 acknowledged updates appeared in their matching final table reads;
none of the 675 rejected 409 updates appeared. None of the 12 updates with a
500 appeared in the final read. They remain **unknown outcomes** under the
[Iceberg REST contract][outcomes]; absence does not prove that the server
rejected them. The observation is an intermittent availability failure under
two-writer contention, not established data loss or an atomicity violation.

The inputs and runtime identities recorded before execution were:

| Component | Recorded identity |
| --- | --- |
| Nessie | 0.109.0; `ghcr.io/projectnessie/nessie@sha256:61a1a14e73d76ac3851e7bef627e4ef426f4f0cf71c352843908a5834acd19e1` |
| Java in Nessie | OpenJDK 21.0.12.1, Red Hat build 21.0.12.1+1-LTS |
| Python client | 3.13.18; `python@sha256:2d44738ebf4ce5f63b9d9ea871e9c7cfdc0426f7b0c97cac915fd88ded1d86bb` |
| Client source SHA-256 | `e8500c18032ab0b160703170ff9d99dc32368130b9b075f2823c25a987cfde77` |
| Version/object stores | PostgreSQL 17.6 / JDBC2; Silo `RELEASE.2026-09-03T13-18-01Z` |

The full raw client histories, server logs, pre-execution manifests, SHA-256
checks, and independent audit output are retained locally under the ignored
`results/nessie-report-stage2-20261031/` directory. The committed
[Compose override][compose] pins all server images and removes host port
bindings. A new run can use the [Python reproducer][client] on that project's
Compose network. Use a new project name and a new output filename for each
run. The exit code is 4 when a final read fails or a 500 outcome remains
unresolved; inspect `status_counts` and the saved history in either case.

The same symptom was observed earlier by aiskakov28. These clean, two-writer
controls strengthen the evidence and close the historical provenance gap for
the executed client and runtimes. They do not establish whether upstream
already has this issue, whether a newer Nessie build still reproduces it, or
the exact cause of the inactive context.

[witness]: evidence/nessie-clean-two-writer-context.json
[outcomes]: design.md#responses-and-unknown-outcomes
[compose]: ../scripts/repro-nessie-request-context.compose.yml
[client]: ../scripts/repro-nessie-request-context.py
