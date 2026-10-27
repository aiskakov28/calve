# Calve design doc

## Goal

Find isolation and durability anomalies in open-source Iceberg REST catalogs under concurrency and faults, with minimal standalone reproductions.

## Non-goals

- Testing managed cloud catalogs.
- Benchmarking performance.
- Testing query engines (Spark, Trino) rather than catalogs.

## What the catalogs promise

The baseline is the [REST OpenAPI shipped with Iceberg 1.10.1][rest-spec], matching our pinned reference catalog. Source links below use release commit `ccb8bc435062171e64bc8b7e5f56e6aed9c5b934`. Record the catalog version and applicable spec revision with each finding; do not silently apply newer protocol features to older catalogs. Quotations are spec text; the checker rules below are Calve's interpretation.

### Single-table commit: requirements and updates

`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (`updateTable`) accepts `requirements` and `updates`. The [endpoint description][single-commit] says:

> Requirements are assertions that will be validated before attempting to make and commit changes.

The [requirement schemas][requirements] define the predicates:

| Requirement | Condition being checked |
| --- | --- |
| `assert-create` | The table does not exist. |
| `assert-table-uuid` | The UUID matches `uuid`. |
| `assert-ref-snapshot-id` | The named branch/tag points to `snapshot-id`; a null ID requires that the ref does not exist. |
| `assert-current-schema-id` | The current schema matches `current-schema-id`. |
| `assert-last-assigned-field-id` | The last assigned column ID matches. |
| `assert-last-assigned-partition-id` | The last assigned partition ID matches. |
| `assert-default-spec-id` | The default partition spec ID matches. |
| `assert-default-sort-order-id` | The default sort order ID matches. |

Both commit endpoints require HTTP 400 for an unrecognized requirement or update. Their HTTP 409 (`CommitFailedException`) descriptions say:

> one or more requirements failed.

The client is allowed to retry after a conflict. Calve records each attempt separately. A retry that refreshes requirements is a new conditional operation, not evidence that the rejected attempt committed.

The [table spec's concurrency model][table-concurrency] supplies the single-table atomicity basis: metadata changes replace the table's metadata pointer atomically. **Calve rule:** a successful commit must fit a sequence of atomic table changes in which its supplied requirements hold. For a deterministic negative test, change a schema from ID `s0` to `s1`, wait for success, then submit a commit requiring `s0`. With no intervening schema changes, success on that stale requirement is a violation.

Two concurrent commits succeeding is not sufficient evidence of a bug. The current property workload sends `requirements: []` and changes distinct keys; both commits may succeed, and both acknowledged keys must survive if no later update removes them. A one-winner test needs a requirement that the winning update actually invalidates; matching only the table UUID does not provide that guard.

### Multi-table commit: atomicity and its limits

`POST /v1/{prefix}/transactions/commit` (`commitTransaction`) is described as an ["atomic operation"][multi-commit]. Its [request schema][commit-requests] contains `table-changes`, each with an identifier, requirements, and updates. An empty `updates` array is permitted, so a table change can contain requirements alone.

**Calve rule:** treat the requested changes as one all-or-nothing commit. After recovery and resolution of in-flight requests, a committed transaction must have applied all its changes; a rejected transaction must have applied none. An unknown outcome permits either complete result, not a permanently committed subset. Orphan metadata/data files alone do not establish a partial commit: inspect the state reachable through the catalog.

Atomicity does not by itself establish serializable isolation. The OpenAPI does not explicitly define a multi-table isolation level, when requirements on unchanged tables must be revalidated against concurrent writes, or a consistent snapshot across separate table loads. The table spec also makes [isolation depend on the validations selected by writers][table-concurrency]. Do not infer a blanket multi-table serializability guarantee from it.

For the workload in [apache/iceberg#18552][skew-report], start from schema IDs `(a0, b0)`: T1 requires both IDs and changes only A; T2 requires both IDs and changes only B. If requirement validation and updates form one serializable operation, at most one can commit: either winner invalidates the other's guard. Both succeeding is an isolation anomaly under that model, but confirmation as a catalog bug needs a cited catalog guarantee or clarification of the OpenAPI's validation semantics. The issue report is a reproduction lead, not a normative guarantee. Separately test a stale requirement on a requirement-only table after the conflicting transaction has already completed; that avoids relying on concurrent isolation semantics.

### Responses and unknown outcomes

These mappings come from the [single-table][single-commit] and [multi-table][multi-commit] response definitions, except where explicitly marked as Calve policy.

| Response | History | Checker interpretation |
| --- | --- | --- |
| Single-table HTTP 200 | `ok` | Commit acknowledged; response contains `metadata-location` and `metadata`. |
| Multi-table HTTP 204 | `ok` | Transaction acknowledged; no response body. |
| HTTP 409 | `fail` | Conditional commit rejected; its changes must not be committed. |
| HTTP 400; documented 401/403/404/419 | `fail` | Request rejected (invalid/unknown input, authentication, authorization, or missing table). |
| HTTP 500/502/504 | `info` | Both endpoints explicitly say the "commit state is unknown." |
| HTTP 503 | `info` | The [shared response][service-unavailable] allows partial request processing; it does not establish rejection. |
| Other 5xx; timeout, reset, or incomplete response | `info` | Conservative Calve policy: insufficient evidence of either success or rejection. |

Keep status, error type, and the raw response available for triage. The [reference catalog report][skew-report] includes HTTP 200 for multi-table success; Calve can accept that acknowledgment while recording the departure from the documented 204 separately. An undocumented status does not itself demonstrate an isolation or durability failure. The 1.10.1 contract does not provide an idempotency key for these operations; do not blindly resubmit an unknown commit and assume exactly-once execution.

### When Calve can call a result a bug

A candidate must identify the endpoint, supplied requirements/updates, observed outcome, and the exact promise it contradicts. Direct candidates include an acknowledged update missing without a later operation that undoes it, a rejected operation's unique effect being present, a successful commit with a demonstrably false requirement, or a permanently partial multi-table commit. Use unique operation markers to establish attribution.

Before confirming a state-based finding, heal faults, stop new writes, let server-side work settle, and reload through the catalog. Rule out intentional overwrites/removals, snapshot expiration, stale client caches, and table recreation. If recovery or a trustworthy final read is unavailable, report the run as inconclusive. A timeout or 5xx alone is never proof of lost data. Likewise, observing old A and new B in two separate reads is not proof of a partial transaction if the transaction could have committed between those reads.

The current property-based append workload can check lost or rejected keys, but a property map cannot expose duplicate occurrences of a key. Its `VALID` result covers that workload's invariants; it does not establish general serializability, exactly-once data-file appends, or behavior under faults that were not exercised.

[rest-spec]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml
[single-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L993-L1109
[requirements]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3093-L3228
[multi-commit]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L1328-L1441
[commit-requests]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3504
[service-unavailable]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L4651-L4667
[table-concurrency]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L90
[skew-report]: https://github.com/apache/iceberg/issues/18552

## History format

One JSON record per line (`internal/history`):

| Field | Meaning |
| --- | --- |
| index | position in the history |
| process | logical client |
| type | invoke, ok, fail, info (unknown) |
| f | operation name |
| value | workload-specific payload |
| time | nanoseconds since run start |
| status | HTTP status, 0 if no response |
| error | error text for fail and info |

## Workloads and checkers

| Workload | Checker | Anomalies |
| --- | --- | --- |
| append | `checker.CheckAppend` | lost, phantom-failed, duplicate, unexpected |
| write skew | `checker.CheckSkew` (planned; see below) | conflicting commits under the serializable model; stale requirements accepted; acknowledged changes lost or rejected changes applied |
| compare-and-set | TODO (Porcupine) | non-linearizable history |
| multi-table general | TODO (dependency graph + cycle search) | G0, G1c, G2 cycles |

### Write-skew workload and checker (planned)

This is the design for the workload, checker, and `calve skew` command in issues [#8](https://github.com/aiskakov28/calve/issues/8), [#9](https://github.com/aiskakov28/calve/issues/9), and [#10](https://github.com/aiskakov28/calve/issues/10); they are not implemented yet. The concurrent probe follows [apache/iceberg#18552][skew-report]. It tests one specific invariant, not general multi-table serializability.

#### Workload rounds

1. Create two tables, A and B, with names unique to the run and round. While no writers are active, load their UUIDs, schemas, and current schema IDs `(a0, b0)`. A failed setup or baseline read makes the round inconclusive, not valid.
2. Construct both requests from that same baseline. Every table change includes `assert-table-uuid` and `assert-current-schema-id`. T1 updates A and includes a requirement-only change for B (`updates: []`); T2 does the reverse:

   | Transaction | Requirements | Updates |
   | --- | --- | --- |
   | T1 | A is `a0`; B is `b0`; both UUIDs match | Add a schema to A and make it current; B unchanged |
   | T2 | A is `a0`; B is `b0`; both UUIDs match | Add a schema to B and make it current; A unchanged |

3. Use valid, distinct added fields that identify the round and transaction. The [schema updates][schema-updates] are `add-schema` followed by `set-current-schema` with `schema-id: -1`. Check the resulting schema contents rather than assuming which new ID the catalog assigns. The new current schema must differ from the baseline. No other writers, schema reversions, or maintenance operations are allowed in these rounds.
4. Release the two clients through a barrier. Each sends exactly one `CommitTransaction`, with no client retries or refreshed requirements. Record invocation/completion intervals: a barrier encourages overlap but does not guarantee it. Report whether the recorded intervals overlapped. Start with fault-free rounds; later fault tests use the same model.
5. After both attempts finish, heal any faults and resolve server-side work before loading both tables again. Preserve the observations and histories before cleanup. Fresh table names prevent a late operation from contaminating later rounds; they do not prove that a timed-out operation has stopped.

Also run a **sequential stale-requirement control**, on fresh tables in each direction. Commit T1, verify A's new schema, then invoke the original T2 requiring `a0`. T2's requirement on A is now false even though its A change has no updates. Expect HTTP 409 and no T2 effect on B. If the first commit or its verification is uncertain, the control is inconclusive. Accepting the stale requirement in this controlled history is a direct contract violation without an assumption about concurrent isolation.

Each transaction in this pair updates only one table. A separate workload is needed to test atomic publication of changes to multiple updated tables.

#### Evidence in the history

Record the run/round ID, mode (concurrent or sequential control), transaction ID, table identifiers and UUIDs, baseline schemas/IDs, exact requirements and updates, and unique added fields. Pair invocations and completions by transaction ID; use the existing `process`, `time`, `status`, and outcome fields. Keep the raw response or transport error, plus baseline and final load operations with their metadata and timing. These workload fields can live in `value`; complete HTTP capture and the extra schema parsing are implementation work, not capabilities of the current append recorder.

The checker must reject malformed evidence (for example, a completion without an invocation or two requests using different baselines) as a history error. A missing completion is an unknown outcome. Missing reads, a changed table UUID, or an unestablished recovery boundary make state-based checks inconclusive; do not invent a successful final observation.

#### Checker rules and unknown outcomes

For the concurrent pair, the serializable model permits at most one committed transaction. Whichever commits first changes a schema ID that the other requires. Either winner is allowed; this safety invariant does not require progress. Count rejected rounds separately so a run with no useful successful operations is visible.

Treat an `ok` as acknowledged commitment, a rejection as non-commitment, and `info` (including an invocation without a completion) as possibly either. A complete, uniquely attributable update visible through the catalog can establish that an unknown operation committed. Absence establishes non-commitment only after server-side work has settled, with trustworthy reads and no operation that could remove the evidence. A timeout or a fixed delay alone does not establish that boundary. Keep the original outcome and record any resolution separately.

| Evidence for the pair | Result for the at-most-one invariant |
| --- | --- |
| Both acknowledged, or otherwise proven committed | Violation of the serializable model |
| One acknowledged, the other rejected; expected effects agree | No violation observed |
| One acknowledged, the other's unknown outcome is proven not committed | No violation observed |
| One acknowledged, the other's unknown outcome is unresolved | Inconclusive; it might have committed too |
| Both unknown | Resolve from evidence where possible; otherwise inconclusive |

Independently flag an acknowledged update missing from a settled table, or a rejected operation's unique effect being present. Inspect the full expected update, not only the schema ID. An acknowledged commit remains an acknowledgment even when its effect is missing: report the missing effect rather than reclassifying it as a rejection to make the pair pass. Malformed requests, unsupported endpoints, and authentication/setup errors must not count as successful isolation tests.

Report the checker verdict separately from bug confirmation. A proven concurrent pair of commits is an **isolation lead** unless the tested catalog's cited guarantees establish the serializable model, as discussed above. Failure of the sequential stale-requirement control, lost acknowledgments, or rejected effects can establish their respective contract violations directly. `VALID` must mean only that the named invariant passed for the fully checked rounds; report inconclusive rounds separately, and do not print an unqualified `VALID` for a run containing them.

#### Checker validation

Before real-catalog results are trusted, extend `fakecatalog` and test:

- Correct validation and commit under one lock: either transaction can win, and the sequential controls reject the stale request.
- A planted race that validates both requests before either writes and skips later checks of requirement-only tables: the pair is detected as a serializable-model violation.
- Ignored stale requirements on unchanged tables, lost acknowledged schema changes, and rejected requests that nevertheless apply: each produces its own finding.
- An operation applied before its response is lost, and one that never applies: evidence resolves each outcome correctly without treating the transport error as a rejection.
- Unresolved or late operations, unavailable final reads, and invalid histories: report inconclusive results or history errors as appropriate. An unresolved unknown must not become a definite anomaly or a definite pass by assumption.

[schema-updates]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L2748-L2782

## Faults

TODO: Toxiproxy (latency, reset, partition) between client and catalog and between catalog and Postgres; kill, pause, restart of the catalog process; every schedule derived from one seed.

## Open questions

- TODO
