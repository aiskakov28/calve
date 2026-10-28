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

The append CLI saves version 1 JSON Lines (`internal/history`). The first record has `type: "meta"`, `f: "run"`, and a `value` containing the run ID (the generated table name), UTC start time, workload, catalog/warehouse, namespace/table, client count, operations per client, and request timeout in nanoseconds. It excludes the bearer token. This is followed by paired invocation/completion records for configuration, table setup, a baseline load, every append, and the final load.

| Field | Meaning |
| --- | --- |
| version | format version; emitted records use 1 |
| id | positive operation ID shared by invocation/completion; absent on run metadata |
| index | position in the history |
| process | logical client; -1 is the coordinator |
| type | meta, invoke, ok, fail, info (unknown) |
| f | operation name |
| phase | setup, baseline, workload, or final |
| round | optional workload round identifier; reserved for future workloads |
| value | workload-specific payload |
| time | nanoseconds since run start |
| request | method, URL, headers and body on the invocation |
| response | status, headers and body on the completion, if a response arrived |
| status | HTTP status, 0 if no response |
| error | error text for fail and info |

`request.body` and `response.body` are base64-encoded bytes, not JSON values. This preserves HTML errors, invalid UTF-8, partial responses, and full 64-bit metadata values. Go's JSON decoder restores them as byte slices. Status and partial data survive body-read errors, and their completion remains `info`, including when the status was 200. The reader accepts legacy unversioned records but rejects unknown versions; legacy files lack the new evidence. There is no fixed 16 MiB line limit. A missing completion is still unknown.

Recording uses snapshots of bodies, headers and values so later caller mutation cannot change saved evidence. It captures application-level HTTP requests/responses around `http.Client.Do`, not packet traces or transport-internal attempts. Automatic redirects are disabled by the default REST client so their statuses remain observable. Credentials are redacted from known sensitive headers, URL userinfo/query fields, known credential keys in valid JSON, and configured bearer-token occurrences in string values or non-JSON bodies; redacted snapshots have `redacted: true`. Unchanged bodies retain their original bytes. This is targeted redaction, not detection of arbitrary secrets in server output; callers must keep workload labels and custom fields free of credentials. If redaction removes model-relevant evidence, the artifact is insufficient for that check.

The CLI saves the accumulated history before returning setup, baseline, final-read, or checker errors; a save error is reported alongside the original error. It checks that baseline and final table UUIDs agree before judging append durability. A failed final load or changed table identity produces an error, never `VALID`. A complete append artifact can be rechecked without a catalog: load the records with `history.ReadJSONL`, parse the successful final `load-table` response with `rest.ParseMetadata`, obtain keys with `workload.AppendKeys`, and pass the saved operations and keys to `checker.CheckAppend`. Require the baseline/final identities and successful final observation; a missing/failed observation is not an empty table.

This implementation still accumulates records in memory and saves at run completion or normal error return. It does not guarantee persistence after a hard kill, save host/container image versions automatically, resolve unknown commits, or implement future fault records. Streaming checkpoints and fault/run-manifest extensions remain future work. The skew library uses the same recorder and supplies the workload labels described below; its caller is responsible for saving the history, including on errors. The smoke command does not write a history artifact.

## Workloads and checkers

See [Elle and Porcupine: what Calve needs](checker-background.md) for the source-based checker background and implementation choices.

| Workload | Checker | Anomalies |
| --- | --- | --- |
| append | `checker.CheckAppend` | lost, phantom-failed, duplicate, unexpected |
| write skew | `checker.CheckSkew` | conflicting commits under the serializable model; stale requirements accepted; acknowledged changes lost or rejected changes applied |
| compare-and-set | Planned: Porcupine model ([#18](https://github.com/aiskakov28/calve/issues/18)) | non-linearizable history |
| multi-table general | Deferred: dependency graph + cycle search ([#19](https://github.com/aiskakov28/calve/issues/19)) | G0, G1c, G2 cycles |

Porcupine requires an explicit single-table model before use. Multi-table checking starts with workload-specific invariants; general cycle analysis is optional if time allows, as scoped in issue #19. Porcupine and general cycle analysis remain future work.

### Write-skew workload and checker

The workload in [#8](https://github.com/aiskakov28/calve/issues/8) is implemented by `workload.RunSkew`, and [#9](https://github.com/aiskakov28/calve/issues/9) supplies `checker.CheckSkew`. The `calve skew` command remains planned in [#10](https://github.com/aiskakov28/calve/issues/10). The concurrent probe follows [apache/iceberg#18552][skew-report]. It tests one specific invariant, not general multi-table serializability.

`RunSkew(ctx, client, SkewConfig{Namespace: "calve", Rounds: 10}, recorder)` returns execution details and an error, not a validity verdict. It generates a unique run ID, configures a private client copy, and runs the requested concurrent rounds followed by one sequential control in each direction. All tables have fresh names and remain available for inspection; existing tables are never dropped. The caller must save `recorder.Ops()` on both success and error. An opt-in `TestSkewLiveCatalog` exercises this API and saves JSONL; see the README for its environment variables. `checker.CheckSkew(recorder.Ops())` returns the checker result independently of catalog access.

#### Workload rounds

1. Create two tables, A and B, with names unique to the run and round. While no writers are active, load their UUIDs, schemas, and current schema IDs `(a0, b0)`. A failed setup or baseline read makes the round inconclusive, not valid.
2. Construct both requests from that same baseline. Every table change includes `assert-table-uuid` and `assert-current-schema-id`. T1 updates A and includes a requirement-only change for B (`updates: []`); T2 does the reverse:

   | Transaction | Requirements | Updates |
   | --- | --- | --- |
   | T1 | A is `a0`; B is `b0`; both UUIDs match | Add a schema to A and make it current; B unchanged |
   | T2 | A is `a0`; B is `b0`; both UUIDs match | Add a schema to B and make it current; A unchanged |

3. Use valid, distinct added fields that identify the round and transaction. The [schema updates][schema-updates] are `add-schema` followed by `set-current-schema` with `schema-id: -1`. The added schema explicitly proposes `max(schema-id) + 1` over the target table's full baseline schema history, matching the [Java client's allocation][java-schema-ids]; using only the current ID can collide with a historical schema. Fail preparation if the signed 32-bit schema ID space is exhausted. Check the resulting schema contents rather than assuming which ID the catalog ultimately uses. The new current schema must differ from the baseline. No other writers, schema reversions, or maintenance operations are allowed in these rounds. See the [Nessie schema-ID triage note](triage-nessie-schema-id.md) for the compatibility failure that motivated explicit IDs.
4. Release the two clients through a barrier. Each sends exactly one `CommitTransaction`, with no client retries or refreshed requirements. Record invocation/completion intervals: a barrier encourages overlap but does not guarantee it. Report whether the recorded intervals overlapped. Start with fault-free rounds; later fault tests use the same model.
5. After both attempts finish, load both tables again. The current fault-free workload records these as observations, checks table identity, and preserves unknown outcomes as unresolved. Future fault runs must heal faults and establish that server-side work has settled before treating absence as non-commitment. Preserve the observations and histories before cleanup. Fresh table names prevent a late operation from contaminating later rounds; they do not prove that a timed-out operation has stopped.

Also run a **sequential stale-requirement control**, on fresh tables in each direction. Commit T1, verify A's new schema, then invoke the original T2 requiring `a0`. T2's requirement on A is now false even though its A change has no updates. Expect HTTP 409 and no T2 effect on B. If the first commit or its verification is uncertain, the control is inconclusive. Accepting the stale requirement in this controlled history is a direct contract violation without an assumption about concurrent isolation.

Each transaction in this pair updates only one table. A separate workload is needed to test atomic publication of changes to multiple updated tables.

#### Evidence in the history

The run starts with `meta`/`run` settings, including the run ID, requested concurrent rounds, two controls, endpoint/warehouse (redacted), namespace, and request timeout. Each `meta`/`skew-round` header names the two tables and its mode (`concurrent`, `control-a-first`, or `control-b-first`). Every HTTP invocation/completion is paired by `id` and labelled with `round` and `phase`: `setup`, `baseline`, `workload`, `control-verify`, or `final`. Coordinator calls use process -1; transactions T1/T2 use 0/1.

Transactions use `f: "skew-txn"`; their `SkewTxn` value contains the run ID, mode, transaction label, target identifier, and unique added field. The exact requirements and updates, including the other table's `updates: []`, are in `request.body`. Baseline and subsequent `load-table` response bodies retain both UUIDs and complete schema metadata. The REST metadata view additionally exposes `last-column-id` and raw `schemas`, so schema contents survive without assuming catalog-assigned IDs. Baselines must describe the fresh one-column table before either request is constructed.

On normal success or error return, `meta`/`skew-round-result` records summarize every started round. Each `SkewRound` includes attempt presence, operation IDs, HTTP statuses/outcomes, interval overlap calculated from the recorded invocation/completion timestamps, whether a sequential control was established, a skipped-control reason, unresolved commit state, and execution errors. These are execution facts, not checker verdicts. A sequential control only sends its second request after an acknowledged first commit and a read verifying the expected new current schema and unchanged UUID. Unacknowledged or missing first effects skip the stale attempt; failed verification reads stop the run with retained evidence. Failed/malformed baseline or final reads and changed table UUIDs also return errors. A successful final read never clears `unresolved` for an `info` attempt. Unknown-commit resolution remains separate work.

The checker must reject malformed evidence (for example, a completion without an invocation or two requests using different baselines) as a history error. A missing completion is an unknown outcome. Missing reads, a changed table UUID, or an unestablished recovery boundary make state-based checks inconclusive; do not invent a successful final observation.

#### Checker rules and unknown outcomes

`CheckSkew(ops []history.Op) (SkewResult, error)` accepts one version-1, fault-free `RunSkew` history. It derives facts from HTTP invocations/completions, declared round tables, and baseline/verification/final responses; execution summaries cannot override that evidence. It validates operation pairing, ordered timestamps/indexes, table identity, catalog origin/routing, fresh schemas, matching guards, and the exact opposing schema updates. Duplicate attempts, orphan completions, mismatched baselines, unsupported operations, and malformed transaction evidence are history errors. Missing completions remain `info`. Missing or malformed observations and incomplete setup make checks inconclusive. Redaction of model data makes evidence unavailable; redacted credential headers alone do not.

`SkewResult.Verdict` and each round's verdict are `valid`, `invalid`, or `inconclusive`. `Valid` is true only for a full pass across the declared rounds and both controls. Proven findings take precedence over incomplete checks, whose reasons and `InconclusiveRounds` remain visible. `OK`, `Failed`, and `Unknown` count original outcomes; `ResolvedCommitted` counts unknowns separately established as committed. `RejectedRounds` exposes rounds with two rejections without imposing a progress guarantee. Each transaction reports its original outcome, operation ID, commit state, and resolution source. `OverlapKnown` distinguishes complete intervals from a missing completion; overlap is not required to establish the pair's at-most-one invariant.

Findings carry `Round`, `OperationIDs`, `Kind`, and `Basis`. `write-skew` has basis `serializable-model` and is an isolation lead; `stale-requirement`, `lost`, and `phantom-failed` have basis `catalog-contract`. Sequential findings require the first acknowledgment and a matching full-schema verification before the stale attempt. State checks require matching table UUIDs and observations after the attempts. A missing effect is called `lost` only when recorded commit outcomes are definite, so no unknown server work is assumed to have stopped.

The checker compares complete schema contents while allowing catalog-assigned schema IDs and equivalent absent/empty identifier-field lists. It preserves 64-bit JSON numbers. A full expected current schema with a changed schema ID can establish that an unknown committed; a historical schema alone or a partial update cannot. Its result retains the original `info` outcome. Negative resolution from absence and recovery/fault boundaries are not implemented here and remain issue #16; no sleep or final-read success substitutes for them. The checker adds no outcome-resolution records to the original history.

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

Report the checker verdict separately from bug confirmation. A proven concurrent pair of commits is an **isolation lead** unless the tested catalog's cited guarantees establish the serializable model, as discussed above. Failure of the sequential stale-requirement control, lost acknowledged changes, or rejected effects can establish their respective contract violations directly. `VALID` must mean only that the named invariant passed for the fully checked rounds; report inconclusive rounds separately, and do not print an unqualified `VALID` for a run containing them.

#### Checker validation

The checker tests serialize histories, stop the fake, and replay them offline. `fakecatalog` normally validates, stages, and publishes under one lock. Opt-in bugs and the tests cover:

- Correct validation and commit under one lock: either transaction can win, and the sequential controls reject the stale request.
- A planted race that validates both requests before either writes and skips later checks of requirement-only tables: the pair is detected as a serializable-model violation.
- Ignored stale requirements on unchanged tables, lost acknowledged schema changes, and rejected requests that nevertheless apply: each produces its own finding.
- An operation applied before its response is lost, and one that never applies: complete positive evidence resolves commitment; absent effects remain unknown without an established recovery boundary.
- Unresolved or late operations, unavailable final reads, and invalid histories: report inconclusive results or history errors as appropriate. An unresolved unknown must not become a definite anomaly or a definite pass by assumption.

`Bugs.WriteSkew` deliberately separates validation from publication and rechecks only updated tables. `AfterTransactionValidation` runs outside the fake's mutex and lets tests hold both validated requests before either publishes; sequential controls still reject stale read requirements. Other switches ignore read-only guards, lose acknowledged changes, reject applied transactions, or return unknown responses with/without applying changes. These modes are off by default. Tests also exercise skipped controls, partial/historical effects, missing completions, invalid request evidence, unavailable observations, forged summaries, and redaction.

Offline replay of the saved schema-ID-fix histories found ten `write-skew` leads and passing controls on reference 1.10.1; Lakekeeper v0.14.2 and Nessie 0.109.0 passed all twelve rounds. The earlier Nessie history containing only HTTP 400 transaction rejections was inconclusive, with no isolation findings. Artifacts are listed in the [Nessie triage note](triage-nessie-schema-id.md); `TestCheckSkewSavedHistory` can replay them using the README's environment variables.

[schema-updates]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L2748-L2782
[java-schema-ids]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651

### Reference catalog reproduction (issue #3)

On 2026-10-27 (UTC), ran the Python code block from [apache/iceberg#18552][skew-report] unchanged against the local reference catalog. The client was Python 3.14.5 in Windows PowerShell; the catalog was the Linux amd64 `apache/iceberg-rest-fixture:1.10.1` container. No faults were injected. The recorded image ID/repository digest was `sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f`.

The script exited successfully and reported:

```text
both transactions committed in 30 of 30 rounds; status codes {200: 60}
```

The script counts acknowledgments, so we also saved and inspected the final table metadata. Both `skew.a` and `skew.b` retained 31 schemas (the initial schema plus 30 new schemas). All 30 expected round markers were present for each table; the current schemas had ID 30, with fields `id, c60` on A and `id, c61` on B. This verifies that all 60 schema changes were retained, in addition to the successful responses.

A relevant limitation is explicit in the fixture's [transaction adapter][fixture-transactions]: its simplified implementation does not guarantee true transactional atomicity. It stages/validates table changes and then commits the individual table transactions separately; [unchanged transactions return without committing][unchanged-transaction]. Code inspection is consistent with the reported validation gap, but this run did not trace server internals. The observed behavior violates the serializable model described above; reproducing it does not establish a new upstream finding or resolve the catalog-contract question.

Local evidence is in `results/iceberg-18552/20261027215521Z/` (ignored by Git): the upstream script and source reference, stdout, run metadata/script hash, both final table responses, and marker verification. That experiment used the standalone Python client before the Calve skew workload was implemented. Other catalogs and fault scenarios were not tested. By agreement between the collaborators, this single clear local reproduction is sufficient to complete [issue #3](https://github.com/aiskakov28/calve/issues/3); a second independent run is not required. Race frequency can vary between runs.

[fixture-transactions]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java#L545-L573
[unchanged-transaction]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/BaseTransaction.java#L354-L358

## Faults

Real fault injection is planned in issues [#13](https://github.com/aiskakov28/calve/issues/13) and [#14](https://github.com/aiskakov28/calve/issues/14). Issue [#15](https://github.com/aiskakov28/calve/issues/15) now has a seeded planner, controller-independent executor, and explicit `calve run --seed N --plan-only` command; real backends and campaign integration remain pending. See the [scheduler/controller contract](fault-scheduler.md). Begin with one catalog and one fault at a time, with a no-fault control for each workload/catalog configuration. Combined faults can follow once individual faults and their cleanup are verified.

### Scope and routing

Route all workload REST requests through one Toxiproxy proxy per catalog: Calve clients -> Toxiproxy -> catalog. This includes baseline loads, commits, and final reads; remove the toxics before final reads rather than silently changing the route. Keep the Toxiproxy management API and Docker control connection outside the affected path so the controller can heal it. Initially all clients of that catalog share the affected path; independently partitioning selected clients is deferred.

Network faults affect only client-to-catalog traffic. Catalog-to-Postgres faults are deferred per issue #13 because each catalog needs different database wiring. Catalog-to-object-storage faults, database-server crashes, and host/storage failures are also outside the initial fault model. Process faults target only the named catalog container in Calve's test stack, leaving external database and object-storage services running; an embedded backend shares the catalog process's fate.

### Fault types and healing

The network mechanisms come from the [Toxiproxy documentation][toxiproxy-faults]. Record direction explicitly: `upstream` is client to catalog, and `downstream` is catalog to client. Each injected toxic has a unique name tied to the run and fault ID.

| Fault | Planned mechanism | Healing |
| --- | --- | --- |
| Latency | `latency` toxic with a fixed delay on the selected stream | Remove that toxic |
| Connection reset | `reset_peer` toxic on the selected stream | Remove the toxic; later requests may establish new connections |
| Partition | `timeout` toxics with `timeout: 0` on both streams, blocking data for a bounded interval | Remove both toxics; interrupted requests retain their recorded outcomes |
| Abrupt process death | [Docker kill][docker-kill] with explicit `SIGKILL`, then a scheduled interval stopped | Start the same container with its existing storage |
| Process pause | [Docker pause][docker-pause] for a scheduled interval | Unpause the same container |
| Controlled restart | Stop/start the same container with a recorded stop signal and grace period | Verify it is running and the existing tables are readable |

For the initial network faults, use `toxicity: 1` and latency `jitter: 0`; the schedule chooses the timing and parameters, without extra probabilistic choices inside Toxiproxy. A reset and a partition exercise different failure modes; do not substitute disabling a proxy for the specified blocked-data partition without labeling it as a different fault. Sending a request without receiving its response can leave a commit applied in either case.

Record automatic container restart policies and disable them for controlled process-fault runs, so they do not end the planned outage early. A [normal Docker restart][docker-restart] allows a stop signal and grace period; it must not be reported as a `SIGKILL` crash unless that was the action actually used. Confirm container state after each control action instead of inferring it from a planned event.

Before durability experiments, verify that catalog metadata and warehouse files survive the selected process fault. The [1.10.1 fixture Dockerfile][fixture-storage] configures SQLite at `/tmp/iceberg_catalog.db`; our current Compose file does not mount persistent volumes. Verify the running image's effective backend configuration and preserve the same container/storage during kill/start and restart tests. Recreating containers, deleting volumes, resetting the catalog, or using an intentionally in-memory backend is not evidence of a durability violation under this process-fault model.

### Seeded schedules

The implemented plan-only command generates and saves the complete fault plan; library execution saves it before starting load or invoking controllers. With the same Calve/scheduler version, seed, and normalized configuration, it produces the same ordered plan. Each event records a fault ID, target, kind, parameters/direction, start offset, duration, and healing action. Targets and kinds are sorted before selection, and the generator uses a private PCG stream so workload randomness cannot shift the fault plan. Version `calve-faults-v1-pcg` globally serializes faults and reserves a control timeout after each planned heal. The full `calve run` campaign still requires real controllers, workload coordination, and recovery integration.

Offsets use a monotonic origin at the recorded workload-start boundary, after caller-owned setup and baseline reads. Healthy warmup is included in the saved plan. Fault windows do not overlap, and each has a healing action within the configured limits. The executor confirms controller healing before the next injection and stops on missed deadlines. It saves the seed, generated plan, scheduler version and timing configuration; full campaign integration must additionally save Calve revision, workload parameters, request/recovery timeouts, image tags/digests, effective configuration without credentials, and storage/restart policies. Controller-state healing does not by itself establish catalog readiness or completion of prior commits.

The seed reproduces the planned schedule, not thread timing, proxy buffering, catalog execution, or the occurrence of a failure. Record late actions and their actual invocation/completion times; do not silently redraw events or claim they happened at their planned offsets. Controller failures or uncertain control responses stop new injections and trigger reconciliation/cleanup. Report failed or unverified injections as harness errors, not catalog anomalies or successful fault coverage. Record which fault windows overlapped workload operations.

### Fault history and recovery

Use a reserved logical controller process in the same history clock. Record `f: "fault"` invocations and completions with the fault ID, apply/heal action, target, parameters, planned offset, control response/error, and observed target state. An acknowledgment records the control action, not a catalog commit; workload checkers must distinguish these operations. For a confirmed action, the control invocation/completion bounds the state change rather than establishing an exact instant of failure. An uncertain control result provides no such bound and requires reconciliation. Preserve the original workload outcomes and record later outcome resolution as additional evidence.

Run completion and cancellation follow this order:

1. Stop scheduling new faults and issuing new workload writes. Bound the wait for outstanding controller actions and reconcile their state so a late apply cannot reintroduce a fault after cleanup. If that cannot be established, record a harness error and still attempt cleanup; do not wait indefinitely before healing.
2. Remove the run's toxics, restore its proxies, unpause paused targets, and start stopped catalog containers. Cleanup is idempotent and uses its own bounded context, so cancelling workload requests does not cancel healing. Inspect all fault IDs from the saved plan, including uncertain applies; do not rely only on the list of acknowledged actions.
3. Verify control state and catalog readiness, then wait for outstanding requests and server-side work to settle within a recorded recovery deadline. `/v1/config` success proves reachability, not completion of prior commits. Load the existing test tables and verify identity; do not recreate missing tables to make recovery appear successful.
4. Once a trustworthy recovery boundary exists, record final table loads and run the checkers. Save history, plan, actual fault events, relevant service logs, and verdicts before cleaning up test data, including on errors or cancellation.

Unknown-outcome handling follows [issue #16](https://github.com/aiskakov28/calve/issues/16): timeouts, resets, and 5xx remain `info` until evidence resolves them. A delay, repeated identical reads, or a healthy endpoint alone does not prove that an outstanding server operation cannot commit later. Use a documented per-catalog drain/completion mechanism where available; otherwise keep absence-based conclusions involving such operations inconclusive. Cleanup/recovery failures also make affected checks inconclusive. Preserve any independently established violation, but never turn missing recovery evidence into a lost-commit finding or an unqualified `VALID`. Report availability and recovery timings separately from correctness findings.

### Validation before fault campaigns

Test the fault controller against a local service before trusting catalog experiments: verify directional delay, connection resets, blocked traffic, and restored connectivity; inspect container pause/stop/running transitions for process faults. Check that the same seed/configuration produces the same plan, that workload randomness cannot change it, and that cancellation or a failed control action still attempts complete cleanup. A correct catalog with responses lost before or after commit must not produce a false anomaly solely from the transport failure. Planned delays, counts, and recovery limits are explicit run parameters; choose and record them before a campaign rather than treating one timeout value as proof of completed recovery.

[toxiproxy-faults]: https://github.com/Shopify/toxiproxy#toxics
[docker-kill]: https://docs.docker.com/reference/cli/docker/container/kill/
[docker-pause]: https://docs.docker.com/reference/cli/docker/container/pause/
[docker-restart]: https://docs.docker.com/reference/cli/docker/container/restart/
[fixture-storage]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/docker/iceberg-rest-fixture/Dockerfile#L36-L41

## Open questions

The first two questions affect whether evidence supports a confirmed bug. The remaining questions concern implementation and coverage. Recording them here does not imply that the remaining workloads, history extensions, or fault controller already exist.

### What multi-table isolation does each catalog promise?

Does requirement validation, including requirements on tables with `updates: []`, participate in one serializable operation with the transaction's updates?

- **Current policy:** report a conflicting concurrent pair as an isolation lead until a cited guarantee establishes the model. Keep the sequential stale-requirement control as a separate direct contract check.
- **Resolution:** obtain version-specific catalog documentation or upstream clarification, record the scope of the guarantee, and check that the workload supplies every required validation. A reproduction alone does not resolve the contract question. Use this evidence when assessing the gate in [issue #12](https://github.com/aiskakov28/calve/issues/12).

### What establishes a trustworthy final state after an unknown commit?

For each catalog, how can Calve establish that outstanding operations have completed or cannot commit later, and what freshness does the final table load guarantee?

- **Current policy:** retain the original `info` outcome; resolve it only from attributable evidence. Keep absence-based checks inconclusive when completion or read freshness is unestablished. Health checks, fixed waits, and repeated identical reads are not completion barriers.
- **Resolution:** document and validate a per-catalog completion/drain mechanism or another justified observation boundary as part of [issue #16](https://github.com/aiskakov28/calve/issues/16). Include a test where an operation commits after the client times out, so an early final read cannot incorrectly resolve it as rejected.

### What artifacts are required to recheck a run offline?

What versioned schema joins workload operations, exact requests/responses, baseline and final metadata, fault events, and run configuration without needing a live catalog?

- **Current policy:** version 1 append JSONL contains a run record, paired HTTP evidence and baseline/final observations, sufficient for offline rechecking of a complete append run. Exclude credentials from saved artifacts. Unknown outcomes and failed reads retain their original classification.
- **Resolution:** [issue #7](https://github.com/aiskakov28/calve/issues/7) establishes and tests the append format. Extend it deliberately for skew payloads, fault identifiers/plans, catalog image versions, and crash-resistant persistence as those features are implemented. These extensions remain open; the append artifact does not imply they already exist.

### When and how do we add real snapshot/data-file appends?

What is the smallest valid snapshot, manifest, and data-file setup needed to exercise the append path and observe duplicate occurrences or lost snapshots?

- **Current policy:** use property updates for the initial commit checks and keep the first gate focused on the write-skew probe and its controls. State that the current property workload cannot establish duplicate data-file behavior or coverage of the full append path.
- **Resolution:** choose the fixture/file-generation approach, define how final catalog-reachable metadata exposes each operation's effect, and validate the extended checker against planted duplicate and lost-snapshot bugs before claiming that coverage. This is later workload work, not a prerequisite for completing this design document.
