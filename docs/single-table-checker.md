# Single-table snapshot linearizability (issue #18)

`checker.CheckSnapshotHistory(ops, timeout)` runs Porcupine v1.3.1 against the
sequential model named in the run manifest, normally `calve-snapshot-v1`.
It checks recorded HTTP evidence offline.
The CLI accepts the same history:

```sh
go run ./cmd/calve check-table --timeout 5s snapshot-history.jsonl
```

Exit codes are 0 valid, 3 invalid, 4 inconclusive, and 1 for malformed or
unsupported evidence. The search budget must be positive; Porcupine exhaustion
is inconclusive. An invalid result identifies the entire history's operation
IDs, not a minimized counterexample.

## Sequential model

The state is one v2 table's UUID, last sequence number, current main snapshot,
and the retained snapshots' complete descriptors. Each successful commit
atomically:

1. Checks `assert-table-uuid` and `assert-ref-snapshot-id` on `main`.
2. Adds one previously unused snapshot with a strictly larger sequence number.
3. Moves `main` and `current-snapshot-id` to that snapshot.

Supported requests contain exactly those two requirements and, in that order,
`add-snapshot` and `set-snapshot-ref` updates. The new snapshot's parent must
match the expected main snapshot; null means that main must not exist.
Snapshot IDs must be unique across all attempts and absent from the baseline.
New descriptors explicitly include `snapshot-id`, `sequence-number`,
`timestamp-ms`, `manifest-list`, `schema-id`, and an append summary. Explicit
schema IDs avoid confusing server-supplied defaults with changed effects.

Reads return the modeled projection of metadata. A successful commit's returned
metadata must match its resulting state too. Retained snapshot order is
irrelevant; descriptor contents and 64-bit IDs are preserved. Missing/null/-1
current snapshot IDs represent an empty table. Main is implicit when `refs` is
absent/null (an empty map is also normalized); explicit main references must
match the current snapshot. Reference retention defaults do not affect this
projection.

Sequence numbers need not be consecutive. The pinned Java metadata builder
accepts an increase larger than one; Calve must not label that accepted input
as a catalog violation.

A rejection makes no changes. The model permits rejection under internal
contention even when the abstract guard holds; it does not test progress or
require every possible commit to succeed.

The model is pure: each transition copies snapshot storage, and all future
states leave earlier search states intact.

### Explicit current-snapshot profile

`calve-snapshot-current-v1` has the same guarded atomic transition, sequence
rules, unique IDs and real-time ordering. Observations compare the UUID, last
sequence, current snapshot ID, main reference, and the complete current
descriptor **except its parent ID**. It does not check historical snapshot
retention or returned parent lineage. Requests still require a parent matching
their main-ref guard. Unknowns require a complete matching current observation
under this projection to resolve as committed; historical descriptors and
absence are insufficient.

Select this profile explicitly with `calve snapshot --model
calve-snapshot-current-v1`. The selected version is recorded before the baseline
and used unchanged by offline replay. The default model retains its complete
descriptor/history comparison; neither model is selected by catalog URL.

Nessie [documents returning only the current snapshot](https://projectnessie.org/guides/iceberg-rest/#time-travel-with-iceberg-rest)
as intentional. Its pinned 0.109.0 responses also omit that descriptor's parent
ID. The Compose runner therefore chooses the current profile for Nessie. A pass
there covers current-head CAS behavior, without a claim about retained lineage.

## Evidence contract

The caller must exclusively own the table, finish setup and any earlier server
work before recording, and record **every** request during the checked window.
The checker cannot discover unrecorded outside writers or establish that setup
has drained. Begin a new recorder after setup. Do not slice an existing history
or rewrite its outcome/timestamp fields to make it fit.

The first version-1 record is `type: "meta", f: "run"`. Its value is a
`checker.SnapshotManifest`, for example:

```json
{
  "run_id": "snapshot-example",
  "workload": "snapshot-cas",
  "model": "calve-snapshot-v1",
  "catalog": "http://localhost:8181",
  "namespace": "calve",
  "table": "snapshot_example",
  "commits": 2
}
```

Use `rest.Client.WithRecorder` and `history.WithOperation` to record calls.
The first REST call is `load-table` with phase `baseline`. It must complete
successfully before any workload call. Later operations are:

| Operation | Method | Phase |
| --- | --- | --- |
| `commit-table` | POST | `workload` |
| `load-table` | GET | `workload`, `final`, or `recovery` |

They all target exactly the same table URL, including the negotiated prefix,
under the manifest's catalog root. Setup/config calls are outside this window;
configure the client before recording. No extra round or derived value labels
are used. Operation IDs pair invocations and completions; labels and statuses
must agree, record indexes are contiguous from zero, and the recorder clock
never moves backward. Successful operations require HTTP 200 metadata.

The baseline can be empty or contain an existing main snapshot and retained
snapshots. A final/recovery GET must succeed after the recorded attempts;
missing attempts, missing final evidence or failed final reads prevent an
unqualified pass until usable recovery evidence is available. Failed/pending
reads supply no observable value and no state constraint; a successful later
recovery read can restore coverage. Redaction in storage credentials, properties
or other fields outside the model is permitted. Redaction of a UUID, snapshot
descriptor or modeled reference makes that evidence unusable.

A changed UUID ends the original table's observation window. Search uses only
records before the invocation that first observes a different UUID; commits
crossing that boundary remain pending in the prefix even if they later finish.
Later table incarnations cannot create findings against the old state machine.
The whole run stays inconclusive unless the earlier prefix already proves a
violation. Finding evidence IDs name only that checked prefix; raw commit
outcomes/counts still describe the full recorded run.

The adapter uses the recorder's call/return **event order**. This preserves
real-time precedence even when monotonic timestamps tie, while overlapping
requests may be linearized in either order. A multi-table transaction is
refused, never split into single-table histories.

## Unknown outcomes

Timeouts, incomplete responses, 5xx and missing commit completions preserve
their original `info` outcome. Their possible execution intervals extend
through the end of the recorded history. Porcupine explores either no effect
or the complete atomic update at any point after invocation; it does not force
publication to precede a client's timeout. This permits a read of old metadata
after a timeout followed by the newly committed snapshot in a later read.

An exact complete observation of an attempt's uniquely identified snapshot at
main can resolve it as committed. The result retains its raw outcome and lists
supporting operation IDs. A historical descriptor alone does not resolve the
main-ref update. Absence never proves rejection: an unresolved unknown makes
an otherwise legal history inconclusive, including at the end of a read-back
budget. A lack of any legal ordering is invalid even with unresolved peers.

These are model-relative findings, requiring the evidence assumptions above and
spec/catalog triage before an upstream bug claim. A search timeout is not a
pass, and a successful bounded run is not a proof for all executions.

## Validation and scope

The correct fake catalog runs two rounds of four concurrent CAS attempts with
one winner each. Planted modes cover accepted stale guards, lost acknowledged
updates, partial publication, rejected effects, and stale reads. Additional
histories cover two overlapping winners, forgotten ancestors, real-time ordering
versus overlap, large IDs, implicit main, sequence gaps, recovered reads, scoped
credential redaction, table-identity boundaries, malformed evidence, and a search
timeout. A real HTTP test lets server work outlive a client timeout
and an early final GET, then records the later complete effect. Offline replay
continues after all fake servers stop, without resending commits.

Try the **synthetic** examples (these are not live-catalog findings):

```sh
go run ./cmd/calve check-table internal/checker/testdata/snapshot-valid.jsonl
go run ./cmd/calve check-table internal/checker/testdata/snapshot-invalid.jsonl
go run ./cmd/calve check-table internal/checker/testdata/snapshot-inconclusive.jsonl
```

`calve snapshot` now generates this evidence using real empty manifest lists;
the existing `append` and `run` commands still write properties. The snapshot
workload does not write data files and is not integrated with fault campaigns.

Other branches/tags, expiration, rollback, schema changes, v1/v3 metadata,
property updates, multi-table transactions and fault-controller histories are
outside this model. Unsupported operations are errors, not silently projected
away. This checker does not implement the schema workload in #20 or replace
the multi-table checker in #19.

## Live snapshot workload (#65)

```sh
go run ./cmd/calve snapshot --catalog "$CATALOG" --clients 4 --rounds 2 \
  --out history-snapshot.jsonl
go run ./cmd/calve check-table --timeout 5s history-snapshot.jsonl
```

The workload creates a randomly named, exclusively owned v2 table. Config,
namespace creation and table creation finish outside the checked window. The
recorded manifest and baseline GET start a new recorder; the baseline must be
empty. The workload never drops tables or reuses snapshot IDs.

Before each round it observes the current main snapshot and last sequence.
Each contender writes an immutable Avro v2 manifest-list object and proposes a
unique snapshot ID, explicit schema ID and sequence `last-sequence-number + 1`.
All contenders in that round use the same UUID/main-ref guard. Their POSTs start
together; each is sent exactly once, with no commit retries. Later rounds use a
fresh GET. All attempted snapshots have zero manifest entries and no data files.

A final GET follows the attempts. Unknowns keep their recorded outcomes;
bounded read-back adds only GETs (`--readback-timeout 5s` by default, `0` to
disable). A client's completion is not a server-side drain barrier. Failures
after recording starts save the available prefix; setup or argument errors
preserve any existing output file. Rejected attempts' manifest lists remain as
orphans for inspection, and successful tables remain available too.

Manifest files use the fresh table's `metadata.location` by default:

- Absolute paths and `file:` locations need a filesystem shared with the catalog.
  `--manifest-root` overrides the root when a shared storage location is needed.
- `s3:` locations need `--s3-endpoint`, `--s3-region` (default `us-east-1`) and
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; `AWS_SESSION_TOKEN` is optional.
  Uploads use path-style S3, SigV4 and `If-None-Match: *`. Catalog-vended storage
  credentials are not consumed. Secrets are excluded from history and CLI errors.

For the repository's Compose catalogs, start and initialize them as in the
[quick start](../README.md#quick-start), export `POLARIS_TOKEN`, then run:

```sh
CGO_ENABLED=0 go build -o bin/calve-snapshot ./cmd/calve
bash scripts/check-snapshots.sh
```

On Windows, build the Linux binary from PowerShell:

```powershell
docker run --rm --mount "type=bind,source=${PWD},target=/work" --workdir /work golang:1.24.7 sh -c 'CGO_ENABLED=0 go build -o bin/calve-snapshot ./cmd/calve'
```

Then use Git Bash in the repository, with `POLARIS_TOKEN` exported there:

```sh
bash scripts/check-snapshots.sh
```

The helper copies the binary into the reference fixture (its local filesystem)
and Silo (S3 uploads to the other catalogs), so no new host ports or shared
mounts are required. Set `COMPOSE_PROJECT_NAME` if using a named Compose project.
It runs two contenders for three rounds on every catalog, saves separate
`history-snapshot-*.jsonl` files and logs, then replays each history in a
**network-disabled** container. Invalid histories or replay disagreement fail
the gate. Inconclusive unknowns are allowed, but every catalog must show at least
one acknowledged or positively resolved commit. The reference fixture gets up
to three fresh-table runs for that coverage requirement; individual writes are
never retried. The same helper runs in CI and its histories/logs are uploaded.

The initial [live results and catalog-specific scope](snapshot-workload-results.md)
include the reference fixture's SQLite errors and Nessie's projection policy.
This workload does not scan data, produce nonempty manifests, expire snapshots,
or inject faults.

Sources: the pinned Iceberg [single-table concurrency and sequence rules](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L98),
[snapshot fields](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L742-L765),
[table metadata and implicit main](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L926-L956),
[REST commit and update schemas](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml),
and [Porcupine's model and event API at v1.3.1](https://github.com/anishathalye/porcupine/blob/v1.3.1/model.go).
The pinned Java [metadata builder's sequence validation](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1232-L1261)
permits sequence-number gaps.
The manifest-list writer follows the pinned Java
[v2 manifest schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/V2Metadata.java)
and [manifest-list metadata](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/ManifestListWriter.java)
in an [Avro object container](https://avro.apache.org/docs/1.11.4/specification/#object-container-files).
