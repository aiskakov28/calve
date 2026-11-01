# Snapshot workload and checker

The snapshot workload races guarded snapshot commits on one table. The checker
uses [Porcupine](https://github.com/anishathalye/porcupine) v1.3.1 to look for a
legal sequential order of every commit and read. If none exists, the history is
not linearizable.

```sh
calve snapshot --catalog "$CATALOG" --clients 4 --rounds 2 --out history-snapshot.jsonl
calve check-table --timeout 5s history-snapshot.jsonl
```

`check-table` exits 0 valid, 3 invalid, 4 inconclusive, and 1 for malformed or
unsupported evidence. If Porcupine runs out of its search budget (`--timeout`),
the result is inconclusive. An invalid result names the operations in the whole
history, not a minimized counterexample.

To see what each verdict looks like, try the synthetic examples (these are not
results from real catalogs):

```sh
calve check-table internal/checker/testdata/snapshot-valid.jsonl
calve check-table internal/checker/testdata/snapshot-invalid.jsonl
calve check-table internal/checker/testdata/snapshot-inconclusive.jsonl
```

## The workload

`calve snapshot` creates a randomly named v2 table that it alone uses. Config,
namespace and table creation happen before recording starts. Recording begins
with the run record and a baseline read, which must show an empty table. The
workload never drops tables or reuses snapshot IDs.

Each round:

1. Reads the current `main` snapshot and last sequence number.
2. Has every contender write an empty Avro v2 manifest list and propose a new
   snapshot with a unique ID, an explicit schema ID, and sequence number
   `last-sequence-number + 1`.
3. Releases all contenders at once. Each sends one commit with the same guard:
   the table UUID and `main` as it was read. No commit is retried.

A final read follows the last round, plus read-back if some commits are still
unknown (see [unknown outcomes](unknown-outcomes.md)). Manifest lists from
rejected attempts stay behind as orphans, and the tables stay available for
inspection. No data files are written: this tests snapshot publication, not
data.

Under `calve run --workload snapshot`, the same workload runs with faults (see
[fault campaigns](fault-campaigns.md)).

### Where manifest lists go

By default, manifest lists go under the fresh table's `metadata.location`.

- **Local paths and `file:` locations** need a filesystem shared with the
  catalog. `--manifest-root` overrides the root.
- **`s3:` locations** need `--s3-endpoint`, `--s3-region` (default
  `us-east-1`) and `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  (`AWS_SESSION_TOKEN` is optional). Uploads use path-style S3, SigV4 and
  `If-None-Match: *`. Credentials vended by the catalog aren't used, and
  secrets never appear in the history or in errors.

### Running it on the Compose catalogs

Start the catalogs as in the [quick start](../README.md#quick-start), export
`POLARIS_TOKEN`, then:

```sh
CGO_ENABLED=0 go build -o bin/calve-snapshot ./cmd/calve
bash scripts/check-snapshots.sh
```

The script copies the binary into the reference fixture, which keeps files on
its own filesystem, and into Silo, which serves S3 for the other catalogs. So it
needs no extra host ports or shared mounts. It runs two contenders for three
rounds on every catalog, saves `history-snapshot-<catalog>-<attempt>.jsonl`
and a log for each, then replays each history in a container with networking
disabled. Set `COMPOSE_PROJECT_NAME` if you use a named Compose project.

The script fails on an invalid history or if replay disagrees with the live
verdict. Inconclusive runs are allowed, but every catalog must show at least
one acknowledged or proven commit. The reference fixture gets up to three fresh
tables to reach that; individual commits are never retried. CI runs the same
script.

On Windows, build the Linux binary from PowerShell, then run the script from Git
Bash with `POLARIS_TOKEN` exported:

```powershell
docker run --rm --mount "type=bind,source=${PWD},target=/work" --workdir /work golang:1.24.7 sh -c 'CGO_ENABLED=0 go build -o bin/calve-snapshot ./cmd/calve'
```

## The model

The state is one v2 table: its UUID, last sequence number, current `main`
snapshot, and the full descriptors of the snapshots it retains. A successful
commit atomically:

1. checks `assert-table-uuid` and `assert-ref-snapshot-id` on `main`;
2. adds one unused snapshot with a strictly larger sequence number;
3. moves `main` and `current-snapshot-id` to it.

A rejection changes nothing. The model allows a catalog to reject a commit
whose guard still holds, for example under internal contention, because it
tests safety, not progress.

Requests must contain exactly those two requirements and then `add-snapshot`
and `set-snapshot-ref`, in that order. The new snapshot's parent must match the
expected `main` (null means `main` must not exist). Snapshot IDs must be unique
and absent from the baseline. Each new snapshot spells out `snapshot-id`,
`sequence-number`, `timestamp-ms`, `manifest-list`, `schema-id` and an append
summary, so a server-filled default can't be mistaken for a changed effect.

A few details the model gets right on purpose:

- **Sequence numbers can skip.** The Java metadata builder
  [accepts gaps](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1232-L1261),
  so a gap is not a violation.
- **An empty table** can show up as a missing, null or `-1` current snapshot ID.
- **`main` is implicit** when `refs` is missing, null or empty. An explicit
  `main` must match the current snapshot.
- **Snapshot order doesn't matter.** Descriptor contents and 64-bit IDs do.

Reads, and the metadata returned by a successful commit, must match the model's
state. The model is pure: every step copies its state, so Porcupine's search
never sees one branch change another.

### The current-snapshot model

`calve-snapshot-current-v1` keeps the same commit rules and real-time ordering
but only compares the UUID, last sequence number, current snapshot ID, `main`,
and the current snapshot's descriptor, ignoring its parent ID. It doesn't check
retained snapshots or lineage.

Nessie [returns only the current snapshot on purpose](https://projectnessie.org/guides/iceberg-rest/#time-travel-with-iceberg-rest),
and 0.109.0 also leaves that snapshot's parent ID out. So the Compose script
uses this model for Nessie. A pass there covers compare-and-set on the current
head, nothing about lineage.

Pick the model with `calve snapshot --model calve-snapshot-current-v1`. It is
recorded before the baseline and used unchanged on replay. Calve never chooses
a model from the catalog URL or relaxes one after a failure.

## Evidence rules

The workload must own the table exclusively, finish setup before recording
starts, and record every request in the checked window. The checker can't see
writers it wasn't told about. Don't slice an existing history or edit its
outcomes to make it fit.

The run record (`type: "meta", f: "run"`) holds a `checker.SnapshotManifest`:

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

The first recorded call is a successful `load-table` in phase `baseline`. After
it:

| Operation | Method | Phase |
| --- | --- | --- |
| `commit-table` | POST | `workload` |
| `load-table` | GET | `workload`, `final` or `recovery` |

Every call targets the same table URL, including the negotiated prefix.
Operation IDs pair invocations and completions, record indexes run from zero
without gaps, and the clock never goes backwards. Successful operations need
HTTP 200 metadata.

A run can't pass without a successful read after all attempts. A failed read
adds no information, but a later successful one can restore coverage.
Redaction outside the model, such as storage credentials, is fine; a redacted
UUID, snapshot descriptor or ref makes that evidence unusable.

**A changed UUID** ends the original table's window. The checker only searches
records before the first read that shows a different UUID, and commits that
cross that point stay pending. The run is inconclusive unless that earlier part
already proves a violation.

**Ordering** uses the recorder's call and return order, so real-time precedence
holds even when timestamps tie, while overlapping requests can be ordered
either way. Multi-table transactions are refused.

## Unknown commits

A timed-out, cut-off or 5xx commit can take effect at any point after it was
sent, up to the end of the history. Porcupine tries both: no effect, or the
complete commit at some point. That allows, for example, a read of old metadata
after a timeout followed by a later read showing the new snapshot.

Seeing the commit's unique snapshot complete at `main` proves it committed. A
historical descriptor alone doesn't, since it doesn't show the `main` update.
Absence never proves rejection, so an otherwise legal history with an
unresolved unknown is inconclusive. If no legal order exists at all, the
history is invalid even with unresolved commits.

## Tests

The correct fake catalog runs two rounds of four contenders with one winner
each. Planted bugs cover accepted stale guards, lost acknowledged commits,
partial publication, effects of rejected commits and stale reads. Other tests
cover two overlapping winners, forgotten ancestors, real-time order against
overlap, large IDs, implicit `main`, sequence gaps, recovered reads, scoped
redaction, UUID changes, malformed evidence and search timeouts. One test lets
server work outlive a client timeout and an early final read, then records the
late effect.

## Scope

Outside this model: other branches and tags, expiration, rollback, schema
changes, v1 and v3 metadata, property updates and multi-table transactions.
Unsupported operations are errors, not silently ignored. Expiration is covered
by the [maintenance workload](maintenance-workload.md).

Sources: the Iceberg spec's
[concurrency and sequence rules](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L98),
[snapshot fields](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L742-L765)
and [table metadata](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L926-L956);
the [REST spec](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml);
[Porcupine's model API](https://github.com/anishathalye/porcupine/blob/v1.3.1/model.go).
The manifest-list writer follows the Java
[v2 manifest schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/V2Metadata.java)
and [manifest-list writer](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/ManifestListWriter.java),
in an [Avro object container](https://avro.apache.org/docs/1.11.4/specification/#object-container-files).
