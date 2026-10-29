# Maintenance race workload (#21)

`calve maintenance` races snapshot expiration against appends on a fresh table:

- **Appender** (process 0) commits a chain of empty snapshots, each with
  `assert-ref-snapshot-id` on main's head as it last knew it, then
  `add-snapshot` and `set-snapshot-ref`. Manifest lists are real empty v2 Avro
  files, written as for [`calve snapshot`](single-table-checker.md). After a
  rejected or unknown append it reloads the table and continues from what it sees.
- **Expirer** (process 1) repeatedly loads the table and sends `remove-snapshots`
  for every snapshot except main's head, guarded only by `assert-table-uuid`.
  That is what the Java client sends for expiration: [`UpdateRequirements`][requirements]
  adds no ref guard for `remove-snapshots`, so the catalog applies it to whatever
  the table is when it arrives.

Every request is sent once. A final load follows.

```sh
go run ./cmd/calve maintenance --catalog http://localhost:8182/catalog --warehouse calve \
  --s3-endpoint http://localhost:9000 --s3-region local-01 --appends 30 --out history-maintenance.jsonl
go run ./cmd/calve check history-maintenance.jsonl
```

`scripts/check-maintenance.sh` runs it on every Compose catalog from inside the
catalog's network, as `scripts/check-snapshots.sh` does, and replays each history
offline.

## What `checker.CheckMaintenance` reports

Iceberg's [metadata builder][remove-snapshots] removes exactly the listed snapshots
and drops a ref only when its snapshot was removed. The expirer never lists the
head it saw, and appends only move main forward, so:

| Kind | Meaning |
| --- | --- |
| `lost-snapshot` | a committed snapshot is missing although no expiration listed it |
| `phantom-failed` | a rejected append's snapshot is present, or a committed snapshot is missing and only rejected expirations listed it |
| `dangling-main` | after an acknowledged append, main is missing or points at a removed snapshot |
| `main-regressed` | main points at a snapshot older than an acknowledged append |
| `unexpected` | a snapshot no earlier append adds is present |

All have basis `catalog-contract`. An unknown append is committed only if a load
shows its snapshot; absence keeps the run inconclusive. A snapshot listed by an
unknown or acknowledged expiration may disappear. The run is also inconclusive
without an acknowledged expiration that overlapped an append, so a run that
never raced is not reported valid. The checker refuses histories whose appends
are not guarded on the appender's known head, or whose expirations list anything
but their preceding load's non-head snapshots.

The fake catalog plants expiration from stale metadata (discarding newer
appends), expiration of the head, and expirations or appends rejected after
applying; tests check each is caught and that a correct catalog has no findings.

### Models

The default `calve-maintenance-v1` assumes loads return every retained snapshot.
`calve-maintenance-current-v1` is for catalogs that return only the current
snapshot: it skips the retention check and keeps the main and outcome checks.
The model is chosen explicitly and recorded in the history, never inferred.
Nessie [documents its single-snapshot REST view][nessie-view], so its run uses the
current model; with nothing but the head visible the expirer has nothing to
expire, and the run is inconclusive by design.

## Initial live results

Local Compose run on 2026-10-29, 30 appends, 10 ms expiration interval, no
injected faults. Each history replayed offline with the same verdict.

| Pinned catalog | Model | Appends ok / rejected | Expirations sent / acknowledged / raced | Verdict |
| --- | --- | --- | --- | --- |
| Reference fixture 1.10.1 | `calve-maintenance-v1` | 30 / 0 | 1 / 1 / 1 | valid |
| Lakekeeper v0.14.2 | `calve-maintenance-v1` | 30 / 0 | 3 / 3 / 3 | valid |
| Nessie 0.109.0 | `calve-maintenance-current-v1` | 30 / 0 | 0 / 0 / 0 | inconclusive |
| Polaris 1.8.0 | `calve-maintenance-v1` | 27 / 3 | 9 / 4 / 3 | valid |

Polaris rejected 3 appends whose guard still held and 5 expirations whose only
requirement was the table UUID, all with 409. That is the unretried
compare-and-set conflict in the [409 triage](triage-409-without-requirements.md)
(apache/polaris#5907), seen from another workload; every rejection was clean.

[requirements]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/UpdateRequirements.java#L51-L59
[remove-snapshots]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1411-L1454
[nessie-view]: https://projectnessie.org/guides/iceberg-rest/#time-travel-with-iceberg-rest
