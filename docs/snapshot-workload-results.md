# Initial live snapshot workload results (#65)

Local Compose run on 2026-10-29: two concurrent contenders, three rounds, fresh
empty tables, no injected faults. These are bounded observations under the
[snapshot evidence/model contract](single-table-checker.md), not catalog-wide
correctness claims. Each raw history replayed with the same verdict in a
network-disabled container using `scripts/check-snapshots.sh`.

| Pinned catalog | Model | Accepted | Rejected | Unknown | Verdict |
| --- | --- | ---: | ---: | ---: | --- |
| Reference fixture 1.10.1 | `calve-snapshot-v1` | 2 | 0 | 4 | inconclusive |
| Lakekeeper v0.14.2 | `calve-snapshot-v1` | 3 | 3 | 0 | valid |
| Nessie 0.109.0 | `calve-snapshot-current-v1` | 3 | 3 | 0 | valid |
| Polaris 1.8.0 | `calve-snapshot-v1` | 3 | 3 | 0 | valid |

All 24 proposed snapshots referenced actual manifest-list objects, including
rejected and unknown attempts. An independent Apache Avro 1.11.5 Python reader
decoded the files copied from the fixture/Silo: zero records, matching snapshot,
parent and sequence metadata, and the v2 field IDs. No data files were produced;
this validates empty manifest-list framing and metadata, not data scanning.

## Reference fixture: unresolved SQLite failures

The reference fixture returned HTTP 500 `UncheckedSQLException` for concurrent
writes. Its logs identify `SQLITE_BUSY: The database file is locked`. A later
table creation also failed while the fixture remained locked; restarting that
same fixture allowed the table to be created and the recorded run above to
finish. No containers or volumes were deleted.

Those responses remain unknown, and the final metadata does not positively
resolve their snapshots. They are not counted as rejected commits or used as a
correctness finding. The fixture made some acknowledged progress, so the CI
coverage check passes while the history stays inconclusive. Persistent setup
failure still fails the gate. No upstream bug claim was made from this run.

## Nessie: explicit observation scope

The default retained-history model did not fit Nessie's responses. Nessie
[documents its single-snapshot REST view as intentional](https://projectnessie.org/guides/iceberg-rest/#time-travel-with-iceberg-rest).
The pinned version also omits `parent-snapshot-id` from that returned descriptor;
its [metadata projection](https://github.com/projectnessie/nessie/blob/nessie-0.109.0/catalog/format/iceberg/src/main/java/org/projectnessie/catalog/formats/iceberg/nessie/NessieModelIceberg.java)
leaves the parent unset.

The Compose runner explicitly records `calve-snapshot-current-v1` for Nessie.
That model checks UUID, main/current snapshot ID, last sequence, current
descriptor except parent, guarded atomic publication and real-time ordering.
It makes no retention or lineage claim. The requests still contain the real
parent and guard, and raw responses are preserved. The default model remains
strict for all other runs; this is not an automatic relaxation after a failure
or a reported Nessie defect.

## Reproduction and evidence

Follow the [Compose workload instructions](single-table-checker.md#live-snapshot-workload-65).
Histories are `history-snapshot-<catalog>-<attempt>.jsonl`; logs are
`snapshot-<catalog>-<attempt>.log`. Local artifacts for this run are under the
gitignored `results/issue65/ci/`, with copied manifests under `results/issue65/avro/`.
CI uploads the histories/logs as its `history` artifact; saved evidence can be
checked with `calve check-table` without contacting a catalog or object store.
