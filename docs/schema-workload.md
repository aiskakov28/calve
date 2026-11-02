# Schema compare-and-set workload

`calve schema` creates a fresh one-column table and runs rounds of concurrent
guarded schema changes against it. In each round it loads the table, then every
contender sends one `POST` to the table with:

- requirements `assert-table-uuid` and `assert-current-schema-id`, both from that load;
- updates `add-schema`, the current schema plus one column unique to the
  contender (`c_<round>_<process>`), proposing an unused schema ID as the
  [Java client does][java-schema-ids], then `set-current-schema` with `-1`.

Requests are sent once. The next round starts after every response, from a new
load. A final load follows the last round.

```sh
calve schema --catalog http://localhost:8182/catalog --warehouse calve \
  --clients 4 --rounds 10 --out history-schema.jsonl
calve check history-schema.jsonl
```

Under `calve run --workload schema` the same rounds run with faults (see
[fault campaigns](fault-campaigns.md)).

## What `checker.CheckSchema` reports

All contenders in a round require the same current schema ID, and any winner
changes it. Under the [table spec's atomic metadata swap][table-concurrency]
and the [`assert-current-schema-id` requirement][requirements], at most one can
take effect. Each new schema copies the current one, so a winner's column stays
in every later schema; a contender's column in any load proves its commit took
effect, whatever schema ID the catalog assigned.

| Kind | Meaning |
| --- | --- |
| `multiple-winners` | two or more commits in one round were acknowledged or took effect |
| `lost` | an acknowledged change, or an unknown one proven by an earlier load, is missing from a later load's current schema |
| `phantom-failed` | a rejected commit's column is present |
| `unexpected` | a run column no earlier commit adds |

All have basis `catalog-contract`. A timeout or 5xx is unknown: its column
proves it committed, but absence never proves rejection, so an absent unknown
leaves the run inconclusive. A run with no winning round is inconclusive. The
checker refuses histories whose requests are not the exact guarded change
built from the round's load.

The fake catalog plants an ignored schema guard, applied-then-rejected and
applied-then-unknown schema commits, and acknowledged-but-unapplied commits;
the tests check each is caught and that a correct catalog has no findings.

## Initial live results

Local Compose run on 2026-10-29, 4 contenders, 10 rounds, no injected faults.
Each history replayed offline with the same verdict.

| Pinned catalog | Rounds with a winner | OK / rejected / unknown | Verdict |
| --- | ---: | --- | --- |
| Reference fixture 1.10.1 | 10 | 10 / 30 / 0 | valid |
| Lakekeeper v0.14.2 | 10 | 10 / 2 / 28 | inconclusive |
| Nessie 0.109.0 | 10 | 10 / 30 / 0 | valid |
| Polaris 1.8.0 | 10 | 10 / 30 / 0 | valid |

### Lakekeeper: losing schema commits answer 503

Every unknown Lakekeeper commit was HTTP 503 with
`Catalog backend error (Unexpected): error returned from database: duplicate key value violates unique constraint "table_schema_pkey"`.
A second run gave 30 of 30. All contenders propose the same next schema ID, so
the losers appear to insert a duplicate schema row before the compare-and-set on
the table rejects them, and the unique violation is reported as a server error
instead of a 409 conflict. None of their columns ever appeared, which fits a
rejection, but a 503 does not say so, and Calve keeps them unknown. This is a
lead in these initial results. The [follow-up investigation](triage-lakekeeper-schema-cas.md)
reproduces it independently and traces the write order, error mapping and
database rollback; no upstream report has been filed. It
resembles [lakekeeper#2110](https://github.com/lakekeeper/lakekeeper/issues/2110)
(a database error surfaced as 503 instead of 409) and
[lakekeeper#2201](https://github.com/lakekeeper/lakekeeper/issues/2201)
(sub-table writes before the table compare-and-set).

[java-schema-ids]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651
[table-concurrency]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/format/spec.md#L76-L90
[requirements]: https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3093-L3228
