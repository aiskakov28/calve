# Nessie rejects an omitted schema ID

Status: an interoperability difference, worked around in Calve by always
sending explicit schema IDs. Not an isolation finding, and not filed upstream.

## What happened

The first version of the skew workload left `schema-id` out of the schema in
its `add-schema` update. On a fresh table the existing schema has ID 0. Nessie
0.109.0 rejected every transaction with HTTP 400, `IllegalArgumentException`:
`A schema with ID 0 already exists`. All 20 concurrent attempts and both
controls' first commits were rejected, so the run tested nothing.

## The workaround

Calve now proposes `schema-id: max(existing schema IDs) + 1`, counting every
schema in the table's history, not just the current one. Everything else in the
request stays the same. Iceberg's
[Java metadata builder](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/core/src/main/java/org/apache/iceberg/TableMetadata.java#L1641-L1651)
picks a new schema's ID the same way.

After the change, ten concurrent rounds plus both controls on each catalog
gave:

| Catalog | Concurrent rounds | Sequential controls |
| --- | --- | --- |
| Reference fixture 1.10.1 | both transactions acknowledged in 10 of 10 | stale second commit rejected with 409, both directions |
| Lakekeeper v0.14.2 | one acknowledged, one 409, in 10 of 10 | stale second commit rejected with 409, both directions |
| Nessie 0.109.0 | one acknowledged (204), one 409, in 10 of 10 | stale second commit rejected with 409, both directions |

Every concurrent pair overlapped, with no unknown responses. The checker found
ten write-skew leads on the reference fixture and a full pass on Lakekeeper and
Nessie.

## Before filing anything

The open question is what the spec expects when `add-schema` omits the schema
ID: should the server assign an unused one, or keep the parser's default of 0?
The [REST schema definition](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml)
and the Java client are the places to start.
