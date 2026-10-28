# Unknown commit outcomes (issue #16)

A timeout, connection reset, incomplete response, or HTTP 5xx leaves a commit's
outcome unknown. Preserve that original outcome in the history. Later table
observations provide additional evidence; they do not change a timeout into an
acknowledgment or a rejection.

The implementation will resolve commitment from attributable, complete effects
and use bounded read-back to observe commits that finish after a client timeout.
The append checker must report unresolved outcomes as inconclusive, consistently
with the skew checker. A recovery deadline bounds observation, not server work.

Absence cannot establish noncommitment while a request could still apply. Calve
currently has no validated per-catalog drain/completion mechanism. Healthy
endpoints, healed controllers, fixed waits, and repeated identical reads are not
such a mechanism. Negative resolution stays unavailable until that contract is
established and tested.

Validation must cover a lost response after commit, a request that never applies,
and a commit deliberately held past the timeout and an early final read. Offline
replay must retain the raw outcomes and produce the same resolutions. Existing
fault-free catalog gates remain applicable; wiring full fault campaigns into
`calve run` is separate integration work.
