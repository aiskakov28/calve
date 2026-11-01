# Unknown commit outcomes

When a commit times out, its connection resets, its response is cut off, or it
gets a 5xx, nobody knows whether it applied. Calve records it as `info` and
keeps it that way. Later reads add evidence, but they never turn the original
outcome into an acknowledgment or a rejection.

## Resolving an unknown as committed

Every checker can prove that an unknown commit took effect, but only from a
complete effect that could only have come from that request:

- **append:** its exact unique property value;
- **skew:** the full expected schema, current, on the original table UUID;
- **atomic:** its marker set to `1` on every table in the transaction;
- **schema:** its unique column in a loaded schema;
- **snapshot and maintenance:** its uniquely identified snapshot.

The resolution names the reads that support it, and the raw outcome stays in
the history. A positive observation stays valid evidence even if a later read
fails.

Once an unknown is proven committed, it's held to the same standard as an
acknowledged commit. If its effect later goes missing, that's a `lost`
finding, as long as nothing else in the run could have removed it.

Rejections work the other way round. For a rejected append, any value of its
unique key showing up contradicts the rejection, even a wrong value, and even
if a later read no longer shows it.

## Absence proves nothing

An unknown commit whose effect never shows up stays unknown. The request might
still be queued on the server and apply later. Calve has no tested way to know
that a catalog has finished with a request. A healthy endpoint, a healed fault,
a fixed wait or a run of identical reads doesn't establish it. So an
unresolved unknown makes a run inconclusive, unless something else in it
already proves a finding.

## Read-back

To give late commits a chance to show up, every workload command does a short
read-back after the run when some commits are still unresolved:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--readback-timeout` | 5s | total time for extra reads; `0` turns read-back off |
| `--readback-interval` | 100ms | pause between read passes |

Read-back only sends GETs, using the same routing as the recorded baseline. It
never resends a commit. Its budget is independent of the workload's
cancellation, and its requests are recorded with phase `recovery`. The
settings are saved in the history for reference. They are not evidence that
anything finished.

## Tests

The tests cover:

- responses lost or turned into 5xx after the commit applied;
- requests that never applied;
- cut-off successful responses and missing completions;
- evidence with the wrong table, value or request;
- a real HTTP timeout where the server holds the commit until after an early
  final read, then applies it.

Every case is replayed offline after the fake server stops. Separate tests
check the exit codes, and that cancelling the workload doesn't cancel
read-back.
