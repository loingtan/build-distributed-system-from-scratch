Every get in your store costs a log entry: a disk write on a majority and a round of replication, for an operation that changes nothing. Most workloads are mostly reads. The obvious shortcut, which your starter takes, is to let the leader answer from its own store.

Run it. A leader cut off from the others still believes it leads. The majority elects someone else and keeps writing, and the old leader serves stale values for as long as the partition lasts. A newly elected leader has a quieter version of the problem: it holds every committed entry, but cannot yet tell which of its entries are committed.

## What you build

The same client protocol as the last task, with gets kept **out of the log**. Keep answering `log`; the tests count its committed entries:

```json
→ { "type": "log" }
← { "type": "log_ok", "entries": [{ "term": 2, "op": "put", "key": "k1", "value": 7 }], "commit": 1 }
```

## The idea: read-index

To answer a get, the leader:

1. makes sure an entry of **its own term** is committed. Append an empty **no-op** entry as soon as it is elected, so this happens without waiting for a write
2. notes its commit index as the read's index
3. confirms it is still the leader: a round of `append_entries` sent **after** the read arrived, answered by a majority without a higher term
4. waits until the read's index is applied, then answers from its store

The heartbeat can do step 3: each round confirms every read that arrived before it was sent.

## Timing

The log may hold at most one committed entry per write request clients sent (every retry and duplicate counted), plus one per term, plus 5. After the leader crashes and clients send only gets, some get must be answered within 2.5s.

## The tests check

- a read-heavy, healthy cluster whose log grows only with writes
- the leader cut off alone while one client keeps reading from it
- the leader crashing the instant it acknowledges a write, then 2.5s of reads only, starting with that key
- throughout: the history is linearizable
