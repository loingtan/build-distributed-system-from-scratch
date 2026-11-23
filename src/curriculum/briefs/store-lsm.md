Checkpoints rewrite every key to save a few changes. With a large data set that gets expensive. A **log-structured merge tree** (LSM tree), the design inside LevelDB, RocksDB and Cassandra, never rewrites old data on a write. Instead:

1. Writes go to a small in-memory table, the **memtable**, and to a log that protects it.
2. When the memtable is full it is **flushed**: written to disk as a new **segment** with its keys sorted, and never changed again. The log starts afresh.
3. A read checks the memtable, then the segments **newest first**. The first place that has the key decides.

A delete cannot remove a key from a segment that is already written. It writes a **tombstone**, a marker meaning "deleted", which hides older values the same way a newer value would.

## What you build

`put`, `get` and `delete` as before, plus a range scan:

```json
→ { "type": "scan", "from": "k010", "to": "k020" }
← { "type": "scan_ok", "entries": [["k011", "v4"], ["k015", "v9"]] }
```

`from` is inclusive, `to` exclusive. Entries come back in ascending key order, one per live key, without deleted keys.

## The disk

The tests read these two things, and nothing else:

- `node.disk.log`: an array of at most **8 entries**. Flush once the memtable has taken 8 writes.
- `node.disk.segments`: an array, oldest first. Each segment is an object with `entries: [[key, value], ...]`, keys **strictly ascending**, one entry per key, `null` as the value of a tombstone. A segment holds at most 8 entries and must never change once written. Other fields on a segment object are allowed.

Read newest first, the segments must agree with the store on every key except those in the memtable: only the last 8 writes may live outside them.

## The tests check

- gets and scans return the latest data across dozens of segments
- deletes hide values in older segments
- segments are sorted, small and never modified
- after a crash, segments and the replayed memtable give back every acknowledged write
