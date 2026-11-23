Your LSM tree works, but every flush adds a segment and nothing removes one. A read for a missing key now searches every segment ever written. A key overwritten fifty times is stored fifty times, and a deleted key is stored twice: once as a value, once as its tombstone.

**Compaction** fixes both. It merges several segments into one, keeping only the newest entry for each key. Here you run it inside the write that triggers it.

## What you build

The same messages as before: `put`, `get`, `delete` and `scan`.

```json
→ { "type": "scan", "from": "k000", "to": "k100" }
← { "type": "scan_ok", "entries": [["k004", "v31"], ["k017", "v52"]] }
```

The disk layout is the same too: `node.disk.log` with at most 8 entries, and `node.disk.segments` oldest first, each `{ entries: [[key, value], ...] }` sorted by key with one entry per key and `null` for a tombstone. Read newest first, the segments must agree with the store on everything except the last 8 writes.

Segments may now be replaced: a merge produces new segments and the old ones go. They may also be larger than 8 entries.

## When can a tombstone go?

A tombstone exists to hide an older value. If the merge includes the **oldest** segment, nothing older is left to hide, and the tombstone can be dropped. If it merges only some newer segments, the tombstone must stay: an older segment may still hold the value.

Replace the segment list in one step (`node.disk.segments = [...]`) so a crash sees either the old segments or the new ones.

## The numbers

After every reply:

- at most **4 segments**
- at most **64 entries** across all segments, tombstones included

The tests keep fewer than 40 keys alive at any time, so a store that discards dead entries stays well inside the limit.

## The tests check

- the segment and entry limits, with many overwrites and with keys that are written once and soon deleted
- reads and scans stay correct, including after crashes in the middle of flushing and merging
