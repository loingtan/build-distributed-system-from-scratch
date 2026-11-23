A get for a key that does not exist is the worst case for an LSM tree: the memtable says no, then every segment has to be searched to be sure.

A **Bloom filter** answers "is this key in the segment?" with either *definitely not* or *maybe*. It is an array of bits, all 0 at first. Adding a key hashes it several ways and sets the bit each hash picks. Testing a key looks at the same bits: any 0 means the key was never added. All 1 means maybe, because other keys may have set those bits. With about 10 bits per key and 6 hashes, a "maybe" for an absent key happens about once in a hundred.

It is never wrong the other way: a key that is there always gets *maybe*.

## What you build

Everything from the compaction task (same messages, same disk layout, same limits: 8 log entries, at most 4 segments, at most 64 entries across them), plus a filter per segment and one debug request:

```json
→ { "type": "stats" }
← { "type": "stats_ok", "segment_reads": 137 }
```

`segment_reads` counts every time a `get` searched one segment's entries, since the node started. Searching three segments for one get adds 3. A segment the filter rules out is not searched and not counted.

Build the filter when the segment is written, and keep it on disk inside the segment object, next to `entries`, so it survives a crash. Its format is up to you; it must be plain JSON.

## The numbers

The tests ask `stats` before and after each get.

- For keys that were never written, gets may search at most **20%** of the segments they would search without filters.
- A get that finds a value written 8 or more writes ago (so it is in a segment) must add at least 1.
- One get adds between 0 and the number of segments.

## The tests check

- the rules above, with half of all lookups for absent keys that sort between real ones
- reads stay correct, including after crashes
