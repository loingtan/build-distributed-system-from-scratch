## The problem

A key-value store on one machine keeps its data in a hash map in memory, because memory is fast. A client sends `put k1 = "blue"`, the node updates the map and replies `put_ok`. One millisecond later the power fails. When the node comes back, the map is empty. The client was told its write was safe, and it is gone.

The obvious fix is to write the map to disk on every change. That has two problems. It is slow: to save one changed key you rewrite every key, so a store holding a million keys does a million keys' worth of I/O per write. And it is fragile: if the power fails halfway through rewriting the file, the file on disk is half old and half new, and you have lost the old copy as well as the new one.

So there are two questions any storage engine must answer:

1. How do you make a write **durable** (still there after a crash) without rewriting everything?
2. Once you have a durable record of writes, how do you keep it from growing forever and still answer reads quickly?

This track answers the first with a write-ahead log and checkpoints, and the second with a small log-structured merge tree: a memtable, sorted immutable segments, compaction and Bloom filters. It is the same design, at toy scale, as LevelDB, RocksDB and the storage layer of Cassandra.

## The core idea

### Log first, acknowledge last

**Durability** is a promise: once a write is acknowledged, it survives a crash. The simulator gives each node a `node.disk` object that survives a crash; every other variable is lost and the code runs again from the top. That is a fair model of a real machine, where memory is lost and the disk is not.

A **write-ahead log** (WAL) is an append-only list of records, one per change, written to disk *before* the change is acknowledged. Appending is the cheapest thing you can do to a disk: one small write at the end, with no need to find or rewrite anything else. And an append is atomic in the sense that matters: after a crash a record is either there or it is not.

The order inside a write handler is fixed:

1. Append a record of the change to the log.
2. Apply the change to the in-memory map.
3. Reply to the client.

If the reply came before step 1, a crash between them would lose a write the client believes is safe. If step 2 came first, it would not matter for correctness (memory is lost anyway), but the log is the source of truth, so it goes first.

**Recovery** is the work a node does after a restart to get back to where it was. With a WAL it is one loop: start with an empty map and apply every log record in order. Because the log holds every change in the order it happened, replaying it reproduces the map exactly.

A delete is a change too. If you log only puts, replay brings deleted keys back to life. Log deletes as their own kind of record, for example `{ "op": "delete", "key": "k1" }`.

<figure>
<svg viewBox="0 0 620 230" role="img" aria-label="A put is logged, applied and acknowledged; after a crash the map is rebuilt from the log">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="10" y="40">client</text><text x="10" y="100">memory</text><text x="10" y="170">disk log</text>
  </g>
  <g style="stroke: var(--line)"><line x1="80" y1="36" x2="610" y2="36"/><line x1="80" y1="96" x2="610" y2="96"/><line x1="80" y1="166" x2="610" y2="166"/></g>
  <line x1="100" y1="36" x2="140" y2="166" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="104" y="210" style="fill: var(--muted); font: 12px var(--sans)">1 append</text>
  <line x1="150" y1="166" x2="190" y2="96" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="170" y="125" style="fill: var(--muted); font: 12px var(--sans)">2 apply</text>
  <line x1="200" y1="96" x2="240" y2="36" style="stroke: var(--good); stroke-width: 2"/>
  <text x="232" y="26" style="fill: var(--good); font: 12px var(--sans)">3 put_ok</text>
  <rect x="300" y="80" width="40" height="32" style="fill: var(--bad); opacity: 0.25"/>
  <text x="304" y="74" style="fill: var(--bad); font: 13px var(--sans)">crash</text>
  <text x="352" y="101" style="fill: var(--bad); font: 13px var(--mono)">map = {}</text>
  <line x1="440" y1="166" x2="480" y2="96" style="stroke: var(--m2); stroke-width: 2; stroke-dasharray: 4 3"/>
  <line x1="460" y1="166" x2="500" y2="96" style="stroke: var(--m2); stroke-width: 2; stroke-dasharray: 4 3"/>
  <line x1="480" y1="166" x2="520" y2="96" style="stroke: var(--m2); stroke-width: 2; stroke-dasharray: 4 3"/>
  <text x="430" y="200" style="fill: var(--m2); font: 12px var(--sans)">replay every record</text>
  <text x="530" y="101" style="fill: var(--ink); font: 13px var(--mono)">map ok</text>
</svg>
<figcaption>The write path is append, apply, reply. After a crash memory is empty, and replaying the log rebuilds it.</figcaption>
</figure>

### Checkpoints: stop the log growing

A WAL grows by one record per write and never shrinks. A store with twenty keys that has taken a million writes replays a million records on every restart. The extra time a restart takes is the **recovery time**, and with a bare WAL it grows without limit.

A **checkpoint** (often called a snapshot) is a copy of the whole current state written to disk. Once it is safely there, every log record it already reflects is redundant and can be dropped. Dropping them is **log truncation**. Recovery becomes two steps in a fixed order: load the checkpoint, then replay the short log written since.

The trade-off is in how often you checkpoint. A log append writes one small record; a checkpoint writes everything. Checkpoint on every write and you are back to rewriting the whole store each time. Never checkpoint and recovery time grows forever. Checkpointing every N writes bounds the log at N records and costs one full write per N appends.

The dangerous moment is the step between "checkpoint saved" and "log truncated". Truncate first and crash before the checkpoint lands, and the data is gone. Real engines handle this with ordering and atomic renames: write the checkpoint to a new file, flush it, atomically rename it into place, and only then delete old log files. In the simulator a handler runs without interruption until it `await`s, so writing the checkpoint and emptying the log in the same handler is atomic.

### The LSM tree: never rewrite old data

A checkpoint still rewrites every key to save a few changes. With a large data set that is too expensive. A **log-structured merge tree** (LSM tree) avoids rewriting old data on the write path entirely:

- Writes go to a small in-memory sorted table, the **memtable**, and to a WAL that protects it.
- When the memtable is full it is **flushed**: written to disk as a new **segment** whose keys are sorted, then never changed again. The WAL is emptied, because the segment now holds those writes durably. (Real systems call these segments SSTables, for "sorted string tables".)
- A read checks the memtable first, then the segments **newest first**. The first place that knows the key decides the answer.

Each segment is **immutable**. That makes flushing a single sequential write, makes crashes easy to reason about (a segment is either fully written or absent), and lets readers use a segment with no locking.

Keeping the keys in each segment **sorted** pays off twice. A lookup can binary-search a segment instead of scanning it. And a range query, "every key from `k010` up to `k020`", can walk each segment from the first key in range.

Immutability creates a puzzle for deletes. A delete cannot remove `k7` from a segment written yesterday. Instead it writes a **tombstone**: an entry for `k7` whose value means "deleted" (the tasks use `null`). A tombstone in a newer segment hides the value in an older one exactly the way a newer value would. When a lookup reaches a tombstone it stops and answers "not found"; it must not keep searching older segments.

<figure>
<svg viewBox="0 0 620 250" role="img" aria-label="A lookup checks the memtable then segments newest first and stops at the first match">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="20" y="30">memory</text><text x="20" y="110">disk</text>
  </g>
  <rect x="100" y="12" width="150" height="40" rx="4" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <text x="110" y="37" style="fill: var(--ink); font: 13px var(--mono)">memtable k2=v9</text>
  <g style="stroke: var(--line); fill: var(--surface)">
    <rect x="100" y="90" width="150" height="70" rx="4"/><rect x="270" y="90" width="150" height="70" rx="4"/><rect x="440" y="90" width="150" height="70" rx="4"/>
  </g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="110" y="110">seg 3 (newest)</text><text x="110" y="130">k1=v8</text><text x="110" y="148">k5=null</text>
    <text x="280" y="110">seg 2</text><text x="280" y="130">k3=v4</text><text x="280" y="148">k5=v6</text>
    <text x="450" y="110">seg 1 (oldest)</text><text x="450" y="130">k1=v1</text><text x="450" y="148">k5=v2</text>
  </g>
  <path d="M 175 52 L 175 88" style="stroke: var(--accent); stroke-width: 2; fill: none"/>
  <path d="M 250 125 L 268 125" style="stroke: var(--accent); stroke-width: 2"/>
  <path d="M 420 125 L 438 125" style="stroke: var(--faint); stroke-width: 2; stroke-dasharray: 4 3"/>
  <text x="100" y="190" style="fill: var(--ink); font: 13px var(--sans)">get k1: memtable no, seg 3 yes, answer v8 (seg 1's v1 is never seen)</text>
  <text x="100" y="212" style="fill: var(--ink); font: 13px var(--sans)">get k5: seg 3 has a tombstone, answer null (older v6 and v2 are hidden)</text>
  <text x="100" y="234" style="fill: var(--ink); font: 13px var(--sans)">get k9: searched everywhere, answer null: the expensive case</text>
</svg>
<figcaption>Newest first, first match wins. A tombstone is a match: it means "deleted" and stops the search.</figcaption>
</figure>

### Compaction: bound the number of segments

Flushing adds a segment and nothing removes one. Two costs grow. **Read amplification**: a lookup for a missing key searches every segment ever written. **Space amplification**: a key overwritten fifty times is stored fifty times, and a deleted key is stored twice, once as its value and once as its tombstone. (Both terms mean the ratio of work or space actually used to what the live data alone would need.)

**Compaction** merges several segments into one, keeping only the newest entry for each key. Walking the segments from oldest to newest and putting every entry into one map does exactly that, because a later entry for a key replaces an earlier one. The result is sorted and written as a new segment; the old segments are then deleted.

The subtle part is **tombstone cleanup**. A tombstone exists only to hide an older value. If the merge includes the **oldest** segment, nothing older remains, so the tombstone has nothing to hide and can be dropped. If the merge covers only some newer segments, the tombstone must be kept: an older segment outside the merge may still hold the value, and dropping the tombstone would bring it back.

Swap the segment list in one step, so a crash sees either the complete old set or the complete new set, never a mixture with entries missing.

### Bloom filters: skip segments that cannot have the key

Even with compaction, a lookup for a key that does not exist searches every segment. A **Bloom filter** is a small summary of a set that answers "is this key in the set?" with either *definitely not* or *maybe*.

It is an array of `m` bits, all 0 at first, and `k` hash functions that each map a key to a bit position. To add a key, set its `k` bits to 1. To test a key, look at its `k` bits: if any is 0, the key was never added. If all are 1, the key *might* have been added, or other keys might have set those bits. A "maybe" for a key that is absent is a **false positive**. There are never false negatives: a key that was added always finds its bits set.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="Two keys set bits in a 16-bit filter; one absent key is rejected, another is a false positive">
  <g style="stroke: var(--line); fill: var(--surface)">
    <rect x="60" y="70" width="30" height="30"/><rect x="90" y="70" width="30" height="30"/><rect x="120" y="70" width="30" height="30"/><rect x="150" y="70" width="30" height="30"/>
    <rect x="180" y="70" width="30" height="30"/><rect x="210" y="70" width="30" height="30"/><rect x="240" y="70" width="30" height="30"/><rect x="270" y="70" width="30" height="30"/>
    <rect x="300" y="70" width="30" height="30"/><rect x="330" y="70" width="30" height="30"/><rect x="360" y="70" width="30" height="30"/><rect x="390" y="70" width="30" height="30"/>
    <rect x="420" y="70" width="30" height="30"/><rect x="450" y="70" width="30" height="30"/><rect x="480" y="70" width="30" height="30"/><rect x="510" y="70" width="30" height="30"/>
  </g>
  <g style="fill: var(--accent-soft)"><rect x="91" y="71" width="28" height="28"/><rect x="211" y="71" width="28" height="28"/><rect x="301" y="71" width="28" height="28"/><rect x="391" y="71" width="28" height="28"/><rect x="481" y="71" width="28" height="28"/></g>
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="101" y="90">1</text><text x="221" y="90">1</text><text x="311" y="90">1</text><text x="401" y="90">1</text><text x="491" y="90">1</text>
  </g>
  <text x="60" y="30" style="fill: var(--accent); font: 13px var(--sans)">add k1 → bits 1, 5, 8      add k3 → bits 5, 11, 14</text>
  <text x="60" y="135" style="fill: var(--good); font: 13px var(--sans)">test k4 → bits 2, 8, 11: bit 2 is 0, definitely absent, skip segment</text>
  <text x="60" y="160" style="fill: var(--warn); font: 13px var(--sans)">test k6 → bits 1, 11, 14: all 1, "maybe", search finds nothing</text>
  <text x="60" y="185" style="fill: var(--muted); font: 13px var(--sans)">k6 is a false positive: bits set by k1 and k3 together</text>
</svg>
<figcaption>A Bloom filter with 16 bits and 3 hashes. Any 0 bit proves absence; all 1s only means "maybe".</figcaption>
</figure>

The false positive rate depends on bits per key and the number of hashes. With `n` keys, `m` bits and `k` hashes it is roughly `(1 − e^(−kn/m))^k`. At 10 bits per key and 6 or 7 hashes that is about 1%. You do not need `k` independent hash functions: two hashes `h1` and `h2` give `k` positions as `(h1 + i·h2) mod m` for `i = 0 … k−1`, which works almost as well.

Each segment gets its own filter, built when the segment is written (by a flush or a merge) and stored with it on disk, so it is still there after a restart. A lookup asks a segment's filter first and searches the segment only on "maybe".

## A worked example

One node, memtable limit 4 writes, compaction when 3 segments exist. Each segment carries a Bloom filter. Times are in milliseconds.

| t | event | log | memtable | segments (oldest first) |
|---|---|---|---|---|
| 0 | `put a=1` | `a=1` | a=1 | none |
| 5 | `put b=2` | `a=1 b=2` | a=1 b=2 | none |
| 9 | `delete a` | `… a=null` | a=null b=2 | none |
| 14 | `put c=3` | 4 records, flush | empty | S1 `[a=null, b=2, c=3]` |
| 20 | `put b=5` | `b=5` | b=5 | S1 |
| 26 | `put d=4` | `b=5 d=4` | b=5 d=4 | S1 |
| 31 | `put a=7` | `b=5 d=4 a=7` | a=7 b=5 d=4 | S1 |
| 33 | **crash** | survives | lost | S1 survives |
| 40 | restart | replay 3 records | a=7 b=5 d=4 | S1 |
| 45 | `get c` | | no | S1 filter says maybe, search finds 3 |
| 50 | `delete c` | 4 records, flush | empty | S1, S2 `[a=7, b=5, c=null, d=4]` |
| 60 | `get z` | | no | S2 filter: no. S1 filter: no. Zero segment reads |
| … | four more writes `e=1 f=2 b=8 e=null` | flush | empty | S1, S2, S3 `[b=8, e=null, f=2]` |
| 90 | compaction | | | merge all three |

Several things happen here that the tasks test:

- At t=9 the delete of `a` is a record in the log and a tombstone in the memtable. When the memtable is flushed at t=14, S1 holds `a=null`. Nothing older exists yet, but a flush never drops tombstones: it does not know what older segments contain.
- At t=33 the node crashes with three writes in the memtable. They were acknowledged, so they must survive. They do, because each was appended to the log before its reply. On restart the node replays the log and gets the memtable back; S1 was already on disk.
- At t=45, `get c` finds nothing in the memtable and asks S1's filter, which says "maybe" (c is there), so S1 is searched: one segment read.
- At t=60, `get z` is the worst case for an LSM tree without filters: two segments to search for nothing. Both filters say "definitely not", so the lookup answers `null` without reading either.
- At t=90 the merge walks S1, S2, S3 oldest to newest into one map: `a=null` is replaced by `a=7`; `b=2`, `b=5`, `b=8` collapse to `b=8`; `c=3` is replaced by `c=null`. (`e=1` never reached a segment: it was overwritten by its delete while still in the memtable, so S3 holds only the tombstone.) Because the merge includes the oldest segment, the tombstones for `c` and `e` have nothing left to hide and are dropped. The result is one segment `[a=7, b=8, d=4, f=2]`, written with a fresh filter, and it replaces the three old segments in a single assignment. Ten stored entries became four.

Had the node crashed during the merge, before the assignment, it would restart with S1, S2 and S3 intact and simply merge again later. Had it crashed after, it would restart with the merged segment. Either way every read gives the same answer.

## Why it is correct

The whole design rests on one invariant:

> At every moment, the newest-first overlay of disk state (checkpoint or segments, then the log replayed on top) equals the state the clients have been told about.

Take each piece in turn.

**The WAL.** A write is acknowledged only after its record is in the log, and a record is never removed from the log until something else on disk reflects it. So every acknowledged write is reflected on disk at every moment. Replay applies records in their original order, and applying a sequence of puts and deletes in order is deterministic, so the rebuilt map equals the map before the crash, minus at most writes that were never acknowledged.

**Checkpoints.** The checkpoint equals the map after the last record it covers. Truncation removes only those records. Loading the checkpoint and replaying what remains is therefore the same as replaying the whole history. This holds only if the checkpoint is durable before truncation; that ordering is the entire correctness argument.

**Segments.** Each segment is a frozen copy of the memtable at flush time, and the memtable reflects the log since the previous flush. Reading newest first, the first entry for a key is the most recent write to that key, because flushes happen in time order and the memtable is newer than any segment. A tombstone is the most recent write when it is found first, so "stop and answer null" is correct.

**Compaction.** Merging oldest to newest with replace-on-collision keeps, for each key, the entry a newest-first lookup over those segments would have found. Replacing a contiguous run of segments with its merge therefore changes no answer, provided the merged run keeps the run's position in the order. Dropping a tombstone changes an answer only if some older segment outside the merge has a value for that key; if the merge includes the oldest segment, there is none.

**Bloom filters.** A filter never says "absent" for a key that was added to it. Skipping a segment on "absent" therefore never skips an entry the lookup needed; a false positive costs one wasted search and nothing else. The filter must be built from exactly the entries of its segment, and since segments are immutable it never goes stale.

The assumptions: the disk keeps what was written to it, appends are atomic per record, and a crash cannot interrupt a group of updates you treat as one step. Real engines earn the last two with checksums on log records, `fsync`, and atomic file renames.

## Trade-offs and failure modes

**Write cost versus read cost.** A B-tree updates data in place: reads are cheap, random writes are expensive. An LSM tree makes every write a sequential append and pushes the cost to reads (several places to look) and to background compaction. That is the right trade for write-heavy workloads, and the reason LSM engines dominate in databases built for high write rates.

**Write amplification.** Each byte is written to the log, then to a segment at flush, then again every time it takes part in a merge. In leveled compaction a byte may be rewritten ten or more times over its life. This consumes disk bandwidth and, on SSDs, device lifetime.

**Compaction can fall behind.** If writes arrive faster than compaction can merge, segments pile up and reads slow down. Production engines throttle or stall writes when this happens (RocksDB calls it a write stall).

**Tombstones linger.** A deleted key costs space until a merge that includes all older data drops its tombstone. Workloads that delete heavily, like queues built on a key-value store, can fill segments with tombstones that reads must step over.

**Checkpoint frequency.** Frequent checkpoints mean fast recovery and a lot of rewriting; rare ones mean cheap steady state and slow recovery. There is no free choice, only a budget.

**`fsync` is the real cost of durability.** In the simulator an array push is durable. On a real machine a write sits in the OS page cache until it is flushed, and `fsync` can take milliseconds. Engines batch many client writes into one flush (group commit) to share that cost. Turning off the flush makes writes fast and quietly breaks the durability promise.

**What none of this protects against.** A disk that dies, a bug that writes the wrong value, or an operator deleting the data directory. Durability on one machine is not availability; for that you need copies on other machines, which is what the replication and Raft tracks add.

## In real systems

**PostgreSQL** is not an LSM engine; it updates pages in place. But it relies on a write-ahead log exactly as this track describes: every change is written to the WAL before the modified data page may be written back, and a commit waits for its WAL record to be flushed. Periodic checkpoints write all dirty pages to disk, after which older WAL segments can be recycled. Crash recovery replays WAL from the last checkpoint.

**LevelDB and RocksDB** are the textbook LSM engines. Writes go to a log file and a memtable; a full memtable becomes an immutable SSTable file. Files are organised in levels, and compaction merges files from one level into the next. RocksDB attaches a Bloom filter to each SSTable, and its documentation recommends around 10 bits per key for roughly a 1% false positive rate.

**Apache Cassandra** writes each mutation to a commit log and a memtable, flushes memtables to SSTables, and offers several compaction strategies (size-tiered, leveled, time-window). Deletes are tombstones, and Cassandra keeps them for a grace period (`gc_grace_seconds`, ten days by default) before compaction may purge them, so that a replica that missed the delete cannot resurrect the value during repair. That is tombstone cleanup with replication in the picture.

**Google Bigtable** introduced the memtable plus SSTable design in its 2006 paper, including minor compactions (flushes), merging compactions, major compactions that drop deletion markers, and Bloom filters to avoid disk reads for absent rows.

## Common misconceptions

- **"Writing to the in-memory map and saving it on a timer is durable enough."** Every write acknowledged since the last save is lost in a crash. Durability means the write is on disk before the reply, not eventually.
- **"A delete can just remove the key."** In a WAL, an unlogged delete is undone by replay. In an LSM tree, the old value sits in an immutable segment; without a tombstone it reappears.
- **"Compaction can always drop tombstones."** Only a merge that includes every older segment can. A partial merge that drops a tombstone revives whatever older value it was hiding.
- **"A Bloom filter can give a wrong answer."** It can give a useless one, "maybe" for an absent key, which costs a search. It never says "absent" for a present key, so the result of a lookup is never wrong.
- **"An LSM tree is always faster."** It is faster to write. Point reads may touch several segments, range scans must merge several sorted sources, and compaction competes with foreground work for disk bandwidth.

## How the tasks build it

[SE-1 Write-ahead log](#/c/store-wal) is the durability promise in its simplest form: log every put and delete to `node.disk.log` before replying, and rebuild the map from the log when the file runs again after a restart. The tests crash the node right after a write is acknowledged, sometimes twice in a row, and check that every acknowledged write and delete is still there.

[SE-2 Checkpoints](#/c/store-checkpoint) bounds the log. The tests require that the log never exceed 50 entries, and that the rest of the disk be rewritten at most once per 10 writes, so checkpointing on every write fails. Crashes land at arbitrary points between checkpoints, so recovery needs both the checkpoint and the log written after it, in the right order.

[SE-3 Memtable and segments](#/c/store-lsm) replaces the checkpoint with an LSM tree: flush a memtable of 8 writes into a sorted, immutable segment, read newest first, delete with tombstones, and answer range scans. The tests inspect `node.disk.segments` directly for sorting, size and immutability, and use keys that are deleted and rewritten across many segments.

[SE-4 Compaction](#/c/store-compaction) keeps the segment count and total entry count bounded under heavy overwrites and under churn where almost everything ever written is dead. Its tests stress the tombstone rule and crash the node while segments are being flushed and merged.

[SE-5 Bloom filters](#/c/store-bloom) adds a filter per segment and counts how many segments each get actually searches. Half the lookups are for absent keys that sort between real ones, so a cheap key-range check does not help; only a filter does. The filters must survive a restart, so they have to live on disk in plain JSON.

[SE-6 B-tree pages](#/c/store-btree) is the other main design: a balanced tree of pages with at most 4 keys each, updated in place. Full pages split, a full root grows a new root, and every write must be crash-safe, by copy-on-write with an atomic root switch or by logging page changes. The tests inspect the pages after every request: sorted keys, size limits, equal leaf depth, no orphan pages, every acknowledged write present, and at most 3 × height + 2 pages touched per write.

## Further reading

- [PostgreSQL: Write-Ahead Logging](https://www.postgresql.org/docs/current/wal-intro.html): a short, clear statement of why a database logs before it writes, from a production system.
- [Bigtable: A Distributed Storage System for Structured Data](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf): sections 5.3, 5.4 and 6 describe memtables, SSTables, compactions and Bloom filters, the design this track rebuilds.
- [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf): the original paper behind the name, with the cost analysis of trading reads for writes.
- [LevelDB implementation notes](https://github.com/google/leveldb/blob/main/doc/impl.md): log files, sorted tables, levels, compaction and recovery in a small, readable production engine.
- [RocksDB wiki: Bloom filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter): how a real engine sizes and stores per-file filters, and what the bits-per-key setting buys.
