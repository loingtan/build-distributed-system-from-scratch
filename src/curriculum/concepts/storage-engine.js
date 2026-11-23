export default [
  {
    term: 'Durability',
    summary: 'The promise that an acknowledged write survives a crash, because it reached stable storage before the reply.',
    detail: `A write is **durable** once it is on storage that survives a crash (a disk, or several machines) rather than only in memory. The rule that delivers it is *store first, acknowledge last*: if the reply goes out before the write is stored, a crash in between loses a write the client believes is safe.

Example: a node appends \`{ op: "put", key: "k1", value: 7 }\` to its on-disk log, then replies \`put_ok\`. If it crashes one millisecond later, the restart replays the log and \`k1\` is still 7.

On real hardware durability costs an \`fsync\`, which can take milliseconds, so databases batch many writes into one flush (group commit).`,
    related: ['Write-ahead log', 'Recovery'],
  },
  {
    term: 'Write-ahead log',
    aliases: ['WAL'],
    summary: 'An append-only file of changes, each written to disk before the change is applied or acknowledged.',
    detail: `A **write-ahead log** records every change as a small record appended to the end of a file, before the change is acknowledged. Appending is cheap (one sequential write) and atomic per record: after a crash a record is either there or not.

Recovery replays the log from the start (or from the last checkpoint), applying each record in order, which rebuilds exactly the state the clients were told about. Deletes must be logged too, or replay brings deleted keys back.

PostgreSQL, MySQL's InnoDB, SQLite (in WAL mode), LevelDB, RocksDB and Cassandra all use one. Raft's replicated log is the same idea spread over several machines.`,
    related: ['Durability', 'Checkpoint', 'Recovery'],
  },
  {
    term: 'Recovery',
    summary: 'What a node does after a restart to rebuild its in-memory state from what survived on disk.',
    detail: `**Recovery** turns durable state back into working state. With a write-ahead log it is a loop: start empty and apply every log record in order. With a checkpoint it is two steps in a fixed order: load the checkpoint, then replay the log written since.

Correct recovery depends on what was stored before each acknowledgement. Anything acknowledged must be reconstructible; anything not yet acknowledged may be lost.`,
    related: ['Write-ahead log', 'Checkpoint', 'Recovery time'],
  },
  {
    term: 'Checkpoint',
    aliases: ['checkpoints'],
    summary: 'A full copy of the current state saved to disk, so that the log entries it covers can be discarded.',
    detail: `A **checkpoint** writes the whole state (for a key-value store, every key) to disk at one moment. Every log record from before that moment is then redundant, and the log can be truncated. Recovery loads the checkpoint and replays only the short log written after it.

The order is everything: the checkpoint must be durable *before* the log is truncated. Real engines write the checkpoint to a new file, flush it, rename it atomically, and only then delete old log files.

How often to checkpoint is a trade-off: a checkpoint rewrites everything, so doing it on every write wastes the log's advantage, while never doing it makes recovery time grow without limit. PostgreSQL checkpoints on a timer and on WAL volume.`,
    related: ['Log truncation', 'Recovery time', 'Write-ahead log'],
  },
  {
    term: 'Log truncation',
    summary: 'Dropping log entries that are already reflected somewhere else on disk, so the log stays short.',
    detail: `**Log truncation** removes the prefix of a log that a checkpoint, a flushed segment or a snapshot already covers. It is what keeps the log, and therefore recovery time, bounded.

It is safe only if the thing that covers those entries is already durable. Truncate first and crash before the checkpoint lands, and the data exists nowhere. In a single-threaded handler with no await between the two steps, or with an atomic file rename, the pair becomes one step.`,
    related: ['Checkpoint', 'Log compaction'],
  },
  {
    term: 'Recovery time',
    summary: 'How long a restarted node needs before it can serve again; with a bare log it grows with every write.',
    detail: `**Recovery time** is dominated by how much must be read and replayed. A store with 20 keys that has taken a million writes replays a million log records if it never checkpoints, even though its state is tiny.

Checkpoints bound it: with a checkpoint every N writes, recovery replays at most N records after loading the checkpoint. The price is the I/O of writing checkpoints. Databases expose this as a tuning knob (for example PostgreSQL's \`checkpoint_timeout\` and \`max_wal_size\`).`,
    related: ['Checkpoint', 'Log truncation'],
  },
  {
    term: 'LSM tree',
    aliases: ['log-structured merge tree', 'LSM'],
    summary: 'A storage design that buffers writes in memory, flushes them as immutable sorted files, and merges those files later.',
    detail: `A **log-structured merge tree** never updates data in place. Writes go to a memtable (protected by a write-ahead log); a full memtable is flushed as a new sorted, immutable segment; reads check the memtable and then the segments newest first; and background compaction merges segments to bound their number.

Every write becomes a sequential append, which suits disks and SSDs, at the cost of reads that may look in several places and of rewriting data during compaction.

LevelDB, RocksDB, Cassandra, HBase, ScyllaDB and the storage layers of CockroachDB (Pebble) and TiKV are LSM trees. The name comes from O'Neil et al. (1996); Bigtable popularised the memtable-plus-SSTable form.`,
    related: ['Memtable', 'Sorted segments', 'Compaction', 'Bloom filter'],
  },
  {
    term: 'Memtable',
    summary: 'The in-memory table in an LSM tree that takes new writes until it is full and is flushed to disk.',
    detail: `The **memtable** holds the most recent writes, usually in a sorted structure (a skip list or balanced tree) so it can be flushed as a sorted file in one pass. Because it lives in memory, it is protected by a write-ahead log: after a crash the log is replayed to rebuild it.

When it reaches a size limit it is **flushed**: written out as a new immutable segment, after which the log that protected it can be emptied. A deleted key is kept in the memtable as a tombstone, not removed, so that the flush carries the delete to disk.`,
    related: ['LSM tree', 'Sorted segments', 'Write-ahead log'],
  },
  {
    term: 'Sorted segments',
    aliases: ['SSTable', 'sorted string table', 'segments'],
    summary: 'Immutable on-disk files of key-value pairs sorted by key, written by flushing a memtable or by merging.',
    detail: `A **sorted segment** (an SSTable in Bigtable, LevelDB and Cassandra) holds one entry per key, in ascending key order, and is never modified after it is written. Sorting allows binary search within a segment and efficient range scans and merges; immutability makes crashes simple (a segment is either complete or absent) and lets readers use it without locks.

A lookup checks segments newest first and stops at the first that has the key. Example: if segment 3 has \`k1=v8\` and segment 1 has \`k1=v1\`, the answer is \`v8\` and segment 1 is never consulted.`,
    related: ['LSM tree', 'Memtable', 'Compaction'],
  },
  {
    term: 'Tombstones',
    aliases: ['tombstone'],
    summary: 'A marker written in place of a deleted value, which hides older copies of the key until compaction removes it.',
    detail: `In a store whose old data is immutable, a delete cannot erase anything. It writes a **tombstone**: an entry for the key that means "deleted". A lookup that meets the tombstone first answers "not found" and must not keep searching older data, where a stale value may still sit.

Tombstones cost space until they are cleaned up, and heavy-delete workloads can leave reads stepping over many of them. Replicated stores also use tombstones so that a replica that missed a delete cannot resurrect the value; Cassandra keeps them for \`gc_grace_seconds\` (ten days by default) for that reason.`,
    related: ['Tombstone cleanup', 'Compaction', 'Sorted segments'],
  },
  {
    term: 'Compaction',
    summary: 'Merging several segments into one that keeps only the newest entry per key, bounding read cost and space.',
    detail: `**Compaction** reads several sorted segments and writes one merged segment. Walking them from oldest to newest and letting a later entry for a key replace an earlier one leaves exactly the newest entry per key, which is what a newest-first lookup would have found.

Example: segments \`[b=2]\`, \`[b=5]\`, \`[b=8]\` merge into \`[b=8]\`: three stored entries become one. The new segment list should replace the old one in one atomic step so that a crash sees either the old set or the new one.

Strategies differ in which segments they merge: size-tiered compaction merges segments of similar size, leveled compaction (LevelDB, RocksDB) keeps levels of non-overlapping files and merges one level into the next. Each trades write amplification against read and space amplification.`,
    related: ['Tombstone cleanup', 'Space amplification', 'Read amplification', 'Write amplification'],
  },
  {
    term: 'Space amplification',
    summary: 'The ratio of disk space a store uses to the size of its live data; overwrites and tombstones inflate it.',
    detail: `**Space amplification** measures waste: a key overwritten fifty times and not yet compacted is stored fifty times, and a deleted key is stored twice (its old value and its tombstone). If 1 GB of live data occupies 3 GB on disk, space amplification is 3.

Compaction reduces it by discarding overwritten values and, where safe, tombstones. Compacting more aggressively lowers space amplification but raises write amplification, since data is rewritten more often.`,
    related: ['Compaction', 'Read amplification', 'Write amplification'],
  },
  {
    term: 'Tombstone cleanup',
    summary: 'Dropping a tombstone during compaction, which is safe only when no older data can still hold the key.',
    detail: `A tombstone exists to hide older values. A merge that includes the **oldest** segment leaves nothing older behind, so its tombstones have nothing to hide and can be dropped. A merge of only some newer segments must keep them: an older segment outside the merge may still hold the value, and dropping the tombstone would bring that value back.

Example: segments S1 \`[k=1]\`, S2 \`[k=null]\`, S3 \`[]\`. Merging S2 and S3 alone must keep \`k=null\`; merging all three may drop both entries.

With replication there is a second condition: every replica must have seen the delete, or anti-entropy can copy the old value back. That is why Cassandra waits a grace period before purging.`,
    related: ['Tombstones', 'Compaction'],
  },
  {
    term: 'Bloom filter',
    aliases: ['Bloom filters'],
    summary: 'A compact bit array that says a key is "definitely not" in a set or "maybe" in it, never wrongly "not".',
    detail: `A **Bloom filter** is an array of m bits, all 0 at first, plus k hash functions. Adding a key sets the k bits its hashes pick. Testing a key checks those bits: any 0 means the key was never added; all 1 means "maybe", since other keys may have set them.

With n keys the false positive rate is about \`(1 − e^(−kn/m))^k\`. Ten bits per key with 6 or 7 hashes gives roughly 1%. Two hashes are enough to derive k positions as \`(h1 + i·h2) mod m\`.

LSM engines keep one filter per segment so a lookup for an absent key can skip segments without reading them. Bloom filters also appear in Bigtable, Cassandra, web caches and network routers. A standard filter cannot remove keys; a segment's filter never needs to, because the segment is immutable.`,
    related: ['False positives', 'Read amplification', 'Sorted segments'],
  },
  {
    term: 'False positives',
    aliases: ['false positive'],
    summary: 'A "maybe" answer from a filter for a key that is not actually present; it costs time, never correctness.',
    detail: `A **false positive** happens when every bit an absent key hashes to was set by other keys. The lookup then searches the segment and finds nothing: one wasted read, but the final answer is still right.

The rate falls as bits per key rise: about 10% at 5 bits per key, 1% at 10, 0.1% at 15 (with a well-chosen number of hashes). The opposite error, a false negative, cannot occur in a Bloom filter, which is what makes it safe to skip a segment on "no".`,
    related: ['Bloom filter'],
  },
  {
    term: 'Read amplification',
    summary: 'The number of places a single read must look; in an LSM tree it grows with the number of segments.',
    detail: `**Read amplification** counts the work behind one logical read. In an LSM tree a lookup checks the memtable and then each segment that might hold the key, so with 12 segments a lookup for a missing key can cost 12 searches.

Three things keep it down: compaction (fewer segments), Bloom filters (skip segments that cannot hold the key), and key-range metadata per segment (skip segments whose range excludes the key). It is one corner of the classic trade-off between read, write and space amplification.`,
    related: ['Bloom filter', 'Compaction', 'Space amplification'],
  },
  {
    term: 'Write amplification',
    summary: 'How many bytes a store writes to disk for each byte the client writes; compaction makes it greater than one.',
    detail: `In an LSM tree a byte is written to the log, then to a segment when the memtable flushes, then again each time compaction merges that segment. Under leveled compaction a byte can be rewritten ten or more times over its life, so **write amplification** of 10 to 30 is common.

It matters because disk bandwidth spent on compaction is not available to clients, and SSDs wear out with writes. Size-tiered compaction lowers it at the cost of more space and read amplification.`,
    related: ['Compaction', 'Space amplification', 'Read amplification'],
  },
  {
    term: 'B-tree',
    aliases: ['B+ tree', 'btree'],
    summary: 'A balanced tree of fixed-size pages, updated in place, where every lookup reads one page per level.',
    detail: `A **B-tree** stores sorted keys in pages of fixed size. Inner pages hold separator keys and child pointers; in the common B+ tree variant only the leaves hold values. Every leaf is at the same depth, so with a branching factor of a few hundred, four levels hold billions of keys.

Unlike an LSM tree, a B-tree overwrites pages where they are. Reads are cheap and predictable; writes cost a random page write or more per change, and crash safety needs a write-ahead log or copy-on-write. PostgreSQL, MySQL InnoDB, SQLite and LMDB are all B-trees.`,
    related: ['Page splits', 'Copy-on-write', 'LSM tree'],
  },
  {
    term: 'Page splits',
    aliases: ['page split'],
    summary: 'When a B-tree page overflows it is cut in two and a separator key is added to its parent.',
    detail: `Inserting into a full page splits it: the first half stays, the second half moves to a new page, and a separator key pointing to the new page goes into the parent. The parent may overflow and split in turn. When the root splits, a new root is created above the two halves; this is the only way the tree grows taller, which keeps every leaf at the same depth.

With at most 4 keys per page, inserting a fifth key into a leaf leaves 3 and 2 keys, and the first key of the right half becomes the separator.`,
    related: ['B-tree', 'Copy-on-write'],
  },
  {
    term: 'Copy-on-write',
    aliases: ['shadow paging', 'copy on write'],
    summary: 'Never modifying live data: write changed pages to new places, then switch one root pointer atomically.',
    detail: `With **copy-on-write**, a B-tree write copies every page on the path from the root to the leaf, applies the change to the copies, and then makes the new root current with a single atomic write. A crash before the switch leaves the old tree intact; after it, the new one. Old pages are freed once nothing refers to them.

The cost is rewriting about one page per level per write. The benefits are crash safety without a log and cheap consistent snapshots: a reader holding the old root sees the old tree. LMDB and Btrfs work this way.`,
    related: ['B-tree', 'Write-ahead log'],
  },
];
