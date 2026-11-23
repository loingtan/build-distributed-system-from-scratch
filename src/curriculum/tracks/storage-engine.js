// Track: Storage engine. See docs/authoring.md for the shape of this file.
import { storeWal, storeCheckpoint, storeLsm, storeCompaction, storeBloom, storeBtree } from '../../sim/workloads/storage-engine.js';

// One node, one client. A fast link keeps the runs short.
// sequential: the client sends a fixed script one request at a time, so on the
// real-process runner (where every write is an fsync) it gets more wall-clock time.
const base = { nodeCount: 1, latency: [1, 4], sequential: true };

export default {
  id: 'storage-engine',
  code: 'SE',
  category: 'storage',
  title: 'Storage engine',
  level: 'Advanced',
  blurb: 'What happens between a write and the disk: a write-ahead log, crash recovery, and a small LSM tree.',
  subtracks: [
    {
      title: 'Surviving a crash',
      tasks: [
        {
          slug: 'store-wal',
          title: 'Write-ahead log',
          level: 'Easy',
          minutes: 20,
          summary: 'Log every change to disk before acknowledging it, and rebuild the store from the log after a crash.',
          concepts: ['durability', 'write-ahead log', 'recovery'],
          workload: storeWal,
          tests: [
            { ...base, name: 'No crashes', blurb: 'Puts, gets and deletes on a dozen keys, checking that each write reaches the log before its reply.', duration: 2500, params: { ops: 120, keys: 12 } },
            { ...base, name: 'Crash and recover', blurb: 'The node is killed six times, each time just as a write is acknowledged, and restarted.', duration: 4000, params: { ops: 200, keys: 16, crashes: 6 } },
            { ...base, name: 'Crash after crash', blurb: 'Twenty-five crashes, some of them twice in a row, on a handful of keys that are overwritten and deleted constantly.', duration: 6000, params: { ops: 250, keys: 6, crashes: 25 } },
          ],
          hints: [
            'Start with `node.disk.log ??= [];`. It creates the log on the first start and leaves it alone on every later one.',
            'In each write handler the order is: push onto `node.disk.log`, change the map, reply. If the reply came first, a crash right after it would lose a write the client believes is safe.',
            'The file runs again from the top after a restart. A loop over `node.disk.log` at the top level, applying each entry to the map, is the whole of recovery.',
            'Log deletes too, as their own kind of entry such as `{ op: "delete", key }`. If only puts are logged, replay brings deleted keys back.',
          ],
        },
        {
          slug: 'store-checkpoint',
          title: 'Checkpoints',
          level: 'Medium',
          minutes: 25,
          summary: 'Stop the log growing forever: save the whole map now and then, and truncate the log.',
          concepts: ['checkpoint', 'log truncation', 'recovery time'],
          workload: storeCheckpoint,
          tests: [
            { ...base, name: 'Long run', blurb: 'Five hundred requests with no crash; the log must stay within 50 entries without rewriting the disk on every write.', duration: 6500, params: { ops: 500, keys: 20 } },
            { ...base, name: 'Crashes between checkpoints', blurb: 'Twelve crashes at arbitrary points, so recovery needs both the checkpoint and the log written after it.', duration: 7000, params: { ops: 500, keys: 20, crashes: 12 } },
            { ...base, name: 'Hot keys', blurb: 'Five keys overwritten and deleted hundreds of times, with twenty crashes.', duration: 6000, params: { ops: 400, keys: 5, crashes: 20 } },
          ],
          hints: [
            'A checkpoint is the map written to disk as one plain object, for example `node.disk.snapshot = Object.fromEntries(data)`. Right after it, the log can become `[]`.',
            'Recovery is now two steps in a fixed order: load the checkpoint into the map, then replay the log on top of it.',
            'Trigger the checkpoint from the write path when `node.disk.log.length` reaches some number below 50. Thirty-two leaves room and stays far inside the rewrite budget.',
            'If you assign a new array to `node.disk.log`, any variable that still points at the old array is now writing to something that is not on disk. Always go through `node.disk.log`.',
          ],
        },
      ],
    },
    {
      title: 'A log-structured merge tree',
      tasks: [
        {
          slug: 'store-lsm',
          title: 'Memtable and segments',
          level: 'Hard',
          minutes: 40,
          summary: 'Flush a small memtable to immutable sorted segments, read newest-first, delete with tombstones, and scan a key range.',
          concepts: ['LSM tree', 'memtable', 'sorted segments', 'tombstones'],
          workload: storeLsm,
          tests: [
            { ...base, name: 'Reads and scans', blurb: 'Three hundred requests over thirty keys; lookups and range scans must see through dozens of segments.', duration: 4500, params: { ops: 300, keys: 30 } },
            { ...base, name: 'Crashes', blurb: 'Ten crashes: the segments are already on disk, the memtable has to come back from its log.', duration: 6000, params: { ops: 400, keys: 30, crashes: 10 } },
            { ...base, name: 'Deletes across segments', blurb: 'Eight keys written and deleted over and over, so most segments hold tombstones for values in older ones.', duration: 5000, params: { ops: 350, keys: 8, crashes: 6 } },
          ],
          hints: [
            'Keep the memtable as a `Map` from key to value, with `null` meaning "deleted". The log holds the same writes in order, so the memtable can be rebuilt after a crash.',
            'Flush when the log reaches 8 entries: sort the memtable entries by key, push `{ entries }` onto `node.disk.segments`, then clear the memtable and set `node.disk.log = []`.',
            'A lookup checks the memtable, then the segments from the last to the first, and stops at the first one that has the key. If what it finds is `null`, the answer is "not found": do not keep looking in older segments.',
            'For `scan`, walk the segments oldest to newest and then the memtable, putting every entry in range into one `Map` so newer entries replace older ones. Then remove the tombstones and sort by key.',
            'Sort strings with a comparison function such as `(a, b) => (a[0] < b[0] ? -1 : 1)`.',
          ],
        },
        {
          slug: 'store-compaction',
          title: 'Compaction',
          level: 'Medium',
          minutes: 30,
          summary: 'Merge segments so their number stays bounded, dropping overwritten values and tombstones.',
          concepts: ['compaction', 'space amplification', 'tombstone cleanup'],
          workload: storeCompaction,
          tests: [
            { ...base, name: 'Overwrites', blurb: 'Forty keys rewritten many times; old versions must not pile up in the segments.', duration: 6500, params: { ops: 500, keys: 40 } },
            { ...base, name: 'Churn', blurb: 'Hundreds of keys are each written once and deleted soon after, so almost everything ever stored is dead.', duration: 8000, params: { ops: 650, churn: true } },
            { ...base, name: 'Crashes', blurb: 'Ten crashes while segments are being flushed and merged.', duration: 6000, params: { ops: 400, keys: 30, crashes: 10 } },
          ],
          hints: [
            'After a flush, check `node.disk.segments.length`. If it has reached 4, merge before replying.',
            'To merge, walk the segments oldest to newest and put every entry into one `Map`. Later entries replace earlier ones, which leaves the newest entry per key.',
            'If the merge covers every segment, including the oldest, then a tombstone has nothing left to hide and can be left out of the result.',
            'If you merge only some of the newer segments, keep their tombstones: an older segment may still hold a value that the tombstone is hiding.',
            'Replace the list in one assignment, `node.disk.segments = [merged]`, rather than editing it in place.',
          ],
        },
        {
          slug: 'store-bloom',
          title: 'Bloom filters',
          level: 'Hard',
          minutes: 35,
          summary: 'Give each segment a Bloom filter so lookups for keys that do not exist skip it.',
          concepts: ['Bloom filter', 'false positives', 'read amplification'],
          workload: storeBloom,
          tests: [
            { ...base, name: 'Absent keys', blurb: 'Half of all lookups are for keys that were never written but sort between ones that were.', duration: 9000, params: { ops: 500, keys: 40, absent: true } },
            { ...base, name: 'Absent keys with crashes', blurb: 'The same with eight crashes: a segment needs its filter after a restart too.', duration: 9000, params: { ops: 450, keys: 30, absent: true, crashes: 8 } },
          ],
          hints: [
            'A Bloom filter is an array of bits, all 0 at first. To add a key, hash it in several different ways and set the bit each hash points at.',
            'To test a key, look at the same bits. Any 0 means the key was never added. All 1 means "maybe", and you still have to search.',
            'About 10 bits per key and 6 hash positions give roughly one false "maybe" in a hundred. You can get the positions from two hashes: `(h1 + i * h2) % size` for i = 0..5. Make sure both hashes are non-negative (`x >>> 0`): `bloom[-3] = 1` seems to work, but it is not an array element and is lost when the disk is saved in a crash.',
            'A simple string hash: start from a seed, and for each character do `h ^= code; h = Math.imul(h, 16777619);`, finishing with `h >>> 0`. Two different seeds give two hashes.',
            'Build the filter where the segment is built, in both flush and compaction, and store it in the segment object next to `entries` so it is still there after a restart. Count a segment read only when the filter lets the lookup through.',
          ],
        },
      ],
    },
    {
      title: 'A B-tree',
      tasks: [
        {
          slug: 'store-btree',
          title: 'B-tree pages',
          level: 'Hard',
          minutes: 50,
          summary: 'Keep the keys in a B-tree of small fixed-size pages on disk: split full pages, grow at the root, scan ranges, and survive crashes.',
          concepts: ['B-tree', 'page splits', 'copy-on-write', 'write-ahead log'],
          workload: storeBtree,
          tests: [
            { ...base, name: 'Random keys', blurb: 'Three hundred requests over forty keys; the pages are checked after every request.', duration: 5000, params: { ops: 300, keys: 40 } },
            { ...base, name: 'Ascending keys', blurb: 'Keys arrive in sorted order, so every split happens at the right edge of the tree.', duration: 5000, params: { ops: 300, keys: 60, ascending: true, deletes: 0.05 } },
            { ...base, name: 'Crashes', blurb: 'Ten crashes, each just after a write is acknowledged, some twice in a row.', duration: 6500, params: { ops: 350, keys: 40, crashes: 10 } },
          ],
          hints: [
            'Run the starter: the root leaf fills past 4 keys and nothing splits it. Start with a leaf split: the first half stays, the second half goes to a new page, and the first key of the new page becomes the separator in the parent.',
            'Write insertion recursively: `insert(pageId, key, value)` returns the page ids that replace this page (one, or two after a split) and, after a split, the separator. The parent puts both ids in its `children` and the separator in its `keys`, and may split in turn.',
            'An inner page with 5 keys splits around the middle one: two keys stay left, two go right, and the middle key moves up. If the root splits, make a new root with that one key and the two halves as children; this is the only way the tree gets taller, so all leaves stay at the same depth.',
            'Copy-on-write makes crashes harmless: never edit a page that is in the tree. Write new copies of the pages on the path (new ids), set `node.disk.root` to the new root, then delete the old copies. Every page must stay reachable from the root, so free the old ones in the same handler.',
            'A scan descends into every child whose range overlaps [from, to): child `i` holds keys from `keys[i - 1]` (inclusive) to `keys[i]` (exclusive).',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Acknowledging before the write is on disk', symptom: 'A put that was answered with put_ok is missing after a restart.', fix: 'Append to node.disk.log first and reply last. Saving on a timer leaves a window in which acknowledged writes exist only in memory.' },
    { title: 'Not logging deletes', symptom: 'A deleted key comes back after a crash, or after an older segment is read.', fix: 'A delete is a write. Log it, and in an LSM tree store it as a tombstone that hides older values.' },
    { title: 'Truncating the log without a checkpoint that covers it', symptom: 'After a restart the store has only the most recent writes.', fix: 'Drop log entries only once everything they describe is saved somewhere else on disk, in the same step.' },
    { title: 'Dropping tombstones in a partial merge', symptom: 'After compaction a get returns a value that was deleted long ago.', fix: 'A tombstone may be discarded only when the merge includes every older segment that could hold the key.' },
  ],
  questions: [
    { q: 'Why write to a log first, instead of updating the data structure on disk directly?', a: 'An append is a small sequential write that either is there or is not. Updating a larger structure in place can be interrupted halfway and leave it corrupt. With a log, the structure can always be rebuilt or repaired by replaying the entries.' },
    { q: 'An LSM tree turns every write into a cheap append. What does it pay for that?', a: 'Reads may have to look in several segments, and the same key can be stored many times until compaction removes the old copies. Compaction itself rewrites data repeatedly in the background, so each byte is written more than once.' },
    { q: 'A Bloom filter says a key might be in a segment, and it is not. What goes wrong?', a: 'Nothing but a wasted search: the segment is read and the key is not found. The filter never says "absent" for a key that is present, so it can cost time but never a wrong answer.' },
  ],
  reading: [
    { kind: 'Docs', title: 'PostgreSQL: write-ahead logging', url: 'https://www.postgresql.org/docs/current/wal-intro.html', note: 'Why a database logs changes before applying them, and what checkpoints are for.' },
    { kind: 'Paper', title: 'Bigtable: A Distributed Storage System for Structured Data', url: 'https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf', note: 'Section 5.3 and 5.4 describe the memtable, SSTables, compactions and Bloom filters this track rebuilds.' },
    { kind: 'Docs', title: 'LevelDB implementation notes', url: 'https://github.com/google/leveldb/blob/main/doc/impl.md', note: 'Log files, sorted tables, levels and recovery in a small production LSM engine.' },
  ],
};
