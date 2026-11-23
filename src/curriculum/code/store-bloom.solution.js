const MAX_SEGMENTS = 4;
const MEMTABLE_LIMIT = 8; // writes

node.disk.log ??= [];      // [key, value] for every write since the last flush
node.disk.segments ??= []; // oldest first; each is { entries: [[key, value], ...] } sorted by key

// key -> value, or null for a tombstone ("this key was deleted").
const memtable = new Map();
for (const [key, value] of node.disk.log) memtable.set(key, value);

const byKey = (a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);

// ---- Bloom filter: a compact, lossy summary of a set of keys.
// Each key sets K bits chosen by hashing. If any of a key's bits is 0 the
// key was never added. If all are 1 it probably was (other keys may have
// set those bits), so the segment still has to be searched.
const BITS_PER_KEY = 10;
const K = 6;

// FNV-1a, a small string hash. Math.imul keeps the multiply in 32 bits.
function hash(text, seed) {
  let h = seed;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

// K positions from two hashes: h1, h1 + h2, h1 + 2*h2, ...
// h2 is made odd with | 1, which also makes it a signed 32-bit number;
// >>> 0 turns it back into a positive one. A negative position would set
// bloom[-5], a property JSON drops, so the filter would break in a crash.
function positions(key, size) {
  const h1 = hash(key, 2166136261);
  const h2 = (hash(key, 40503) | 1) >>> 0;
  const out = [];
  for (let i = 0; i < K; i++) out.push((h1 + i * h2) % size);
  return out;
}

function makeSegment(entries) {
  const bloom = new Array(Math.max(64, entries.length * BITS_PER_KEY)).fill(0);
  for (const [key] of entries) {
    for (const p of positions(key, bloom.length)) bloom[p] = 1;
  }
  return { entries, bloom };
}

function mightContain(segment, key) {
  return positions(key, segment.bloom.length).every((p) => segment.bloom[p] === 1);
}

// The memtable becomes a segment: sorted, and never changed again. Its
// contents are now durable in the segment, so the log can start afresh.
function flush() {
  node.disk.segments.push(makeSegment([...memtable].sort(byKey)));
  node.disk.log = [];
  memtable.clear();
}

// Merge every segment into one. Going from oldest to newest, a later entry
// for a key replaces an earlier one, so only the newest survives. Because
// the merge includes the oldest segment, nothing older is left for a
// tombstone to hide, and tombstones can be dropped too.
// The new list replaces the old one in a single assignment: a crash leaves
// either all the old segments or the merged one, never a mixture.
function compact() {
  const merged = new Map();
  for (const segment of node.disk.segments) {
    for (const [key, value] of segment.entries) merged.set(key, value);
  }
  const entries = [...merged].filter(([, value]) => value !== null).sort(byKey);
  node.disk.segments = [makeSegment(entries)];
}

function write(key, value) {
  node.disk.log.push([key, value]);
  memtable.set(key, value);
  if (node.disk.log.length >= MEMTABLE_LIMIT) {
    flush();
    if (node.disk.segments.length >= MAX_SEGMENTS) compact();
  }
}

// Binary search: this is what sorting the segment buys.
// Returns the value (null for a tombstone), or undefined if the key is not here.
function search(entries, key) {
  let lo = 0;
  let hi = entries.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (entries[mid][0] === key) return entries[mid][1];
    if (entries[mid][0] < key) lo = mid + 1;
    else hi = mid - 1;
  }
  return undefined;
}

// Newest data wins: memtable, then segments from the last flushed to the
// first. A tombstone is an answer ("deleted"), so the search stops there.
// How many times a get has searched a segment's entries since startup.
let segmentReads = 0;

function lookup(key) {
  if (memtable.has(key)) return memtable.get(key);
  for (let i = node.disk.segments.length - 1; i >= 0; i--) {
    const segment = node.disk.segments[i];
    if (!mightContain(segment, key)) continue; // definitely not in this one
    segmentReads += 1;
    const value = search(segment.entries, key);
    if (value !== undefined) return value;
  }
  return null;
}

node.on('put', (msg) => {
  write(msg.body.key, msg.body.value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('delete', (msg) => {
  write(msg.body.key, null);
  node.reply(msg, { type: 'delete_ok' });
});

node.on('get', (msg) => {
  node.reply(msg, { type: 'get_ok', value: lookup(msg.body.key) });
});

// Overlay everything from oldest to newest so later writes replace earlier
// ones, then drop the tombstones and sort.
node.on('scan', (msg) => {
  const { from, to } = msg.body;
  const found = new Map();
  const take = ([key, value]) => {
    if (key >= from && key < to) found.set(key, value);
  };
  for (const segment of node.disk.segments) segment.entries.forEach(take);
  [...memtable].forEach(take);
  const entries = [...found].filter(([, value]) => value !== null).sort(byKey);
  node.reply(msg, { type: 'scan_ok', entries });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', segment_reads: segmentReads });
});
