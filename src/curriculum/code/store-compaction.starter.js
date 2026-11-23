// The LSM tree from the previous task. It is correct, but every flush adds a
// segment and nothing ever removes one: reads get slower and slower, and
// overwritten values and tombstones stay on disk forever.
const MAX_SEGMENTS = 4;
const MEMTABLE_LIMIT = 8; // writes

node.disk.log ??= [];      // [key, value] for every write since the last flush
node.disk.segments ??= []; // oldest first; each is { entries: [[key, value], ...] } sorted by key

// key -> value, or null for a tombstone ("this key was deleted").
const memtable = new Map();
for (const [key, value] of node.disk.log) memtable.set(key, value);

const byKey = (a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);

// The memtable becomes a segment: sorted, and never changed again. Its
// contents are now durable in the segment, so the log can start afresh.
function flush() {
  node.disk.segments.push({ entries: [...memtable].sort(byKey) });
  node.disk.log = [];
  memtable.clear();
}

// TODO: merge the segments into one. For each key keep only the newest
//       entry. Think about when a tombstone may be dropped: only when no
//       older segment could still hold a value for that key.
function compact() {}

function write(key, value) {
  node.disk.log.push([key, value]);
  memtable.set(key, value);
  if (node.disk.log.length >= MEMTABLE_LIMIT) flush();
  // TODO: compact before there are more than MAX_SEGMENTS segments.
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
function lookup(key) {
  if (memtable.has(key)) return memtable.get(key);
  for (let i = node.disk.segments.length - 1; i >= 0; i--) {
    const value = search(node.disk.segments[i].entries, key);
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
