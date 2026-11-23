// An LSM tree, half built. Writes go to the memtable and to a log that
// protects it. Nothing is ever flushed, so the log grows without limit and
// there are no segments to read.
const MEMTABLE_LIMIT = 8; // writes

node.disk.log ??= [];      // [key, value] for every write since the last flush
node.disk.segments ??= []; // oldest first; each is { entries: [[key, value], ...] } sorted by key

// key -> value, or null for a tombstone ("this key was deleted").
const memtable = new Map();
for (const [key, value] of node.disk.log) memtable.set(key, value);

// TODO: turn the memtable into a segment: its entries sorted by key, pushed
//       onto node.disk.segments. Then empty the memtable and the log.
function flush() {}

function write(key, value) {
  node.disk.log.push([key, value]);
  memtable.set(key, value);
  // TODO: flush once the memtable has taken MEMTABLE_LIMIT writes.
}

function lookup(key) {
  if (memtable.has(key)) return memtable.get(key);
  // TODO: search the segments, newest first. The first one that has the
  //       key decides, even if what it holds is a tombstone.
  return null;
}

node.on('put', (msg) => {
  write(msg.body.key, msg.body.value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('delete', (msg) => {
  // TODO: removing the key from the memtable is not enough: an older value
  //       may sit in a segment. Write a tombstone instead.
  memtable.delete(msg.body.key);
  node.reply(msg, { type: 'delete_ok' });
});

node.on('get', (msg) => {
  node.reply(msg, { type: 'get_ok', value: lookup(msg.body.key) });
});

// TODO: node.on('scan', ...) -> { type: 'scan_ok', entries: [[key, value], ...] }
//       for from <= key < to, in key order, without deleted keys.
