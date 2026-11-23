// Serializable snapshot isolation (SSI).
//
// Snapshot isolation with first-committer-wins, plus one more check. A
// transaction R that reads a key which a concurrent transaction W writes has
// an rw-antidependency R -> W: R must come before W in any serial order,
// because it did not see W's write. Every cycle that snapshot isolation lets
// through contains a "pivot": a transaction with an rw edge in and an rw edge
// out, both to concurrent transactions. So we never let a pivot commit.
//
// Edges are recorded when the second of the two transactions commits. When a
// commit would give some committed transaction (or itself) both an incoming
// and an outgoing rw edge, the committing transaction is aborted instead.

const versions = new Map(); // key -> [{ ts, value }], oldest first
let clock = 0; // timestamp of the newest commit
const txns = new Map(); // txn id -> { snap, writes: Map, reads: Set }
let recent = []; // committed transactions an open one may overlap: { commitTs, reads, writes, rwIn, rwOut }

function visible(key, snap) {
  const list = versions.get(key) || [];
  for (let i = list.length - 1; i >= 0; i--) if (list[i].ts <= snap) return list[i].value;
  return null;
}

function collect() {
  let oldest = clock;
  for (const t of txns.values()) oldest = Math.min(oldest, t.snap);
  for (const [key, list] of versions) {
    let keep = 0;
    for (let i = 0; i < list.length; i++) if (list[i].ts <= oldest) keep = i;
    if (keep > 0) versions.set(key, list.slice(keep));
  }
  // A committed transaction matters only while some open one began before it committed.
  recent = recent.filter((c) => c.commitTs > oldest);
}

const overlaps = (set, keys) => {
  for (const k of keys) if (set.has(k)) return true;
  return false;
};

function abort(msg) {
  txns.delete(msg.body.txn);
  collect();
  node.reply(msg, { type: 'aborted' });
}

node.on('begin', (msg) => {
  txns.set(msg.body.txn, { snap: clock, writes: new Map(), reads: new Set() });
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const key = msg.body.key;
  t.reads.add(key);
  const value = t.writes.has(key) ? t.writes.get(key) : visible(key, t.snap);
  node.reply(msg, { type: 'read_ok', value });
});

node.on('write', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  t.writes.set(msg.body.key, msg.body.value);
  node.reply(msg, { type: 'write_ok' });
});

node.on('commit', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const writes = [...t.writes.keys()];
  // Committed transactions concurrent with t: they committed after t's snapshot.
  const concurrent = recent.filter((c) => c.commitTs > t.snap);
  // First committer wins.
  if (concurrent.some((c) => overlaps(c.writes, writes))) return abort(msg);
  // rw edges between t and each concurrent committed transaction c.
  const outTo = concurrent.filter((c) => overlaps(c.writes, t.reads)); // t read what c wrote: t -> c
  const inFrom = concurrent.filter((c) => overlaps(c.reads, writes)); // c read what t writes: c -> t
  const pivot =
    (outTo.length > 0 && inFrom.length > 0) || // t itself
    outTo.some((c) => c.rwOut) || // c would have t -> c in and c -> x out
    inFrom.some((c) => c.rwIn); // c would have x -> c in and c -> t out
  if (pivot) return abort(msg);

  clock++;
  for (const [key, value] of t.writes) {
    if (!versions.has(key)) versions.set(key, []);
    versions.get(key).push({ ts: clock, value });
  }
  for (const c of outTo) c.rwIn = true;
  for (const c of inFrom) c.rwOut = true;
  recent.push({ commitTs: clock, reads: t.reads, writes: new Set(writes), rwIn: inFrom.length > 0, rwOut: outTo.length > 0 });
  txns.delete(msg.body.txn);
  collect();
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  txns.delete(msg.body.txn);
  collect();
  node.reply(msg, { type: 'abort_ok' });
});

node.on('versions', (msg) => {
  let count = 0;
  for (const list of versions.values()) count += list.length;
  node.reply(msg, { type: 'versions_ok', count });
});
