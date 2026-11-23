// Snapshot isolation. Read the brief first.
//
// This is a read-committed store: each read returns the latest committed
// value. A transaction that reads twice, or reads two related keys, can see a
// commit land in between (a non-repeatable read, read skew).
//
// TODO: keep every committed write as a version stamped with a commit
// timestamp, give each transaction a snapshot when it begins, and read the
// newest version at or below the snapshot.

const committed = new Map(); // key -> the latest committed value
const txns = new Map(); // txn id -> { writes: Map of key -> value }

node.on('begin', (msg) => {
  // TODO: remember which commits this transaction may see.
  txns.set(msg.body.txn, { writes: new Map() });
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const key = msg.body.key;
  // TODO: read from the transaction's snapshot, not the latest commit.
  const value = t.writes.has(key) ? t.writes.get(key) : committed.has(key) ? committed.get(key) : null;
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
  // TODO: add new versions instead of overwriting, then drop versions no
  // open snapshot can see.
  for (const [key, value] of t.writes) committed.set(key, value);
  txns.delete(msg.body.txn);
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  txns.delete(msg.body.txn);
  node.reply(msg, { type: 'abort_ok' });
});

node.on('versions', (msg) => {
  // TODO: count every version still stored, across all keys.
  node.reply(msg, { type: 'versions_ok', count: committed.size });
});
