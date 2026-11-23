// Read committed: a transaction's writes stay private until it commits, then
// become visible all at once.

const committed = new Map(); // key -> the latest committed value
const txns = new Map(); // txn id -> { writes: Map of key -> value }

node.on('begin', (msg) => {
  txns.set(msg.body.txn, { writes: new Map() });
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const key = msg.body.key;
  // Own writes first, then the latest committed value. Never another
  // transaction's uncommitted write: that would be a dirty read.
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
  // One synchronous handler: every write lands before anyone reads again, so
  // no reader sees half a commit and no two commits interleave.
  for (const [key, value] of t.writes) committed.set(key, value);
  txns.delete(msg.body.txn);
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  // Nothing was published, so rolling back is just forgetting the buffer.
  txns.delete(msg.body.txn);
  node.reply(msg, { type: 'abort_ok' });
});
