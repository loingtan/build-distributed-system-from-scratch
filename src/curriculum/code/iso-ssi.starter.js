// Serializable snapshot isolation. Read the brief first.
//
// This is the snapshot-isolation store with first-committer-wins (it starts
// from the lost-updates task, not from two-phase locking). It lets write skew
// through: two transactions that each read what the other writes both commit.
//
// TODO: remember which keys each transaction read, and keep committed
// transactions around while an open one overlaps them. At commit, find the
// rw-antidependencies with concurrent committed transactions and abort the
// commit if it would leave a pivot (a transaction with an rw edge in and an
// rw edge out).

const versions = new Map(); // key -> [{ ts, value }], oldest first
let clock = 0; // timestamp of the newest commit
const txns = new Map(); // txn id -> { snap, writes: Map }

function visible(key, snap) {
  const list = versions.get(key) || [];
  for (let i = list.length - 1; i >= 0; i--) if (list[i].ts <= snap) return list[i].value;
  return null;
}

// Drop every version that no open snapshot can see: for each key keep the
// newest version at or below the oldest snapshot, and everything newer.
function collect() {
  let oldest = clock;
  for (const t of txns.values()) oldest = Math.min(oldest, t.snap);
  for (const [key, list] of versions) {
    let keep = 0;
    for (let i = 0; i < list.length; i++) if (list[i].ts <= oldest) keep = i;
    if (keep > 0) versions.set(key, list.slice(keep));
  }
}

node.on('begin', (msg) => {
  // TODO: also track the keys this transaction reads.
  txns.set(msg.body.txn, { snap: clock, writes: new Map() });
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const key = msg.body.key;
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
  for (const key of t.writes.keys()) {
    const list = versions.get(key) || [];
    if (list.length && list[list.length - 1].ts > t.snap) {
      txns.delete(msg.body.txn);
      collect();
      return node.reply(msg, { type: 'aborted' });
    }
  }
  // TODO: look for rw-antidependencies here and abort a pivot.
  clock++;
  for (const [key, value] of t.writes) {
    if (!versions.has(key)) versions.set(key, []);
    versions.get(key).push({ ts: clock, value });
  }
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
