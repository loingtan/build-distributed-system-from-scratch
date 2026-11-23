// Snapshot isolation that does not lose updates: first committer wins.
//
// Built on the MVCC store. At commit, if another transaction has committed a
// newer version of any key this one wrote since this one's snapshot, this one
// would overwrite a write it never saw. Abort it; the client retries on the
// new value.
//
// Every commit gets the next timestamp, and every committed write is kept as a
// version stamped with it. A transaction remembers the timestamp of the last
// commit before it began (its snapshot) and reads, for each key, the newest
// version no newer than that. Writers never disturb readers.

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
