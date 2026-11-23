// Serializable isolation with strict two-phase locking (2PL).
//
// A read takes a shared lock on its key, a write an exclusive one. Many
// transactions may share a key for reading; a writer has it alone. Locks are
// held until commit or abort, so once T has read a key, nobody can change it
// under T, and once T has written a key, nobody can read it until T is done.
//
// A request that cannot get its lock waits. Waiting can form a cycle (T1 waits
// for T2, which waits for T1): a deadlock. We look for a cycle every time a
// transaction starts to wait, and abort the transaction that would close it.

const data = new Map(); // key -> latest committed value
const txns = new Map(); // txn id -> { id, writes: Map, held: Map key -> 'S' | 'X', waiting: null | waiter }
const locks = new Map(); // key -> Map of txn id -> 'S' | 'X'
let waiters = []; // requests waiting for a lock, oldest first

function holders(key) {
  if (!locks.has(key)) locks.set(key, new Map());
  return locks.get(key);
}

// Can t take `mode` on key right now?
function grantable(t, key, mode) {
  for (const [id, m] of holders(key)) {
    if (id === t.id) continue;
    if (mode === 'X' || m === 'X') return false;
  }
  return true;
}

function grant(t, key, mode) {
  const have = t.held.get(key);
  if (have === 'X' || have === mode) return;
  t.held.set(key, mode);
  holders(key).set(t.id, mode);
}

// The transactions t would wait for if it waited for this lock.
function blockers(t, key, mode) {
  const out = [];
  for (const [id, m] of holders(key)) if (id !== t.id && (mode === 'X' || m === 'X')) out.push(id);
  return out;
}

// Would t waiting for `key` close a cycle in the waits-for graph?
function deadlocks(t, key, mode) {
  const seen = new Set();
  const stack = blockers(t, key, mode);
  while (stack.length) {
    const id = stack.pop();
    if (id === t.id) return true;
    if (seen.has(id)) continue;
    seen.add(id);
    const other = txns.get(id);
    if (other && other.waiting) stack.push(...blockers(other, other.waiting.key, other.waiting.mode));
  }
  return false;
}

// Release everything t holds and let waiting requests proceed.
function finish(t) {
  for (const key of t.held.keys()) holders(key).delete(t.id);
  txns.delete(t.id);
  waiters = waiters.filter((w) => w.t !== t);
  wake();
}

function wake() {
  let progress = true;
  while (progress) {
    progress = false;
    for (const w of waiters) {
      if (!grantable(w.t, w.key, w.mode)) continue;
      waiters = waiters.filter((x) => x !== w);
      w.t.waiting = null;
      grant(w.t, w.key, w.mode);
      w.then();
      progress = true;
      break;
    }
  }
}

// Take a lock, then run `then`. If the lock is busy, wait for it, unless
// waiting would deadlock: then abort t and answer the request with `aborted`.
function lock(t, key, mode, msg, then) {
  if (grantable(t, key, mode)) {
    grant(t, key, mode);
    return then();
  }
  if (deadlocks(t, key, mode)) {
    finish(t);
    return node.reply(msg, { type: 'aborted' });
  }
  const w = { t, key, mode, then };
  t.waiting = w;
  waiters.push(w);
}

node.on('begin', (msg) => {
  txns.set(msg.body.txn, { id: msg.body.txn, writes: new Map(), held: new Map(), waiting: null });
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const key = msg.body.key;
  lock(t, key, 'S', msg, () => {
    const value = t.writes.has(key) ? t.writes.get(key) : data.has(key) ? data.get(key) : null;
    node.reply(msg, { type: 'read_ok', value });
  });
});

node.on('write', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  const { key, value } = msg.body;
  lock(t, key, 'X', msg, () => {
    t.writes.set(key, value);
    node.reply(msg, { type: 'write_ok' });
  });
});

node.on('commit', (msg) => {
  const t = txns.get(msg.body.txn);
  if (!t) return node.reply(msg, { type: 'aborted' });
  for (const [key, value] of t.writes) data.set(key, value);
  finish(t);
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  const t = txns.get(msg.body.txn);
  if (t) finish(t);
  node.reply(msg, { type: 'abort_ok' });
});
