// Fencing tokens. n1 is the lock server, n2 the storage, n3..n5 the workers.
//
// This works while every worker stays awake: n1 hands out leases, a worker
// takes the lock, reads the list from n2, appends its job and writes it back.
// But a worker that is paused after its read (or whose write is slow) wakes up
// after its lease ended and writes back an old list, wiping out jobs another
// worker finished in the meantime.
//
// TODO, n1: give every new grant a token, higher than any before it, in
// acquire_ok ({ granted: true, lease_ms, token }). The same holder acquiring
// again keeps its token.
// TODO, n2: every read and write carries a token. Refuse ({ type: "rejected" })
// any token lower than the highest one seen so far, on reads as well as writes.
// TODO, workers: send your token with every read and write.

const LEASE_MS = 300;
const LOCK = 'n1';
const STORE = 'n2';

// ---- lock server (n1)

let holder = null;
let expires = 0;

const heldBy = (c) => holder === c && node.now() < expires;
const isFree = () => holder === null || node.now() >= expires;

node.on('acquire', (msg) => {
  if (heldBy(msg.src)) {
    expires = node.now() + LEASE_MS;
  } else if (isFree()) {
    holder = msg.src;
    expires = node.now() + LEASE_MS;
  } else {
    node.reply(msg, { type: 'acquire_ok', granted: false, lease_ms: LEASE_MS });
    return;
  }
  node.reply(msg, { type: 'acquire_ok', granted: true, lease_ms: LEASE_MS });
});

node.on('renew', (msg) => {
  const ok = heldBy(msg.src);
  if (ok) expires = node.now() + LEASE_MS;
  node.reply(msg, { type: 'renew_ok', ok });
});

node.on('release', (msg) => {
  if (heldBy(msg.src)) holder = null;
  node.reply(msg, { type: 'release_ok' });
});

// ---- storage (n2)

let entries = [];

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', entries });
});

node.on('write', (msg) => {
  entries = msg.body.entries;
  node.reply(msg, { type: 'write_ok' });
});

node.on('inspect', (msg) => {
  node.reply(msg, { type: 'inspect_ok', entries });
});

// ---- workers (n3..n5)

const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));

// Ask for the lock until it is granted, for up to 1.5s. Returns true once granted.
async function acquire() {
  const deadline = node.now() + 1500;
  while (node.now() < deadline) {
    const res = await node.rpc(LOCK, { type: 'acquire' }, { timeout: 300 });
    if (res && res.granted) return true;
    await sleep(15 + node.random() * 20);
  }
  return false;
}

node.on('job', async (msg) => {
  const job = msg.body.job;
  const granted = await acquire();
  if (!granted) {
    node.reply(msg, { type: 'job_ok', done: false });
    return;
  }
  let done = false;
  const read = await node.rpc(STORE, { type: 'read' }, { timeout: 1000 });
  if (read && read.type === 'read_ok') {
    const res = await node.rpc(STORE, { type: 'write', entries: [...read.entries, job] }, { timeout: 1000 });
    done = !!res && res.type === 'write_ok';
  }
  node.send(LOCK, { type: 'release' });
  node.reply(msg, { type: 'job_ok', done });
});
