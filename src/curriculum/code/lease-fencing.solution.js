// Fencing tokens. n1 is the lock server, n2 the storage, n3..n5 the workers.
//
// A lease alone cannot stop a holder that was paused (or whose message was
// slow) past its expiry: when it wakes up it still believes it holds the lock.
// So every grant carries a token that is higher than any before it, the worker
// sends that token with every request to the storage, and the storage refuses
// any token lower than the highest it has seen. The stale holder's write is
// rejected however late it arrives.

const LEASE_MS = 300;
const LOCK = 'n1';
const STORE = 'n2';

// ---- lock server (n1)

let holder = null;
let expires = 0;
let token = 0;        // the token of the current (or last) grant

const heldBy = (c) => holder === c && node.now() < expires;
const isFree = () => holder === null || node.now() >= expires;

node.on('acquire', (msg) => {
  if (heldBy(msg.src)) {
    expires = node.now() + LEASE_MS; // same holder: same token, fresh lease
  } else if (isFree()) {
    holder = msg.src;
    expires = node.now() + LEASE_MS;
    token += 1;                      // every new grant gets a higher token
  } else {
    node.reply(msg, { type: 'acquire_ok', granted: false, lease_ms: LEASE_MS });
    return;
  }
  node.reply(msg, { type: 'acquire_ok', granted: true, lease_ms: LEASE_MS, token });
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
let highest = 0;      // the highest token any request has carried

// A request with a lower token comes from a holder whose lease is over.
// Reads count too: once the new holder has read, the old one must not write.
function fenced(msg) {
  const t = msg.body.token;
  if (typeof t !== 'number' || t < highest) {
    node.reply(msg, { type: 'rejected', highest });
    return true;
  }
  highest = t;
  return false;
}

node.on('read', (msg) => {
  if (fenced(msg)) return;
  node.reply(msg, { type: 'read_ok', entries });
});

node.on('write', (msg) => {
  if (fenced(msg)) return;
  entries = msg.body.entries;
  node.reply(msg, { type: 'write_ok' });
});

node.on('inspect', (msg) => {
  node.reply(msg, { type: 'inspect_ok', entries });
});

// ---- workers (n3..n5)

const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));

// Ask for the lock until it is granted, for up to 1.5s. Returns the token or null.
async function acquire() {
  const deadline = node.now() + 1500;
  while (node.now() < deadline) {
    const res = await node.rpc(LOCK, { type: 'acquire' }, { timeout: 300 });
    if (res && res.granted) return res.token;
    await sleep(15 + node.random() * 20);
  }
  return null;
}

node.on('job', async (msg) => {
  const job = msg.body.job;
  const t = await acquire();
  if (t === null) {
    node.reply(msg, { type: 'job_ok', done: false });
    return;
  }
  let done = false;
  const read = await node.rpc(STORE, { type: 'read', token: t }, { timeout: 1000 });
  if (read && read.type === 'read_ok') {
    const res = await node.rpc(STORE, { type: 'write', token: t, entries: [...read.entries, job] }, { timeout: 1000 });
    done = !!res && res.type === 'write_ok';
  }
  node.send(LOCK, { type: 'release' });
  node.reply(msg, { type: 'job_ok', done });
});
