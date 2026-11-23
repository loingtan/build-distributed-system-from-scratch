// Tokens that survive a crash. n1 is the lock server, n2 the storage, n3..n5
// the workers.
//
// Fencing only works if tokens never go backwards. So n1 keeps its counter on
// node.disk, and n2 keeps its list and the highest token it has seen there:
// a crash loses memory, not promises already made.

const LEASE_MS = 300;
const LOCK = 'n1';
const STORE = 'n2';

// ---- lock server (n1)

let holder = null;
let expires = 0;
// The token counter lives on disk: after a crash, n1 must carry on from the
// last token it handed out, never below it. (The holder and expiry may be
// forgotten: a stale holder's requests carry an old token and n2 refuses them.)
node.disk.token ??= 0;

const heldBy = (c) => holder === c && node.now() < expires;
const isFree = () => holder === null || node.now() >= expires;

node.on('acquire', (msg) => {
  if (heldBy(msg.src)) {
    expires = node.now() + LEASE_MS; // same holder: same token, fresh lease
  } else if (isFree()) {
    holder = msg.src;
    expires = node.now() + LEASE_MS;
    node.disk.token += 1;            // on disk before the reply that hands it out
  } else {
    node.reply(msg, { type: 'acquire_ok', granted: false, lease_ms: LEASE_MS });
    return;
  }
  node.reply(msg, { type: 'acquire_ok', granted: true, lease_ms: LEASE_MS, token: node.disk.token });
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

// The list and the highest token survive a crash. Each is updated before the
// reply that depends on it.
node.disk.entries ??= [];
node.disk.highest ??= 0;

// A request with a lower token comes from a holder whose lease is over.
// Reads count too: once the new holder has read, the old one must not write.
function fenced(msg) {
  const t = msg.body.token;
  if (typeof t !== 'number' || t < node.disk.highest) {
    node.reply(msg, { type: 'rejected', highest: node.disk.highest });
    return true;
  }
  node.disk.highest = t;
  return false;
}

node.on('read', (msg) => {
  if (fenced(msg)) return;
  node.reply(msg, { type: 'read_ok', entries: node.disk.entries });
});

node.on('write', (msg) => {
  if (fenced(msg)) return;
  node.disk.entries = msg.body.entries;
  node.reply(msg, { type: 'write_ok' });
});

node.on('inspect', (msg) => {
  node.reply(msg, { type: 'inspect_ok', entries: node.disk.entries });
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
