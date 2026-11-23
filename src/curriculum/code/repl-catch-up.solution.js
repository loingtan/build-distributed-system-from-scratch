const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

// Every write, in the primary's order. An entry's position is its sequence
// number, so "how many entries do you have?" is all a backup needs to say
// for the primary to know what it is missing. The log lives on disk.
node.disk.log ??= [];
const log = node.disk.log;

// The store is just the log replayed, so it can be rebuilt after a restart.
const store = new Map();
for (const entry of log) store.set(entry.key, entry.value);

function append(entry) {
  log.push(entry);
  store.set(entry.key, entry.value);
}

// ---- primary: one sender per backup

const have = {};    // backup -> how many log entries it has confirmed
const sending = {}; // backup -> is a sender loop running?

async function catchUp(peer) {
  if (sending[peer]) return;
  sending[peer] = true;
  while ((have[peer] ?? 0) < log.length) {
    const from = have[peer] ?? 0;
    const res = await node.rpc(peer, { type: 'replicate', from, entries: log.slice(from, from + 50) }, { timeout: 100 });
    // No answer: lost, partitioned or crashed. Just ask again.
    if (res) have[peer] = res.have;
  }
  sending[peer] = false;
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
    node.reply(msg, res ? { type: res.type } : { type: 'error', code: 'unavailable' });
    return;
  }

  append({ key, value }); // on disk before the client hears write_ok
  node.reply(msg, { type: 'write_ok' });
  for (const peer of node.peers) catchUp(peer);
});

// After a restart the primary does not know what its backups have. Sending
// from 0 is safe: each backup answers with its real position.
if (isPrimary) for (const peer of node.peers) catchUp(peer);

// ---- backup

node.on('replicate', (msg) => {
  const { from, entries } = msg.body;
  // Take only what extends the log exactly: skip entries we already have,
  // and take nothing if there would be a gap.
  if (from <= log.length) {
    for (const entry of entries.slice(log.length - from)) append(entry);
  }
  node.reply(msg, { type: 'replicate_ok', have: log.length });
});

node.on('read', (msg) => {
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null });
});
