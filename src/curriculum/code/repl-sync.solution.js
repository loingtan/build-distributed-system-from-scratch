const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

const store = new Map(); // key -> { value, seq }
let seq = 0;

function apply(key, value, s) {
  const current = store.get(key);
  if (!current || s > current.seq) store.set(key, { value, seq: s });
}

// Keep sending one write to one backup until it confirms, or time runs out.
async function replicate(peer, entry) {
  const deadline = node.now() + 500;
  while (node.now() < deadline) {
    const res = await node.rpc(peer, { type: 'replicate', key: entry.key, value: entry.value, seq: entry.seq }, { timeout: 60 });
    if (res) return true;
  }
  return false;
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    // The primary answers within about 500ms either way; relay what it says.
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 800 });
    node.reply(msg, res && res.type === 'write_ok' ? { type: 'write_ok' } : { type: 'error', code: 'unavailable' });
    return;
  }

  seq += 1;
  const entry = { key, value, seq };
  apply(key, value, seq);

  // Synchronous replication: write_ok means "every node has it", so wait
  // for every backup before saying so.
  const confirmed = await Promise.all(node.peers.map((peer) => replicate(peer, entry)));
  if (confirmed.every(Boolean)) node.reply(msg, { type: 'write_ok' });
  else node.reply(msg, { type: 'error', code: 'unavailable' });
});

node.on('replicate', (msg) => {
  // A retry can deliver the same write twice, or an old one late.
  // apply() makes both harmless. Confirm only after applying.
  apply(msg.body.key, msg.body.value, msg.body.seq);
  node.reply(msg, { type: 'replicate_ok' });
});

node.on('read', (msg) => {
  const entry = store.get(msg.body.key);
  node.reply(msg, { type: 'read_ok', value: entry ? entry.value : null });
});
