const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

// key -> { value, seq }. seq is the position of the write in the primary's
// order; it is what lets a backup tell an old write from a new one.
const store = new Map();
let seq = 0; // used by the primary only

function apply(key, value, s) {
  const current = store.get(key);
  if (!current || s > current.seq) store.set(key, { value, seq: s });
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    // Backups do not decide anything. Pass the write on and relay the answer.
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
    node.reply(msg, res ? { type: res.type } : { type: 'error', code: 'unavailable' });
    return;
  }

  seq += 1;
  apply(key, value, seq);
  // Asynchronous replication: answer first, tell the backups afterwards.
  node.reply(msg, { type: 'write_ok' });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', key, value, seq });
});

node.on('replicate', (msg) => {
  // Messages can overtake each other; apply() drops the ones that are late.
  apply(msg.body.key, msg.body.value, msg.body.seq);
});

node.on('read', (msg) => {
  const entry = store.get(msg.body.key);
  node.reply(msg, { type: 'read_ok', value: entry ? entry.value : null });
});
