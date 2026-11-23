// Asynchronous primary-backup, as in RP-1: the primary numbers its writes,
// answers at once and then tells the backups. Clients now read from any
// replica and carry a token: they send the last token they were given in
// every request, as msg.body.token (null at first).
const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

// key -> { value, seq }
const store = new Map();
let seq = 0; // used by the primary only

// TODO: per-key numbers stop a node from going backwards, but they cannot say
// whether a node has caught up with what a client has seen. Apply the
// primary's writes strictly in order instead (keep early arrivals aside), so
// that one number, how many writes this node has applied, describes it.
function apply(key, value, s) {
  const current = store.get(key);
  if (!current || s > current.seq) store.set(key, { value, seq: s });
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
    // TODO: relay the primary's token too.
    node.reply(msg, res ? { type: res.type } : { type: 'error', code: 'unavailable' });
    return;
  }

  seq += 1;
  apply(key, value, seq);
  // TODO: give write_ok a token that covers this write.
  node.reply(msg, { type: 'write_ok' });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', key, value, seq });
});

node.on('replicate', (msg) => {
  apply(msg.body.key, msg.body.value, msg.body.seq);
});

node.on('read', (msg) => {
  // TODO: if this node is behind msg.body.token, wait (node.after in a loop)
  // until it has caught up, or answer { type: "error", code: "unavailable" }
  // after about 600ms. Then answer with a token of your own.
  const entry = store.get(msg.body.key);
  node.reply(msg, { type: 'read_ok', value: entry ? entry.value : null });
});
