const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

// Every node applies the primary's writes strictly in the primary's order, so
// one number describes its whole state: `applied`, how many writes it has
// applied. That number is the session token. A token N says "this client has
// seen the store as it was after write N"; a replica may answer it only once
// it has applied at least N writes.
const store = new Map(); // key -> value
let applied = 0;
const early = new Map(); // seq -> { key, value }: arrived before an earlier write

function deliver(seq, key, value) {
  if (seq <= applied) return;
  early.set(seq, { key, value });
  while (early.has(applied + 1)) {
    const entry = early.get(applied + 1);
    early.delete(applied + 1);
    store.set(entry.key, entry.value);
    applied += 1;
  }
}

function sleep(ms) {
  return new Promise((resolve) => node.after(ms, resolve));
}

// Wait until this node has applied `need` writes, or give up after `limit` ms.
async function catchUp(need, limit) {
  const deadline = node.now() + limit;
  while (applied < need) {
    if (node.now() >= deadline) return false;
    await sleep(5);
  }
  return true;
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    // Pass the write on and relay the answer, token included.
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
    if (res && res.type === 'write_ok') node.reply(msg, { type: 'write_ok', token: res.token });
    else node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }

  const seq = applied + 1;
  deliver(seq, key, value);
  // The token covers the write itself, so the client will read its own write.
  node.reply(msg, { type: 'write_ok', token: seq });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', seq, key, value });
});

node.on('replicate', (msg) => {
  deliver(msg.body.seq, msg.body.key, msg.body.value);
});

node.on('read', async (msg) => {
  const need = typeof msg.body.token === 'number' ? msg.body.token : 0;
  // Behind what this client has already seen? Wait for replication to catch up.
  if (!(await catchUp(need, 600))) {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null, token: applied });
});
