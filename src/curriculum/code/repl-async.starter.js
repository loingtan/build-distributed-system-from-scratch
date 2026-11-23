// Primary-backup replication. The first node is the primary; it never changes.
const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

const store = new Map(); // key -> value

node.on('write', (msg) => {
  const { key, value } = msg.body;
  // TODO: only the primary may accept a write. A backup passes the request on:
  //   const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
  // and then answers its own client (make the handler async to use await).
  store.set(key, value);
  node.reply(msg, { type: 'write_ok' });
  // TODO (primary): after replying, send the write to every backup with
  // node.send. Do not wait for them.
});

// TODO: a handler for the message the primary sends to its backups.
// Two of those messages can arrive in the opposite order to the one they
// were sent in. How does a backup know which write is the newer one?

node.on('read', (msg) => {
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'read_ok', value });
});
