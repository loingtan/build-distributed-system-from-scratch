// Read committed. Read the brief first.
//
// This store applies every write the moment it arrives. Other transactions can
// read it before it commits (a dirty read), and an abort cannot take it back.

const store = new Map(); // key -> value

node.on('begin', (msg) => {
  // TODO: remember the transaction, with somewhere to keep its writes.
  node.reply(msg, { type: 'begin_ok' });
});

node.on('read', (msg) => {
  // TODO: a transaction sees its own writes, and otherwise only committed values.
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'read_ok', value });
});

node.on('write', (msg) => {
  // TODO: keep the write private to its transaction until it commits.
  store.set(msg.body.key, msg.body.value);
  node.reply(msg, { type: 'write_ok' });
});

node.on('commit', (msg) => {
  // TODO: publish the transaction's writes, all at once.
  node.reply(msg, { type: 'commit_ok' });
});

node.on('abort', (msg) => {
  // TODO: throw the transaction's writes away.
  node.reply(msg, { type: 'abort_ok' });
});
