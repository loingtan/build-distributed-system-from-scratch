// A key-value store that lives in memory. Fast, and empty after every crash.
const data = new Map();

// node.disk is the only thing that survives a crash.
// TODO: keep a log of changes there: node.disk.log ??= [];
// TODO: at startup (this file runs again after a restart), replay the log
//       to rebuild data.

node.on('put', (msg) => {
  const { key, value } = msg.body;
  // TODO: push a record of this change onto node.disk.log before replying.
  data.set(key, value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('delete', (msg) => {
  // TODO: a delete is a change too. Log it.
  data.delete(msg.body.key);
  node.reply(msg, { type: 'delete_ok' });
});

node.on('get', (msg) => {
  const value = data.has(msg.body.key) ? data.get(msg.body.key) : null;
  node.reply(msg, { type: 'get_ok', value });
});
