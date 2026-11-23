// The write-ahead log: every change, in the order it happened. It is the
// only copy that survives a crash; data is rebuilt from it.
node.disk.log ??= [];
const data = new Map();

function apply(entry) {
  if (entry.op === 'put') data.set(entry.key, entry.value);
  else data.delete(entry.key);
}

// Recovery. This file runs from the top after a restart, with the old
// node.disk, so replaying the log brings back every acknowledged change.
for (const entry of node.disk.log) apply(entry);

// Log first, then change memory, then acknowledge. Once the push has run
// the change is durable, so it is safe to tell the client.
function write(entry) {
  node.disk.log.push(entry);
  apply(entry);
}

node.on('put', (msg) => {
  write({ op: 'put', key: msg.body.key, value: msg.body.value });
  node.reply(msg, { type: 'put_ok' });
});

node.on('delete', (msg) => {
  write({ op: 'delete', key: msg.body.key });
  node.reply(msg, { type: 'delete_ok' });
});

node.on('get', (msg) => {
  const value = data.has(msg.body.key) ? data.get(msg.body.key) : null;
  node.reply(msg, { type: 'get_ok', value });
});
