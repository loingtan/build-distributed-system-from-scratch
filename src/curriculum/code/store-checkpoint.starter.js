// The write-ahead log from the previous task. It recovers correctly, but
// node.disk.log grows by one entry per write, forever.
node.disk.log ??= [];
const data = new Map();

function apply(entry) {
  if (entry.op === 'put') data.set(entry.key, entry.value);
  else data.delete(entry.key);
}

// TODO: recovery now has two steps: load the checkpoint (if there is one),
//       then replay the log on top of it.
for (const entry of node.disk.log) apply(entry);

// TODO: write the whole map to node.disk as one object (for example
//       node.disk.snapshot = Object.fromEntries(data)) and start a fresh log.
function checkpoint() {}

function write(entry) {
  node.disk.log.push(entry);
  apply(entry);
  // TODO: when the log is long enough, checkpoint. Not on every write:
  //       a checkpoint rewrites everything, a log append writes one entry.
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
