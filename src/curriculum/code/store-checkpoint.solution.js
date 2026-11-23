// Two things on disk: a checkpoint (the whole map at some moment) and the
// log of changes made since that moment.
const CHECKPOINT_EVERY = 32; // log entries; the tests allow up to 50

node.disk.snapshot ??= {};
node.disk.log ??= [];
const data = new Map();

function apply(entry) {
  if (entry.op === 'put') data.set(entry.key, entry.value);
  else data.delete(entry.key);
}

// Recovery: start from the checkpoint, then replay the (short) log.
for (const key of Object.keys(node.disk.snapshot)) data.set(key, node.disk.snapshot[key]);
for (const entry of node.disk.log) apply(entry);

// Everything the log says is already in data, so once data is saved the
// log can be dropped. Both lines run in one handler, with no await between
// them, so a crash cannot land between the save and the truncation.
function checkpoint() {
  node.disk.snapshot = Object.fromEntries(data);
  node.disk.log = [];
}

function write(entry) {
  node.disk.log.push(entry);
  apply(entry);
  if (node.disk.log.length >= CHECKPOINT_EVERY) checkpoint();
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
