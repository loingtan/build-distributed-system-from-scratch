const logs = new Map();
const committed = {};

node.on('send', (msg) => {
  const { key, msg: value } = msg.body;
  if (!logs.has(key)) logs.set(key, []);
  const log = logs.get(key);
  log.push(value);
  node.reply(msg, { type: 'send_ok', offset: log.length - 1 });
});

node.on('poll', (msg) => {
  const msgs = {};
  for (const [key, from] of Object.entries(msg.body.offsets)) {
    const log = logs.get(key) ?? [];
    msgs[key] = log.slice(from).map((value, i) => [from + i, value]);
  }
  node.reply(msg, { type: 'poll_ok', msgs });
});

node.on('commit_offsets', (msg) => {
  for (const [key, offset] of Object.entries(msg.body.offsets)) {
    committed[key] = Math.max(committed[key] ?? 0, offset);
  }
  node.reply(msg, { type: 'commit_offsets_ok' });
});

node.on('list_committed_offsets', (msg) => {
  const offsets = {};
  for (const key of msg.body.keys) {
    if (key in committed) offsets[key] = committed[key];
  }
  node.reply(msg, { type: 'list_committed_offsets_ok', offsets });
});
