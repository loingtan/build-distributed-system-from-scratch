// Remember which requests have been applied. A retry is acknowledged again
// (the client is still waiting for an answer) but changes nothing.
let total = 0;
const applied = new Set();

node.on('add', (msg) => {
  const { request_id, delta } = msg.body;
  if (!applied.has(request_id)) {
    applied.add(request_id);
    total += delta;
  }
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: total });
});
