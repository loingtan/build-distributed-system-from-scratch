// Clients retry when a reply is slow, so the same request can arrive twice.
// Every message carries msg.body.request_id, which is the same on a retry.
let total = 0;

node.on('add', (msg) => {
  // TODO: apply each request_id only once, but still answer every copy.
  total += msg.body.delta;
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: total });
});
