const seen = new Set();

node.on('broadcast', (msg) => {
  seen.add(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
