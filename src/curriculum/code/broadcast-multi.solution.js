const seen = new Set();

// Forward a value only the first time we see it. Without this check the
// nodes would pass the same value back and forth forever.
function learn(value) {
  if (seen.has(value)) return;
  seen.add(value);
  for (const peer of node.peers) node.send(peer, { type: 'gossip', message: value });
}

node.on('broadcast', (msg) => {
  learn(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
});

node.on('gossip', (msg) => learn(msg.body.message));

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
