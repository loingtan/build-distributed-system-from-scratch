// Flooding from the previous challenge. It assumes every message arrives.
const seen = new Set();

function learn(value) {
  if (seen.has(value)) return;
  seen.add(value);
  for (const peer of node.peers) {
    // TODO: node.send never tells you whether the message arrived.
    // Use node.rpc and retry until the peer acknowledges.
    node.send(peer, { type: 'gossip', message: value });
  }
}

node.on('broadcast', (msg) => {
  learn(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
});

node.on('gossip', (msg) => {
  learn(msg.body.message);
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
