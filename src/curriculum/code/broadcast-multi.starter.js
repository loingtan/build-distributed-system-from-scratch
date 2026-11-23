// Your single-node solution. Each node only knows what it was told directly.
// node.peers lists the other nodes, e.g. ['n2', 'n3', 'n4', 'n5'].
const seen = new Set();

node.on('broadcast', (msg) => {
  seen.add(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
  // TODO: tell the other nodes. node.send(peer, { type: 'gossip', ... })
});

// TODO: handle the messages nodes send each other.

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
