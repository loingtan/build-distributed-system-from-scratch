// When did we last hear from each peer?
const lastHeard = new Map(node.peers.map((peer) => [peer, 0]));

// TODO: send { type: 'heartbeat' } to every peer on a timer (node.every).

node.on('heartbeat', (msg) => {
  // TODO: note that msg.src is alive right now (node.now()).
});

node.on('status', (msg) => {
  // TODO: a peer is suspected when it has been silent for too long.
  const suspected = [];
  node.reply(msg, { type: 'status_ok', suspected });
});
