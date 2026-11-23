// The heartbeat failure detector from earlier, now reporting a membership
// view. It works, but every node tells every other node "I am alive" ten
// times a second: (N - 1) * 10 messages per node per second. That grows with
// the cluster and blows the message budget.
//
// TODO: replace the heartbeats with SWIM probing. Every protocol period
// (200ms), ping ONE member and wait briefly for its ack. No ack: mark it
// 'suspect'. An ack: mark it 'alive' again. Go round a shuffled list of
// peers so every member is probed regularly.
const HEARTBEAT_MS = 100;
const SUSPECT_AFTER_MS = 500;

const lastHeard = new Map(node.peers.map((peer) => [peer, 0]));

node.every(HEARTBEAT_MS, () => {
  for (const peer of node.peers) node.send(peer, { type: 'heartbeat' });
});

node.on('heartbeat', (msg) => {
  lastHeard.set(msg.src, node.now());
});

// The view: every node id, this one included, mapped to 'alive' or 'suspect'.
node.on('members', (msg) => {
  const members = { [node.id]: 'alive' };
  for (const peer of node.peers) members[peer] = node.now() - lastHeard.get(peer) > SUSPECT_AFTER_MS ? 'suspect' : 'alive';
  node.reply(msg, { type: 'members_ok', members });
});
