// Heartbeats every 100ms; a peer is suspected after 500ms of silence. That
// tolerates four lost or late heartbeats in a row before raising an alarm.
const HEARTBEAT_MS = 100;
const SUSPECT_AFTER_MS = 500;

const lastHeard = new Map(node.peers.map((peer) => [peer, 0]));

node.every(HEARTBEAT_MS, () => {
  for (const peer of node.peers) node.send(peer, { type: 'heartbeat' });
});

node.on('heartbeat', (msg) => {
  lastHeard.set(msg.src, node.now());
});

// Suspicion is computed when asked, never stored. A peer that starts
// talking again stops being suspected with no extra code.
node.on('status', (msg) => {
  const suspected = node.peers.filter((peer) => node.now() - lastHeard.get(peer) > SUSPECT_AFTER_MS);
  node.reply(msg, { type: 'status_ok', suspected });
});
