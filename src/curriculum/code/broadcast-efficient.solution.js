// Two ideas, each of which cuts the message count:
//
// 1. Batching. Each node keeps, per neighbour, the values that neighbour has
//    not acknowledged yet, and flushes them in one message on a timer.
// 2. Topology. Nodes are arranged as a star: everyone talks to one hub, and
//    the hub talks to everyone. A value needs at most two hops.
//
// The price: values wait for the next flush, and the hub is a single point
// of failure for propagation (values still arrive once it is reachable).
const seen = new Set();
const hub = node.nodeIds[0];
const neighbours = node.id === hub ? node.peers : [hub];
const unacked = new Map(neighbours.map((peer) => [peer, new Set()]));

function learn(value, from) {
  if (seen.has(value)) return;
  seen.add(value);
  for (const [peer, pending] of unacked) {
    if (peer !== from) pending.add(value);
  }
}

node.on('broadcast', (msg) => {
  learn(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
});

node.on('gossip', (msg) => {
  for (const value of msg.body.messages) learn(value, msg.src);
  node.reply(msg, { type: 'gossip_ok' });
});

node.every(250, async () => {
  for (const [peer, pending] of unacked) {
    if (pending.size === 0) continue;
    const batch = [...pending];
    node.rpc(peer, { type: 'gossip', messages: batch }, { timeout: 240 }).then((ack) => {
      // Only forget what this batch carried; newer values stay queued.
      if (ack) for (const value of batch) pending.delete(value);
    });
  }
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
