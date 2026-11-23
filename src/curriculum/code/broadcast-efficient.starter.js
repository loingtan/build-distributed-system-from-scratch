// Your fault-tolerant solution. Correct, but count the messages in the timeline.
const seen = new Set();

// Keep sending until the peer says it has the value. rpc resolves with null
// when no reply arrives in time, which is our cue to try again.
async function deliver(peer, value) {
  for (;;) {
    const ack = await node.rpc(peer, { type: 'gossip', message: value }, { timeout: 300 });
    if (ack) return;
  }
}

function learn(value, from) {
  if (seen.has(value)) return;
  seen.add(value);
  for (const peer of node.peers) {
    if (peer !== from) deliver(peer, value);
  }
}

node.on('broadcast', (msg) => {
  learn(msg.body.message);
  node.reply(msg, { type: 'broadcast_ok' });
});

// Always acknowledge, even a value we already had: the sender may be
// retrying because our previous acknowledgement was lost.
node.on('gossip', (msg) => {
  learn(msg.body.message, msg.src);
  node.reply(msg, { type: 'gossip_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', messages: [...seen] });
});
