// One entry per node: how much has been added *at that node*.
// A node only ever increases its own entry, so for any entry the larger
// value is always the newer one. That makes merging safe to repeat.
const counts = {};
for (const id of node.nodeIds) counts[id] = 0;

node.on('add', (msg) => {
  counts[node.id] += msg.body.delta;
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  const value = Object.values(counts).reduce((a, b) => a + b, 0);
  node.reply(msg, { type: 'read_ok', value });
});

node.on('merge', (msg) => {
  for (const [id, n] of Object.entries(msg.body.counts)) {
    counts[id] = Math.max(counts[id], n);
  }
});

// Anti-entropy: keep telling peers everything we know. A lost message is
// repaired by the next round; a duplicate changes nothing.
node.every(150, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', counts });
});
