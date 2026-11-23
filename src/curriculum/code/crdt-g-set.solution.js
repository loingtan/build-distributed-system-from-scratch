// A grow-only set. Elements are only ever added, so two replicas merge by
// taking the union. Union is commutative, associative and idempotent: send
// the whole set to every peer on a timer, and a lost, late or repeated
// message does no harm. The next round repairs whatever was missed.
const elements = new Set();

node.on('add', (msg) => {
  elements.add(msg.body.element);
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: [...elements] });
});

node.on('merge', (msg) => {
  for (const e of msg.body.elements) elements.add(e);
});

// Anti-entropy: every 100ms, every peer gets the full state.
node.every(100, () => {
  const list = [...elements];
  for (const peer of node.peers) node.send(peer, { type: 'merge', elements: list });
});
