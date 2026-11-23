// A grow-only set. Each add is stored locally and sent to every peer once.
// Run the tests: a message lost in transit, or dropped by a partition, is
// never sent again, so some replicas never learn about some elements.
// TODO: replace the one-off message with anti-entropy.
const elements = new Set();

node.on('add', (msg) => {
  const element = msg.body.element;
  elements.add(element);
  for (const peer of node.peers) node.send(peer, { type: 'merge', elements: [element] });
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: [...elements] });
});

node.on('merge', (msg) => {
  for (const e of msg.body.elements) elements.add(e);
});
