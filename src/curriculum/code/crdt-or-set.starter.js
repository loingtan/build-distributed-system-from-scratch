// Your grow-only set, with a remove that deletes the element locally.
// Run the tests: the next merge from a peer that still has the element puts
// it straight back. A union cannot tell "never had it" from "removed it".
// TODO: make removes stick, let an element be added again, and let an add
// win over a remove that could not have seen it.
const elements = new Set();

node.on('add', (msg) => {
  elements.add(msg.body.element);
  node.reply(msg, { type: 'add_ok' });
});

node.on('remove', (msg) => {
  elements.delete(msg.body.element);
  node.reply(msg, { type: 'remove_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: [...elements] });
});

node.on('merge', (msg) => {
  for (const e of msg.body.elements) elements.add(e);
});

node.every(100, () => {
  const list = [...elements];
  for (const peer of node.peers) node.send(peer, { type: 'merge', elements: list });
});
