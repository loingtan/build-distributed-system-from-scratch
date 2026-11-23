// A shopping cart: a map from item to quantity, with observed-remove keys.
// Every add is a tagged quantity change { tag: qty }. An item is in the cart
// while it has a change that no remove has deleted; its quantity is the sum
// of those changes. A remove deletes exactly the changes it has seen, so the
// quantity starts again from zero, and an add it could not see survives.
//
// This is the observed-remove set from before, with a number on each tag.
const changes = {}; // item -> { tag: qty }
const removed = {}; // tag -> true (tombstones)
let counter = 0;

node.on('add', (msg) => {
  const item = msg.body.item;
  counter += 1;
  changes[item] ??= {};
  changes[item][node.id + ':' + counter] = msg.body.qty;
  node.reply(msg, { type: 'add_ok' });
});

node.on('remove', (msg) => {
  for (const t of Object.keys(changes[msg.body.item] || {})) removed[t] = true;
  node.reply(msg, { type: 'remove_ok' });
});

node.on('read', (msg) => {
  const value = {};
  for (const [item, tags] of Object.entries(changes)) {
    const live = Object.keys(tags).filter((t) => !removed[t]);
    if (live.length) value[item] = live.reduce((s, t) => s + tags[t], 0);
  }
  node.reply(msg, { type: 'read_ok', value });
});

node.on('merge', (msg) => {
  for (const [item, tags] of Object.entries(msg.body.changes)) {
    changes[item] ??= {};
    Object.assign(changes[item], tags); // a tag's qty never changes, so copying is safe
  }
  for (const t of Object.keys(msg.body.removed)) removed[t] = true;
});

node.every(100, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', changes, removed });
});
