// An observed-remove set. Every add creates a unique tag. A remove deletes
// the tags this replica has seen for that element, and only those. An
// element is present while at least one of its tags has not been removed.
//
// Both tables only grow (tags are added, tombstones are added), so merging
// is a union, exactly as in the grow-only set.
const tags = {}; // element -> { tag: true }
const removed = {}; // tag -> true (tombstones)
let counter = 0;

const present = (element) => Object.keys(tags[element] || {}).some((t) => !removed[t]);

node.on('add', (msg) => {
  const element = msg.body.element;
  // A fresh tag even when the element is already present: this add must
  // survive a concurrent remove that could not have seen it.
  counter += 1;
  tags[element] ??= {};
  tags[element][node.id + ':' + counter] = true;
  node.reply(msg, { type: 'add_ok' });
});

node.on('remove', (msg) => {
  for (const t of Object.keys(tags[msg.body.element] || {})) removed[t] = true;
  node.reply(msg, { type: 'remove_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: Object.keys(tags).filter(present) });
});

node.on('merge', (msg) => {
  for (const [element, ts] of Object.entries(msg.body.tags)) {
    tags[element] ??= {};
    for (const t of Object.keys(ts)) tags[element][t] = true;
  }
  for (const t of Object.keys(msg.body.removed)) removed[t] = true;
});

node.every(100, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', tags, removed });
});
