// A cart built by nesting what you have: an observed-remove set of item
// names, and a PN-counter of quantities per item.
// Run the tests: removing an item hides it, but its counter keeps its old
// value, so adding it again brings the removed quantity back.
// TODO: make a remove clear the quantity it has seen, and only that.
const tags = {}; // item -> { tag: true }
const removed = {}; // tag -> true
const up = {}; // item -> { node: total added }
const down = {}; // item -> { node: total subtracted }
let counter = 0;

const present = (item) => Object.keys(tags[item] || {}).some((t) => !removed[t]);
const sum = (table) => Object.values(table || {}).reduce((a, b) => a + b, 0);

function maxInto(mine, theirs) {
  for (const [item, perNode] of Object.entries(theirs)) {
    mine[item] ??= {};
    for (const [id, n] of Object.entries(perNode)) mine[item][id] = Math.max(mine[item][id] || 0, n);
  }
}

node.on('add', (msg) => {
  const { item, qty } = msg.body;
  counter += 1;
  tags[item] ??= {};
  tags[item][node.id + ':' + counter] = true;
  const table = qty >= 0 ? up : down;
  table[item] ??= {};
  table[item][node.id] = (table[item][node.id] || 0) + Math.abs(qty);
  node.reply(msg, { type: 'add_ok' });
});

node.on('remove', (msg) => {
  for (const t of Object.keys(tags[msg.body.item] || {})) removed[t] = true;
  node.reply(msg, { type: 'remove_ok' });
});

node.on('read', (msg) => {
  const value = {};
  for (const item of Object.keys(tags)) {
    if (present(item)) value[item] = sum(up[item]) - sum(down[item]);
  }
  node.reply(msg, { type: 'read_ok', value });
});

node.on('merge', (msg) => {
  for (const [item, ts] of Object.entries(msg.body.tags)) {
    tags[item] ??= {};
    for (const t of Object.keys(ts)) tags[item][t] = true;
  }
  for (const t of Object.keys(msg.body.removed)) removed[t] = true;
  maxInto(up, msg.body.up);
  maxInto(down, msg.body.down);
});

node.every(100, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', tags, removed, up, down });
});
