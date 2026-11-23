// Two grow-only tables: everything ever added, everything ever subtracted.
// Each only grows, so each merges with max. The value is the difference.
const up = {};
const down = {};
for (const id of node.nodeIds) {
  up[id] = 0;
  down[id] = 0;
}

const sum = (table) => Object.values(table).reduce((a, b) => a + b, 0);

function mergeInto(mine, theirs) {
  for (const [id, n] of Object.entries(theirs)) mine[id] = Math.max(mine[id], n);
}

node.on('add', (msg) => {
  const delta = msg.body.delta;
  if (delta >= 0) up[node.id] += delta;
  else down[node.id] += -delta;
  node.reply(msg, { type: 'add_ok' });
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: sum(up) - sum(down) });
});

node.on('merge', (msg) => {
  mergeInto(up, msg.body.up);
  mergeInto(down, msg.body.down);
});

node.every(150, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', up, down });
});
