// Last-writer-wins registers, one per key. Each stored value carries a tag
// (ts, node). The tag with the higher ts wins; on equal ts the higher node
// id wins, so every replica orders any two tags the same way.
//
// ts comes from a Lamport clock: a write uses clock + 1, and every tag
// received pushes the clock forward. A write is therefore stamped later than
// every write its node had already seen.
const registers = {}; // key -> { value, ts, node }
let clock = 0;

function newer(x, y) {
  if (!y) return true;
  if (x.ts !== y.ts) return x.ts > y.ts;
  return x.node > y.node;
}

node.on('write', (msg) => {
  clock += 1;
  registers[msg.body.key] = { value: msg.body.value, ts: clock, node: node.id };
  node.reply(msg, { type: 'write_ok' });
});

node.on('read', (msg) => {
  const r = registers[msg.body.key];
  node.reply(msg, { type: 'read_ok', value: r ? r.value : null });
});

node.on('merge', (msg) => {
  for (const [key, entry] of Object.entries(msg.body.registers)) {
    clock = Math.max(clock, entry.ts);
    if (newer(entry, registers[key])) registers[key] = entry;
  }
});

node.every(100, () => {
  for (const peer of node.peers) node.send(peer, { type: 'merge', registers });
});
