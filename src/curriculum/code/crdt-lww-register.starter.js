// Last-writer-wins registers, one per key, stamped with a Lamport clock.
// Run the tests: two writes on opposite sides of a partition can get the
// same timestamp, and then each replica keeps the one it had.
// TODO: make the order of tags total, so every replica picks the same winner.
const registers = {}; // key -> { value, ts, node }
let clock = 0;

function newer(x, y) {
  if (!y) return true;
  return x.ts > y.ts;
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
