// Your last-writer-wins registers, now answering reads with a list.
// Run the tests: of two writes made on opposite sides of a partition, one
// silently disappears. A multi-value register keeps both.
// TODO: replace the (ts, node) tag with version vectors, keep concurrent
// versions as siblings, and let a write replace every version it has seen.
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
  node.reply(msg, { type: 'read_ok', values: r ? [r.value] : [] });
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
