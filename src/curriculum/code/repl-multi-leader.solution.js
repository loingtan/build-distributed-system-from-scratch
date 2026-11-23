// Every node is a leader: it accepts writes to any key, answers at once and
// replicates afterwards. Concurrent writes to the same key are settled by one
// rule that every node applies: the higher timestamp wins.
//
// A timestamp is [counter, nodeId]. Compare counters first; on a tie, the
// higher node id (as strings) wins. No two writes share a timestamp.
const store = new Map(); // key -> { value, ts }
let clock = 0; // a Lamport clock

function newer(a, b) {
  if (a[0] !== b[0]) return a[0] > b[0];
  return a[1] > b[1];
}

// Merging is the same everywhere and in any order, so nodes converge.
function merge(key, value, ts) {
  // Lamport rule: never fall behind a timestamp we have seen. Then a write
  // accepted here always beats every value this node already had.
  clock = Math.max(clock, ts[0]);
  const current = store.get(key);
  if (!current || newer(ts, current.ts)) store.set(key, { value, ts });
}

function entries() {
  const out = [];
  for (const [key, e] of store) out.push([key, e.value, e.ts]);
  return out;
}

node.on('write', (msg) => {
  const { key, value } = msg.body;
  clock += 1;
  const ts = [clock, node.id];
  merge(key, value, ts);
  node.reply(msg, { type: 'write_ok', ts });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', entries: [[key, value, ts]] });
});

node.on('replicate', (msg) => {
  for (const [key, value, ts] of msg.body.entries) merge(key, value, ts);
});

// Anti-entropy: messages lost during a partition are never resent, so every
// 100ms each node sends its whole (small) store to every peer.
node.every(100, () => {
  const all = entries();
  if (all.length) for (const peer of node.peers) node.send(peer, { type: 'replicate', entries: all });
});

node.on('read', (msg) => {
  const e = store.get(msg.body.key);
  node.reply(msg, { type: 'read_ok', value: e ? e.value : null });
});
