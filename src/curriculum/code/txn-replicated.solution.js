// key -> { value, ts, by }: the value plus a tag saying when and where it
// was written. ts is a Lamport timestamp, by is the writer's node id.
const store = new Map();
let clock = 0;

// Last writer wins. Timestamps can tie, so the node id breaks ties. Every
// node applies the same rule, so every node picks the same winner.
function newer(a, b) {
  return !b || a.ts > b.ts || (a.ts === b.ts && a.by > b.by);
}

node.on('txn', (msg) => {
  clock += 1;
  const result = msg.body.txn.map(([op, key, value]) => {
    if (op === 'r') return ['r', key, store.get(key)?.value ?? null];
    store.set(key, { value, ts: clock, by: node.id });
    return ['w', key, value];
  });
  // Reply immediately. Never wait for peers: they may be unreachable.
  node.reply(msg, { type: 'txn_ok', txn: result });
});

node.on('sync', (msg) => {
  for (const [key, incoming] of msg.body.entries) {
    clock = Math.max(clock, incoming.ts);
    if (newer(incoming, store.get(key))) store.set(key, incoming);
  }
});

// Anti-entropy: ship the whole store to every peer, over and over.
node.every(200, () => {
  if (store.size === 0) return;
  const entries = [...store];
  for (const peer of node.peers) node.send(peer, { type: 'sync', entries });
});
