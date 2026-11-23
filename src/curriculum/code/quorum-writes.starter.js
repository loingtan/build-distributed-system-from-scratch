// Leaderless replication, step 1: quorum writes.
// Every node is a replica of every key, and any node can coordinate a put.
const N = node.nodeIds.length;
const W = Math.floor(N / 2) + 1; // acknowledgements needed before put_ok

const store = new Map(); // key -> value: this replica's own copy

node.on('put', (msg) => {
  const { key, value } = msg.body;
  // This only writes the local copy, so one crash loses the write.
  // TODO: send the write to every replica (this node counts as one of them):
  //   const res = await node.rpc(peer, { type: 'store', key, value }, { timeout: 300 })
  // Reply put_ok as soon as W replicas have stored it. Do not wait for all N.
  // If W cannot be reached in time, reply { type: 'error', text: '...' }.
  store.set(key, value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('store', (msg) => {
  // TODO: a write from a coordinator. Keep it and reply { type: 'store_ok' }.
});

// Debug request used by the tests: this replica's own copy, no quorum.
node.on('local_get', (msg) => {
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'local_get_ok', value });
});
