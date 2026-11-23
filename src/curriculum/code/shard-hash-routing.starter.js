const store = new Map(); // the keys stored on this node

// A deterministic string hash (FNV-1a with a final mix). The same string gives
// the same 32-bit number (0 .. 4294967295) on every node, every time.
function hash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

// TODO: decide which node owns a key. Every node must reach the same answer
// on its own, so it can only depend on the key and on node.nodeIds.
function ownerOf(key) {
  return node.id;
}

// For now every node keeps whatever it is handed, so a value written through
// n1 cannot be read through n2.
// TODO: if ownerOf(key) is another node, forward the request to it with
// node.rpc(owner, { type: 'put', key, value }) and relay the answer.
node.on('put', (msg) => {
  const { key, value } = msg.body;
  store.set(key, value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('get', (msg) => {
  const { key } = msg.body;
  node.reply(msg, { type: 'get_ok', value: store.has(key) ? store.get(key) : null });
});

// The tests use this to see where each key really lives.
node.on('local_keys', (msg) => {
  node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] });
});
