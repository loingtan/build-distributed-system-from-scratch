const store = new Map(); // the keys this node owns

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

let members = [...node.nodeIds]; // the nodes that currently own keys

// This is the previous task's rule, applied to the member list. It routes
// correctly, but when the list changes length almost every key gets a new owner.
// TODO: replace it with a hash ring.
//   - Give each member a position: hash(id).
//   - Keep the positions sorted.
//   - A key belongs to the first member at or after hash(key), wrapping
//     round to the smallest position after the largest.
function ownerOf(key) {
  return members[hash(key) % members.length];
}

node.on('set_members', (msg) => {
  members = [...msg.body.members].sort();
  node.reply(msg, { type: 'set_members_ok' });
});

node.on('lookup', (msg) => {
  node.reply(msg, { type: 'lookup_ok', owners: msg.body.keys.map(ownerOf) });
});

// put and get take the same path: do the work here if this node owns the
// key, otherwise pass the request to the owner and relay its answer.
node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    store.set(key, value);
    return node.reply(msg, { type: 'put_ok' });
  }
  const res = await node.rpc(owner, { type: 'put', key, value });
  if (res) node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) {
    return node.reply(msg, { type: 'get_ok', value: store.has(key) ? store.get(key) : null });
  }
  const res = await node.rpc(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('local_keys', (msg) => {
  node.reply(msg, { type: 'local_keys_ok', keys: [...store.keys()] });
});
