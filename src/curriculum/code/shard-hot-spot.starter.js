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

const N = node.nodeIds.length;
const ownerOf = (key) => node.nodeIds[hash(key) % N];

const counters = new Map(); // key -> count, for the keys this node owns

// Every request for a key ends up on its one owner. That is fine for a
// hundred quiet keys, and a disaster when one of them gets most of the traffic.
// TODO: count requests per key on the owner; once a key is hot, tell every
// node, spread its increments over several nodes (salting) and have reads of
// it add up all the parts. Keep cold keys on their owner.
node.on('incr', async (msg) => {
  const { key, delta } = msg.body;
  const owner = ownerOf(key);
  if (owner !== node.id) {
    const res = await node.rpc(owner, { type: 'incr', key, delta });
    if (res) node.reply(msg, { type: 'incr_ok' });
    return;
  }
  counters.set(key, (counters.get(key) ?? 0) + delta);
  node.reply(msg, { type: 'incr_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) return node.reply(msg, { type: 'get_ok', value: counters.get(key) ?? 0 });
  const res = await node.rpc(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});
