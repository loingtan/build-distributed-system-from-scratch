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

const records = new Map(); // key -> { value, version }, for the keys this node owns

// TODO: a global index, partitioned by term. Pick one node per color, for
// example node.nodeIds[hash('color:' + color) % N], to hold the list of keys
// with that color. When a put changes a record, tell the index node of the old
// color and of the new one. Those updates can overtake each other on the way:
// use the record's version so an older update never undoes a newer one.

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const owner = ownerOf(key);
  if (owner !== node.id) {
    const res = await node.rpc(owner, { type: 'put', key, value });
    if (res) node.reply(msg, { type: 'put_ok' });
    return;
  }
  const old = records.get(key);
  const version = (old ? old.version : 0) + 1;
  records.set(key, { value, version });
  node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) return node.reply(msg, { type: 'get_ok', value: records.has(key) ? records.get(key).value : null });
  const res = await node.rpc(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

// The keys of this node's own records that have the color: a local index.
function localMatches(color) {
  return [...records].filter(([, r]) => r.value.color === color).map(([key]) => key);
}

node.on('find_local', (msg) => {
  node.reply(msg, { type: 'find_local_ok', keys: localMatches(msg.body.color) });
});

// For now a find scatters to every node and gathers the answers. Always
// correct, and every query costs a message to every node.
node.on('find', async (msg) => {
  const { color } = msg.body;
  const parts = await Promise.all(
    node.nodeIds.map(async (id) => {
      if (id === node.id) return localMatches(color);
      const res = await node.rpc(id, { type: 'find_local', color });
      return res ? res.keys : null;
    }),
  );
  if (parts.includes(null)) return;
  node.reply(msg, { type: 'find_ok', keys: parts.flat() });
});
