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

// Records are partitioned by key. The index is partitioned by term: the list
// of keys for one color lives on one node, wherever the records are.
const ownerOf = (key) => node.nodeIds[hash(key) % N];
const indexNodeOf = (color) => node.nodeIds[hash('color:' + color) % N];

const records = new Map(); // key -> { value, version }, for the keys this node owns
const index = new Map();   // color -> Map(key -> { version, present }), for the colors this node indexes

// Index updates travel on their own and may overtake each other. Each one
// carries the version of the record that caused it, and an entry changes only
// for a newer version, so the outcome does not depend on arrival order.
// A removal is kept as { present: false } so that an older add arriving
// later cannot bring the key back.
function applyIndex({ color, key, version, present }) {
  if (!index.has(color)) index.set(color, new Map());
  const entries = index.get(color);
  const cur = entries.get(key);
  if (!cur || version > cur.version) entries.set(key, { version, present });
}

function sendIndex(update) {
  const dest = indexNodeOf(update.color);
  if (dest === node.id) applyIndex(update);
  else node.send(dest, { type: 'index', ...update });
}

node.on('index', (msg) => applyIndex(msg.body));

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
  // Asynchronous index maintenance: reply now, the index catches up shortly.
  if (old && old.value.color !== value.color) sendIndex({ color: old.value.color, key, version, present: false });
  sendIndex({ color: value.color, key, version, present: true });
  node.reply(msg, { type: 'put_ok' });
});

node.on('get', async (msg) => {
  const { key } = msg.body;
  const owner = ownerOf(key);
  if (owner === node.id) return node.reply(msg, { type: 'get_ok', value: records.has(key) ? records.get(key).value : null });
  const res = await node.rpc(owner, { type: 'get', key });
  if (res) node.reply(msg, { type: 'get_ok', value: res.value });
});

function lookup(color) {
  const entries = index.get(color) ?? new Map();
  return [...entries].filter(([, e]) => e.present).map(([key]) => key);
}

node.on('index_lookup', (msg) => {
  node.reply(msg, { type: 'index_lookup_ok', keys: lookup(msg.body.color) });
});

// One node holds the whole answer: no scatter, no gather.
node.on('find', async (msg) => {
  const { color } = msg.body;
  const dest = indexNodeOf(color);
  if (dest === node.id) return node.reply(msg, { type: 'find_ok', keys: lookup(color) });
  const res = await node.rpc(dest, { type: 'index_lookup', color });
  if (res) node.reply(msg, { type: 'find_ok', keys: res.keys });
});
