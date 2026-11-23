const CAPACITY = 20;

// A Map remembers insertion order, which is all an LRU list needs: the first
// key is the least recently used, the last key the most recently used.
const cache = new Map();   // key -> value

function lookup(key) {
  if (!cache.has(key)) return undefined;
  const value = cache.get(key);
  // A hit is a use: move the key to the most recently used end.
  cache.delete(key);
  cache.set(key, value);
  return value;
}

function store(key, value) {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > CAPACITY) {
    // Evict the least recently used key: the first one in the Map.
    cache.delete(cache.keys().next().value);
  }
}

node.on('get', async (msg) => {
  const key = msg.body.key;
  const cached = lookup(key);
  if (cached !== undefined) {
    node.reply(msg, { type: 'get_ok', value: cached });
    return;
  }
  const res = await node.rpc('db', { type: 'read', key });
  if (!res || res.type !== 'read_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  store(key, res.value);
  node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', entries: cache.size });
});
