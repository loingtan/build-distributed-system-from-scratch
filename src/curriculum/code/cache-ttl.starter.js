// The LRU cache from the last task. Once a value is cached it is served
// until it is evicted, however long ago it was read. In this task the
// database changes behind the cache's back, so old entries go stale.
const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read

const cache = new Map();   // key -> value, least recently used first

// TODO: store { value, at } instead of the bare value, where `at` is when
// you read it (node.now() just before sending the read is the safe choice).
// In lookup, an entry with node.now() - at >= TTL is a miss: delete it.
function lookup(key) {
  if (!cache.has(key)) return undefined;
  const value = cache.get(key);
  cache.delete(key);
  cache.set(key, value);
  return value;
}

function store(key, value) {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > CAPACITY) cache.delete(cache.keys().next().value);
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
