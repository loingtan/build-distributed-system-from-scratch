// The LRU cache with expiry from the last task. When a popular key is
// missing (cold start, or its entry just expired), every get that arrives
// during the database read is a miss too, and each sends its own read.
const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read
const DB_TIMEOUT = 300;    // ms to wait for the database before giving up

const cache = new Map();     // key -> { value, at }, least recently used first
const inflight = new Map();  // TODO: key -> promise of the read in progress

function lookup(key) {
  const entry = cache.get(key);
  if (!entry) return undefined;
  if (node.now() - entry.at >= TTL) {
    cache.delete(key);
    return undefined;
  }
  cache.delete(key);
  cache.set(key, entry);
  return entry.value;
}

function store(key, value, at) {
  cache.delete(key);
  cache.set(key, { value, at });
  while (cache.size > CAPACITY) cache.delete(cache.keys().next().value);
}

// Reads `key` from the database. Resolves to the value, or null on failure.
// TODO: if a read for this key is already in flight, return its promise
// instead of starting another. Remove the key from `inflight` when the read
// ends, however it ends.
async function load(key) {
  const sentAt = node.now();
  const res = await node.rpc('db', { type: 'read', key }, { timeout: DB_TIMEOUT });
  if (!res || res.type !== 'read_ok') return null;
  store(key, res.value, sentAt);
  return res.value;
}

node.on('get', async (msg) => {
  const key = msg.body.key;
  const cached = lookup(key);
  if (cached !== undefined) {
    node.reply(msg, { type: 'get_ok', value: cached });
    return;
  }
  const value = await load(key);
  if (value === null) {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', entries: cache.size });
});
