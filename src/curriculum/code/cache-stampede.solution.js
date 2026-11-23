const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read
const DB_TIMEOUT = 300;    // ms to wait for the database before giving up

const cache = new Map();     // key -> { value, at }, least recently used first
const inflight = new Map();  // key -> promise of the value (null if the read failed)

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

// Single flight: at most one read per key at a time. Every miss for a key
// that is already being read waits for that read instead of starting one.
function load(key) {
  if (inflight.has(key)) return inflight.get(key);
  const read = (async () => {
    const sentAt = node.now();
    const res = await node.rpc('db', { type: 'read', key }, { timeout: DB_TIMEOUT });
    // Forget the read whatever happened, or a failure would block the key forever.
    inflight.delete(key);
    if (!res || res.type !== 'read_ok') return null;
    store(key, res.value, sentAt);
    return res.value;
  })();
  inflight.set(key, read);
  return read;
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
