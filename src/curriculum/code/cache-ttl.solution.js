const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read

const cache = new Map();   // key -> { value, at }, least recently used first

function lookup(key) {
  const entry = cache.get(key);
  if (!entry) return undefined;
  if (node.now() - entry.at >= TTL) {
    // Expired: the database may have changed since. Treat it as a miss.
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

node.on('get', async (msg) => {
  const key = msg.body.key;
  const cached = lookup(key);
  if (cached !== undefined) {
    node.reply(msg, { type: 'get_ok', value: cached });
    return;
  }
  // Date the entry from when the read was sent: the value is at least that
  // fresh, and the time the reply spent travelling counts against the TTL.
  const sentAt = node.now();
  const res = await node.rpc('db', { type: 'read', key });
  if (!res || res.type !== 'read_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  store(key, res.value, sentAt);
  node.reply(msg, { type: 'get_ok', value: res.value });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', entries: cache.size });
});
