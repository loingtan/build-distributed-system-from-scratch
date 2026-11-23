// The single-flight cache from the last task, now on three nodes, plus a
// `put` that writes to the database. Nothing tells the caches about the
// write, so every node keeps serving the old value until it expires.
const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read
const DB_TIMEOUT = 300;    // ms to wait for the database before giving up

const cache = new Map();     // key -> { value, at }, least recently used first
const inflight = new Map();  // key -> promise of the read in progress

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

// TODO: an invalidate(key) that makes this node forget `key`. Deleting the
// entry is not enough:
//   - a read that started before the write may return after the
//     invalidation and must not put the old value back (keep a per-key
//     generation number, bump it here, and only store a read whose
//     generation has not changed);
//   - a get that arrives after the invalidation must not wait for a read
//     that started before it.

function load(key) {
  if (inflight.has(key)) return inflight.get(key);
  const read = (async () => {
    const sentAt = node.now();
    const res = await node.rpc('db', { type: 'read', key }, { timeout: DB_TIMEOUT });
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

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const res = await node.rpc('db', { type: 'write', key, value }, { timeout: DB_TIMEOUT });
  if (!res || res.type !== 'write_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  // TODO: invalidate the key here, and on every peer. Send each peer
  // { type: 'invalidate', key }, retry until it answers (messages can be
  // lost), and only reply put_ok once every peer has confirmed.
  node.reply(msg, { type: 'put_ok' });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', entries: cache.size });
});
