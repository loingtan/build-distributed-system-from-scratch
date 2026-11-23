const CAPACITY = 20;
const TTL = 1000;          // ms an entry may be served after it was read
const DB_TIMEOUT = 300;    // ms to wait for the database before giving up

const cache = new Map();     // key -> { value, at }, least recently used first
const inflight = new Map();  // key -> { gen, promise } of the read in progress
const gen = new Map();       // key -> generation, bumped by every invalidation

const genOf = (key) => gen.get(key) || 0;

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

// Forget everything this node knows about `key`: the entry, the read in
// flight (later misses must start a fresh read), and, through the new
// generation, the right of any older read to fill the cache when it returns.
function invalidate(key) {
  gen.set(key, genOf(key) + 1);
  cache.delete(key);
  inflight.delete(key);
}

function load(key) {
  const current = inflight.get(key);
  if (current) return current.promise;
  const entry = { gen: genOf(key), promise: null };
  entry.promise = (async () => {
    const sentAt = node.now();
    const res = await node.rpc('db', { type: 'read', key }, { timeout: DB_TIMEOUT });
    if (inflight.get(key) === entry) inflight.delete(key);
    if (!res || res.type !== 'read_ok') return null;
    // Store only if no invalidation happened while the read was out: the
    // value may have been read before the write it would now hide.
    if (genOf(key) === entry.gen) store(key, res.value, sentAt);
    return res.value;
  })();
  inflight.set(key, entry);
  return entry.promise;
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

// Ask one peer to drop `key`, until it confirms. Messages can be lost.
async function tell(peer, key) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const ack = await node.rpc(peer, { type: 'invalidate', key }, { timeout: 100 });
    if (ack) return;
  }
}

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const res = await node.rpc('db', { type: 'write', key, value }, { timeout: DB_TIMEOUT });
  if (!res || res.type !== 'write_ok') {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  // Delete rather than store the new value: two puts racing through two
  // nodes could otherwise leave the older value cached.
  invalidate(key);
  await Promise.all(node.peers.map((peer) => tell(peer, key)));
  // Only now can no node serve the old value: acknowledge.
  node.reply(msg, { type: 'put_ok' });
});

node.on('invalidate', (msg) => {
  invalidate(msg.body.key);
  node.reply(msg, { type: 'invalidate_ok' });
});

node.on('stats', (msg) => {
  node.reply(msg, { type: 'stats_ok', entries: cache.size });
});
