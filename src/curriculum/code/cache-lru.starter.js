// The cache-aside node from the last task. It never forgets anything, so a
// long scan of keys that are asked for once fills its memory without limit.
const CAPACITY = 20;       // most entries a node may hold

const cache = new Map();   // key -> value

// TODO: make this an LRU cache.
//   lookup: a hit counts as a use. Move the key to the "recently used" end.
//           (A Map keeps insertion order: delete the key and set it again.)
//   store:  after adding a key, while there are more than CAPACITY entries,
//           evict the least recently used one (the Map's first key:
//           cache.keys().next().value).
function lookup(key) {
  return cache.get(key);
}

function store(key, value) {
  cache.set(key, value);
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
