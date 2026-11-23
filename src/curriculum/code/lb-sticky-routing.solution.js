const BACKEND_TIMEOUT = 1000;

let backends = []; // the pool, from the latest set_backends

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

// Rendezvous (highest random weight) hashing: every backend gets a score for
// this key, and the highest score wins. The score depends only on the key and
// the backend's id, so the order of the list does not matter, and a change to
// the pool only affects the keys whose winner left or whose new winner joined.
function pick(key) {
  let best = null;
  let bestScore = -1;
  for (const id of backends) {
    const score = hash(key + '/' + id);
    if (score > bestScore || (score === bestScore && id < best)) {
      best = id;
      bestScore = score;
    }
  }
  return best;
}

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  node.reply(msg, { type: 'set_backends_ok' });
});

node.on('get', async (msg) => {
  if (backends.length === 0) {
    node.reply(msg, { type: 'error', code: 'no_backends' });
    return;
  }
  const backend = pick(msg.body.key);
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, res);
});
