// Least outstanding requests from the previous task. It spreads load well,
// but a key lands wherever there is room at that moment. Each backend caches
// the keys it has served, so a key that wanders misses the cache every time.
const BACKEND_TIMEOUT = 1000;

let backends = [];   // the pool, from the latest set_backends
const inFlight = {}; // backend id -> requests in flight

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

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) inFlight[id] = inFlight[id] || 0;
  node.reply(msg, { type: 'set_backends_ok' });
});

// TODO: choose the backend from the key alone, so that every request for a
// key goes to the same backend while the pool is unchanged, and a change to
// the pool moves only the keys it has to.
function pick(key) {
  let best = [];
  let fewest = Infinity;
  for (const id of backends) {
    if (inFlight[id] < fewest) {
      fewest = inFlight[id];
      best = [id];
    } else if (inFlight[id] === fewest) {
      best.push(id);
    }
  }
  return best[Math.floor(node.random() * best.length)];
}

node.on('get', async (msg) => {
  if (backends.length === 0) {
    node.reply(msg, { type: 'error', code: 'no_backends' });
    return;
  }
  const backend = pick(msg.body.key);
  inFlight[backend] += 1;
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  inFlight[backend] -= 1;
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, res);
});
