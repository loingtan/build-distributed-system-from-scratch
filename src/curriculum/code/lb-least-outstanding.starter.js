// Round robin from the previous task. It shares requests evenly, but some
// backends are much slower than others: an equal share of requests to a slow
// backend means a quarter of all clients wait on it while the others idle.
const BACKEND_TIMEOUT = 1000;

let backends = []; // the pool, from the latest set_backends
let next = 0;

// TODO: keep, for every backend, the number of requests sent to it that have
// not finished yet, and pick the backend where that number is smallest.
const inFlight = {}; // backend id -> requests in flight

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) inFlight[id] = inFlight[id] || 0;
  node.reply(msg, { type: 'set_backends_ok' });
});

function pick() {
  const backend = backends[next % backends.length];
  next += 1;
  return backend;
}

node.on('get', async (msg) => {
  if (backends.length === 0) {
    node.reply(msg, { type: 'error', code: 'no_backends' });
    return;
  }
  const backend = pick();
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, res);
});
