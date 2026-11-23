const BACKEND_TIMEOUT = 1000;

let backends = [];  // the pool, from the latest set_backends
const inFlight = {}; // backend id -> requests sent to it and not yet finished

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) inFlight[id] = inFlight[id] || 0;
  node.reply(msg, { type: 'set_backends_ok' });
});

// The backend with the fewest requests in flight. Ties are broken at random:
// with a fixed order the first backend in the list would win every tie.
function pick() {
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
  const backend = pick();
  // Counted the moment it is sent, so the next request in a burst already sees it.
  inFlight[backend] += 1;
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  inFlight[backend] -= 1;
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, res);
});
