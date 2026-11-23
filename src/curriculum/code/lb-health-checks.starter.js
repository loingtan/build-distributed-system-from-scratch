// Least outstanding requests from the first subtrack. It knows nothing about
// failures: a request sent to a dead backend times out and the client gets an
// error, and a backend that fails fast looks like the least busy of all.
const BACKEND_TIMEOUT = 250; // healthy backends answer within about 100ms

let backends = [];   // the pool, from the latest set_backends
const inFlight = {}; // backend id -> requests in flight

// TODO: keep track of which backends are healthy.
//   - every 200ms or so, send { type: 'health' } to every backend
//   - { type: 'health_ok' } means healthy; no answer, or an error, is a failure
//   - two failed probes in a row take a backend out of service, one good probe
//     puts it back
const healthy = {};  // backend id -> true while in service

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) {
    if (inFlight[id] === undefined) {
      inFlight[id] = 0;
      healthy[id] = true;
    }
  }
  node.reply(msg, { type: 'set_backends_ok' });
});

// TODO: choose only among healthy backends you have not tried yet for this request.
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
  return best.length ? best[Math.floor(node.random() * best.length)] : null;
}

// TODO: if the answer is not get_ok (a timeout or an error), retry once on
// another backend before giving up.
node.on('get', async (msg) => {
  const backend = pick();
  if (!backend) {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  inFlight[backend] += 1;
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  inFlight[backend] -= 1;
  if (res && res.type === 'get_ok') {
    node.reply(msg, res);
    return;
  }
  node.reply(msg, { type: 'error', code: 'unavailable' });
});
