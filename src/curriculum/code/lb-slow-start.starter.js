// Health checks and retries from the previous task. When a backend comes back,
// least outstanding routing sees an idle, fast backend and hands it a full
// share at once, before its caches are warm.
//
// TODO: remember when each backend came into service (a probe put it back, or
// a later set_backends added it; the very first pool is warm), give it a
// weight that grows from 0 to 1 over 3000ms, and send it about that fraction
// of an equal share until then.

const BACKEND_TIMEOUT = 250; // healthy backends answer within about 100ms
const PROBE_EVERY = 200;
const PROBE_TIMEOUT = 150;
const DOWN_AFTER = 2;        // failed probes in a row that take a backend out
const MAX_ATTEMPTS = 2;      // the first try and one retry elsewhere

let backends = [];   // the pool, from the latest set_backends
const inFlight = {}; // backend id -> requests in flight
const healthy = {};  // backend id -> true while in service
const failures = {}; // backend id -> failed probes in a row

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) {
    if (inFlight[id] === undefined) {
      inFlight[id] = 0;
      healthy[id] = true; // innocent until a probe says otherwise
      failures[id] = 0;
    }
  }
  node.reply(msg, { type: 'set_backends_ok' });
});

// Probe every backend, in service or not: a probe is the only way a backend
// that was taken out can come back.
async function probe(id) {
  const res = await node.rpc(id, { type: 'health' }, { timeout: PROBE_TIMEOUT });
  if (res && res.type === 'health_ok') {
    failures[id] = 0;
    healthy[id] = true;
  } else {
    failures[id] += 1;
    if (failures[id] >= DOWN_AFTER) healthy[id] = false;
  }
}

node.every(PROBE_EVERY, () => {
  for (const id of backends) probe(id);
});

// Fewest requests in flight among the healthy backends not tried yet.
function pick(tried) {
  let best = [];
  let fewest = Infinity;
  for (const id of backends) {
    if (!healthy[id] || tried.includes(id)) continue;
    if (inFlight[id] < fewest) {
      fewest = inFlight[id];
      best = [id];
    } else if (inFlight[id] === fewest) {
      best.push(id);
    }
  }
  return best.length ? best[Math.floor(node.random() * best.length)] : null;
}

node.on('get', async (msg) => {
  const tried = [];
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const backend = pick(tried);
    if (!backend) break;
    tried.push(backend);
    inFlight[backend] += 1;
    const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
    // Every attempt gives its slot back, however it ended.
    inFlight[backend] -= 1;
    if (res && res.type === 'get_ok') {
      node.reply(msg, res);
      return;
    }
  }
  node.reply(msg, { type: 'error', code: 'unavailable' });
});
