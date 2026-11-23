const BACKEND_TIMEOUT = 250;
const PROBE_EVERY = 200;
const PROBE_TIMEOUT = 150;
const DOWN_AFTER = 2;
const MAX_ATTEMPTS = 2;
const SLOW_START = 3000;     // ms for a backend to ramp from nothing to a full share

let backends = [];
let firstList = true;        // the very first pool is warm
const inFlight = {};
const healthy = {};
const failures = {};
const upSince = {};          // backend id -> when it came into service; unset for warm backends

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  for (const id of backends) {
    if (inFlight[id] === undefined) {
      inFlight[id] = 0;
      healthy[id] = true;
      failures[id] = 0;
      if (!firstList) upSince[id] = node.now(); // a new backend starts cold
    }
  }
  firstList = false;
  node.reply(msg, { type: 'set_backends_ok' });
});

async function probe(id) {
  const res = await node.rpc(id, { type: 'health' }, { timeout: PROBE_TIMEOUT });
  if (res && res.type === 'health_ok') {
    failures[id] = 0;
    if (!healthy[id]) {
      healthy[id] = true;
      upSince[id] = node.now(); // back from an outage: cold again
    }
  } else {
    failures[id] += 1;
    if (failures[id] >= DOWN_AFTER) healthy[id] = false;
  }
}

node.every(PROBE_EVERY, () => {
  for (const id of backends) probe(id);
});

// 0 when a backend has just come into service, 1 from SLOW_START ms later.
function weight(id) {
  if (upSince[id] === undefined) return 1;
  return Math.min(1, (node.now() - upSince[id]) / SLOW_START);
}

function leastInFlight(ids) {
  let best = [];
  let fewest = Infinity;
  for (const id of ids) {
    if (inFlight[id] < fewest) {
      fewest = inFlight[id];
      best = [id];
    } else if (inFlight[id] === fewest) {
      best.push(id);
    }
  }
  return best.length ? best[Math.floor(node.random() * best.length)] : null;
}

// Two steps. A warming backend is idle and fast, so least-outstanding alone
// would hand it a full share at once. First give each warming backend this
// request with probability weight / total weight; otherwise choose among the
// warm backends by fewest requests in flight.
function pick(tried) {
  const candidates = backends.filter((id) => healthy[id] && !tried.includes(id));
  if (candidates.length === 0) return null;
  const warm = candidates.filter((id) => weight(id) >= 1);
  const warming = candidates.filter((id) => weight(id) < 1);
  let total = 0;
  for (const id of candidates) total += weight(id);
  let r = node.random() * total;
  for (const id of warming) {
    if (r < weight(id)) return id;
    r -= weight(id);
  }
  return warm.length ? leastInFlight(warm) : leastInFlight(warming);
}

node.on('get', async (msg) => {
  const tried = [];
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const backend = pick(tried);
    if (!backend) break;
    tried.push(backend);
    inFlight[backend] += 1;
    const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
    inFlight[backend] -= 1;
    if (res && res.type === 'get_ok') {
      node.reply(msg, res);
      return;
    }
  }
  node.reply(msg, { type: 'error', code: 'unavailable' });
});
