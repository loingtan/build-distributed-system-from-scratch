const UPSTREAM_TIMEOUT = 200;
const FAILURE_THRESHOLD = 5;   // failures in a row that open the breaker
const COOL_DOWN = 1000;        // ms to stay open before probing

let state = 'closed';          // 'closed' | 'open' | 'half_open'
let failures = 0;              // consecutive failures while closed
let openedAt = 0;

function open() {
  state = 'open';
  openedAt = node.now();
  failures = 0;
}

// May this request call the upstream?
function allow() {
  if (state === 'closed') return true;
  if (state === 'open' && node.now() - openedAt >= COOL_DOWN) {
    // Cool-down over: this request is the single probe.
    state = 'half_open';
    return true;
  }
  return false; // open, or half-open with the probe still in flight
}

function record(success, wasProbe) {
  if (wasProbe) {
    // The probe alone decides: closed again, or another full cool-down.
    if (success) {
      state = 'closed';
      failures = 0;
    } else {
      open();
    }
    return;
  }
  // Results of calls sent before the breaker opened still trickle in. They
  // say nothing new, so they are ignored unless the breaker is closed.
  if (state !== 'closed') return;
  if (success) {
    failures = 0;
    return;
  }
  failures += 1;
  if (failures >= FAILURE_THRESHOLD) open();
}

node.on('get', async (msg) => {
  if (!allow()) {
    node.reply(msg, { type: 'error', code: 'circuit_open' });
    return;
  }
  const wasProbe = state === 'half_open';
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
  const success = !!res && res.type === 'fetch_ok';
  record(success, wasProbe);
  if (success) node.reply(msg, { type: 'get_ok', value: res.value });
  else node.reply(msg, { type: 'error', code: res ? res.code : 'timeout' });
});
