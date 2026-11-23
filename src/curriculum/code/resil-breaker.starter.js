// One attempt per request, with a timeout (retries are left out of this task
// to keep the code short). During an outage every request still calls the
// upstream and waits the full 200ms to learn what the last hundred learned.
const UPSTREAM_TIMEOUT = 200;
const FAILURE_THRESHOLD = 5;   // failures in a row that open the breaker
const COOL_DOWN = 1000;        // ms to stay open before probing

// The breaker's state:
let state = 'closed';          // 'closed' | 'open' | 'half_open'
let failures = 0;              // consecutive failures while closed
let openedAt = 0;              // node.now() when the breaker last opened

// TODO: decide whether a request may call the upstream.
//   closed     yes
//   open       no, unless COOL_DOWN has passed: then become half_open and
//              let this one request through as the probe
//   half_open  no (the probe is already on its way)
function allow() {
  return true;
}

// TODO: record the result of a call.
//   success    reset the failure count; a successful probe closes the breaker
//   failure    count it; FAILURE_THRESHOLD in a row, or a failed probe,
//              opens the breaker and sets openedAt
function record(success, wasProbe) {}

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
