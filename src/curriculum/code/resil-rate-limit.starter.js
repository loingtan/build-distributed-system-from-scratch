// The plain service from the first task: every request becomes an upstream
// call. Nothing stops a burst of clients from handing the upstream more than
// it can take.
const UPSTREAM_TIMEOUT = 200;
const RATE = 20;    // tokens added per second
const BURST = 10;   // most tokens the bucket can hold

let tokens = BURST;          // the bucket starts full
let refilledAt = node.now(); // when the token count was last brought up to date

// TODO: bring the token count up to date (elapsed ms * RATE / 1000, never
// above BURST), then take one if there is a whole token and return true.
// Otherwise return false.
function takeToken() {
  return true;
}

node.on('get', async (msg) => {
  if (!takeToken()) {
    node.reply(msg, { type: 'error', code: 'rate_limited' });
    return;
  }
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
