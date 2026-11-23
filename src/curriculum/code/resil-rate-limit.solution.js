const UPSTREAM_TIMEOUT = 200;
const RATE = 20;    // tokens added per second
const BURST = 10;   // most tokens the bucket can hold

let tokens = BURST;
let refilledAt = node.now();

// No timer needed: work out how many tokens have dripped in since the last
// look. Fractions are kept, so 30ms of waiting is not thrown away.
function takeToken() {
  const now = node.now();
  tokens = Math.min(BURST, tokens + ((now - refilledAt) * RATE) / 1000);
  refilledAt = now;
  if (tokens < 1) return false;
  tokens -= 1;
  return true;
}

node.on('get', async (msg) => {
  if (!takeToken()) {
    // Refusing is cheap and immediate; the upstream never hears of it.
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
