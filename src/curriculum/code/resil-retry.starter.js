// One attempt per request, with a timeout. A single lost or refused call
// now fails the whole request.
const UPSTREAM_TIMEOUT = 200;
const MAX_ATTEMPTS = 4;   // one first try plus three retries
const BASE_WAIT = 100;    // ceiling of the wait before the first retry, in ms

// await sleep(ms) pauses this request without blocking the node.
const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));

node.on('get', async (msg) => {
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });

  // TODO: three outcomes need three different reactions.
  //   res === null, or res.code === 'busy'   transient: wait, then try again,
  //                                          up to MAX_ATTEMPTS calls in all
  //   any other error (e.g. 'not_found')     permanent: pass the code on
  //   res.type === 'fetch_ok'                done
  // The wait before retry n is a random share of BASE_WAIT * 2^(n-1):
  //   node.random() * BASE_WAIT * Math.pow(2, n - 1)
  // When every attempt has failed, reply { type: 'error', code: 'unavailable' }.
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  if (res.type === 'error') {
    node.reply(msg, { type: 'error', code: res.code });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
