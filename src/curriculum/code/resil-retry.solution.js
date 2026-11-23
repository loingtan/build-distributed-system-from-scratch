const UPSTREAM_TIMEOUT = 200;
const MAX_ATTEMPTS = 4;   // one first try plus three retries
const BASE_WAIT = 100;    // ceiling of the wait before the first retry, in ms

const sleep = (ms) => new Promise((resolve) => node.after(ms, resolve));

node.on('get', async (msg) => {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    if (attempt > 1) {
      // Exponential backoff with full jitter: the ceiling doubles after
      // every failure (100, 200, 400ms) and the actual wait is a random
      // share of it, so requests that failed together do not return together.
      const ceiling = BASE_WAIT * Math.pow(2, attempt - 2);
      await sleep(node.random() * ceiling);
    }
    const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
    if (res && res.type === 'fetch_ok') {
      node.reply(msg, { type: 'get_ok', value: res.value });
      return;
    }
    // Silence and "busy" may pass. Anything else will be the same next time.
    if (res && res.code !== 'busy') {
      node.reply(msg, { type: 'error', code: res.code });
      return;
    }
  }
  // Worst case so far: 4 timeouts of 200ms plus waits of at most 700ms.
  node.reply(msg, { type: 'error', code: 'unavailable', text: 'gave up after ' + MAX_ATTEMPTS + ' attempts' });
});
