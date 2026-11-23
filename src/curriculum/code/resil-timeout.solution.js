// Longer than the upstream's slowest normal answer (about 175ms seen from
// here), short enough that the reply still reaches the client inside 300ms.
const UPSTREAM_TIMEOUT = 200;

node.on('get', async (msg) => {
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
  if (!res) {
    // Nothing came back. We do not know whether the upstream is slow or
    // dead, only that the client should not wait any longer.
    node.reply(msg, { type: 'error', code: 'timeout', text: 'upstream did not answer in ' + UPSTREAM_TIMEOUT + 'ms' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
