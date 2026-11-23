const UPSTREAM_TIMEOUT = 200;
const MAX_IN_FLIGHT = 4;   // upstream calls allowed at the same time

let inFlight = 0;          // upstream calls sent and not yet finished

node.on('get', async (msg) => {
  if (inFlight >= MAX_IN_FLIGHT) {
    // No queue: a request that waited would only be answered late, and the
    // queue would hide the overload from the client.
    node.reply(msg, { type: 'error', code: 'overloaded' });
    return;
  }
  inFlight += 1;
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
  // Give the slot back before looking at the result, so that no outcome
  // (reply, error, timeout) can leak it.
  inFlight -= 1;
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
