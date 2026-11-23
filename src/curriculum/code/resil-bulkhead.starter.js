// The plain service from the first task. When the upstream slows down,
// requests pile up inside this node, each one holding an upstream call open.
const UPSTREAM_TIMEOUT = 200;
const MAX_IN_FLIGHT = 4;   // upstream calls allowed at the same time

let inFlight = 0;          // upstream calls sent and not yet finished

node.on('get', async (msg) => {
  // TODO: if MAX_IN_FLIGHT calls are already out, refuse at once with
  // { type: 'error', code: 'overloaded' }. Otherwise count this call in, and
  // count it out again when it ends: on a reply, an error or a timeout.
  const res = await node.rpc('upstream', { type: 'fetch', key: msg.body.key }, { timeout: UPSTREAM_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, { type: 'get_ok', value: res.value });
});
