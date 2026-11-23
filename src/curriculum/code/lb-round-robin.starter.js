// A load balancer that forwards every request and relays the answer.
// It works, but it sends everything to the first backend in the pool.
const BACKEND_TIMEOUT = 500;

let backends = []; // the pool, from the latest set_backends

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  node.reply(msg, { type: 'set_backends_ok' });
});

// TODO: give the backends turns: the first request to backends[0], the next
// to backends[1], and so on, starting over after the last one. The pool can
// change size between two requests.
function pick() {
  return backends[0];
}

node.on('get', async (msg) => {
  if (backends.length === 0) {
    node.reply(msg, { type: 'error', code: 'no_backends' });
    return;
  }
  const backend = pick();
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  node.reply(msg, res);
});
