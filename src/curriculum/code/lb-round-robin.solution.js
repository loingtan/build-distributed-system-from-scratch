const BACKEND_TIMEOUT = 500;

let backends = []; // the pool, from the latest set_backends
let next = 0;      // whose turn it is; only ever grows

node.on('set_backends', (msg) => {
  backends = msg.body.backends;
  node.reply(msg, { type: 'set_backends_ok' });
});

node.on('get', async (msg) => {
  if (backends.length === 0) {
    node.reply(msg, { type: 'error', code: 'no_backends' });
    return;
  }
  // Pick before the await: other requests run while this one waits, and each
  // of them must see the counter already moved on.
  const backend = backends[next % backends.length];
  next += 1;
  const res = await node.rpc(backend, { type: 'get', key: msg.body.key }, { timeout: BACKEND_TIMEOUT });
  if (!res) {
    node.reply(msg, { type: 'error', code: 'timeout' });
    return;
  }
  // The backend's answer goes back as it is. Awaiting inside this handler
  // keeps it paired with this client's request, whatever order answers arrive in.
  node.reply(msg, res);
});
