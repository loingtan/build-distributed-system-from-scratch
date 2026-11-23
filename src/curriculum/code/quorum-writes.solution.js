const N = node.nodeIds.length;
const W = Math.floor(N / 2) + 1;

const store = new Map(); // key -> value: this replica's own copy

// What this node does as a replica, whether the request came over the
// network or from itself acting as coordinator.
function local(body) {
  store.set(body.key, body.value);
  return { type: 'store_ok' };
}

// Send a request to every replica. Resolves with the answers as soon as enough
// (need) have answered, or with null once that has become impossible.
function ask(body, need, timeout) {
  return new Promise((resolve) => {
    const answers = [];
    let waiting = node.peers.length;
    let done = false;
    const got = (res) => {
      if (res) answers.push(res);
      if (done) return;
      if (answers.length >= need) {
        done = true;
        resolve(answers);
      } else if (waiting === 0) {
        done = true;
        resolve(null);
      }
    };
    for (const peer of node.peers) {
      node.rpc(peer, body, { timeout }).then((res) => {
        waiting -= 1;
        got(res);
      });
    }
    got(local(body)); // the coordinator is a replica too
  });
}

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  const acks = await ask({ type: 'store', key, value }, W, 300);
  if (acks) node.reply(msg, { type: 'put_ok' });
  else node.reply(msg, { type: 'error', text: 'fewer than ' + W + ' replicas acknowledged the write' });
});

node.on('store', (msg) => {
  node.reply(msg, local(msg.body));
});

node.on('local_get', (msg) => {
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'local_get_ok', value });
});
