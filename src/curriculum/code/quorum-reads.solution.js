const N = node.nodeIds.length;
const W = Math.floor(N / 2) + 1;
const R = N - W + 1; // the smallest R with W + R > N

// key -> { value, counter, origin }. (counter, origin) is the version.
const store = new Map();

function newer(a, b) {
  if (!a) return false;
  if (!b) return true;
  return a.counter > b.counter || (a.counter === b.counter && a.origin > b.origin);
}

function newest(entries) {
  let best = null;
  for (const e of entries) if (newer(e, best)) best = e;
  return best;
}

// What this node does as a replica.
function local(body) {
  if (body.type === 'store') {
    // Writes can arrive late or twice; only a newer version replaces ours.
    if (newer(body.entry, store.get(body.key))) store.set(body.key, body.entry);
    return { type: 'store_ok' };
  }
  return { type: 'read_ok', entry: store.get(body.key) || null };
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
    got(local(body));
  });
}

node.on('put', async (msg) => {
  const { key, value } = msg.body;
  // This node may have missed writes, so its own copy cannot tell it which
  // version comes next. R replicas together can: one of them saw the last write.
  const seen = await ask({ type: 'read', key }, R, 300);
  if (!seen) return node.reply(msg, { type: 'error', text: 'fewer than ' + R + ' replicas answered' });
  const latest = newest(seen.map((a) => a.entry));
  const entry = { value, counter: (latest ? latest.counter : 0) + 1, origin: node.id };

  const acks = await ask({ type: 'store', key, entry }, W, 300);
  if (acks) node.reply(msg, { type: 'put_ok' });
  else node.reply(msg, { type: 'error', text: 'fewer than ' + W + ' replicas acknowledged the write' });
});

node.on('get', async (msg) => {
  const answers = await ask({ type: 'read', key: msg.body.key }, R, 300);
  if (!answers) return node.reply(msg, { type: 'error', text: 'fewer than ' + R + ' replicas answered' });
  const latest = newest(answers.map((a) => a.entry));
  node.reply(msg, { type: 'get_ok', value: latest ? latest.value : null });
});

node.on('store', (msg) => node.reply(msg, local(msg.body)));
node.on('read', (msg) => node.reply(msg, local(msg.body)));

node.on('local_get', (msg) => {
  const entry = store.get(msg.body.key);
  node.reply(msg, { type: 'local_get_ok', value: entry ? entry.value : null });
});
