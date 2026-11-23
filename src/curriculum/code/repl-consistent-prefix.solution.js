// There is no primary: every node accepts writes and sends them to the others.
// A write is named by its origin (the node that accepted it) and its number
// there. `applied[origin]` counts the writes from each origin this node has
// applied; it is the node's state in a nutshell, and it is also the token.
const store = new Map(); // key -> value
const applied = {};
for (const id of node.nodeIds) applied[id] = 0;
const pending = []; // replicated writes that cannot be applied yet

// Does this node have everything in vector v (a token or a write's deps)?
function covers(v) {
  if (!v) return true;
  for (const id of Object.keys(v)) if ((applied[id] || 0) < v[id]) return false;
  return true;
}

// Apply what is ready: the next write from its origin, whose dependencies
// are all here. Applying one write can make another ready, so loop.
function drain() {
  let progress = true;
  while (progress) {
    progress = false;
    for (let i = 0; i < pending.length; i++) {
      const e = pending[i];
      const done = e.n <= applied[e.origin];
      if (done || (e.n === applied[e.origin] + 1 && covers(e.deps))) {
        if (!done) {
          store.set(e.key, e.value);
          applied[e.origin] = e.n;
          progress = true;
        }
        pending.splice(i, 1);
        i -= 1;
      }
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => node.after(ms, resolve));
}

// Wait until this node has seen everything the client has seen.
async function catchUp(token, limit) {
  const deadline = node.now() + limit;
  while (!covers(token)) {
    if (node.now() >= deadline) return false;
    await sleep(5);
  }
  return true;
}

node.on('write', async (msg) => {
  const { key, value, token } = msg.body;
  if (!(await catchUp(token, 600))) {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  // Everything this node has applied (which includes everything the client
  // has seen) happened before this write: those are its dependencies.
  const deps = Object.assign({}, applied);
  const n = applied[node.id] + 1;
  applied[node.id] = n;
  store.set(key, value);
  node.reply(msg, { type: 'write_ok', token: Object.assign({}, applied) });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', origin: node.id, n, key, value, deps });
});

node.on('replicate', (msg) => {
  const { origin, n, key, value, deps } = msg.body;
  pending.push({ origin, n, key, value, deps });
  drain();
});

node.on('read', async (msg) => {
  if (!(await catchUp(msg.body.token, 600))) {
    node.reply(msg, { type: 'error', code: 'unavailable' });
    return;
  }
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null, token: Object.assign({}, applied) });
});
