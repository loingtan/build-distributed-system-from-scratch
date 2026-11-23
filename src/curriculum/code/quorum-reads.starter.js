// Leaderless replication, step 2: versions and quorum reads.
// This is the quorum write from the previous task, plus a get that only
// looks at this node's own copy. A node that missed a write answers with
// an old value, and a late 'store' can overwrite a newer one.
const N = node.nodeIds.length;
const W = Math.floor(N / 2) + 1;
const R = N - W + 1; // the smallest R with W + R > N

// TODO: keep a version next to each value: key -> { value, counter, origin }.
// (counter, origin) is the version: the higher counter is newer, and
// origin (the coordinator's node id) breaks ties.
const store = new Map(); // key -> value

// What this node does as a replica.
function local(body) {
  // TODO: only let a 'store' replace the copy if its version is newer.
  // TODO: answer a second kind of request, 'read', with this replica's
  // copy and its version.
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
  // TODO: give the write a version higher than every earlier write to this
  // key. This node may have missed some, so ask R replicas for their
  // current version first and use the highest counter plus one.
  const acks = await ask({ type: 'store', key, value }, W, 300);
  if (acks) node.reply(msg, { type: 'put_ok' });
  else node.reply(msg, { type: 'error', text: 'fewer than ' + W + ' replicas acknowledged the write' });
});

node.on('get', (msg) => {
  // TODO: ask every replica, wait for R answers, and return the value with
  // the highest version. Reply { type: 'error', text } if R do not answer.
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'get_ok', value });
});

node.on('store', (msg) => node.reply(msg, local(msg.body)));

node.on('local_get', (msg) => {
  const value = store.has(msg.body.key) ? store.get(msg.body.key) : null;
  node.reply(msg, { type: 'local_get_ok', value });
});
