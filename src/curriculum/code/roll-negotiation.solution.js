// A user directory spread over the cluster. Each key lives on one node, its
// home; any node accepts a request and forwards it there.
//
// Every node runs this same file. The harness upgrades a node by stopping it,
// setting node.disk.version = 2 and starting it again, one node at a time,
// while clients keep sending requests.
const version = node.disk.version ?? 1;

node.disk.users ??= {};
const users = node.disk.users; // key -> { name }

function home(key) {
  let sum = 0;
  for (let i = 0; i < key.length; i++) sum += key.charCodeAt(i);
  return node.nodeIds[sum % node.nodeIds.length];
}

// ---------------------------------------------------------------- version 1
// Already running in production. Do not change it: a node that has not been
// upgraded yet runs exactly this code, and you cannot edit it there.
function runVersion1() {
  node.on('put', async (msg) => {
    const { key, name } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      const rec = (users[key] ??= {});
      rec.name = name;
      node.reply(msg, { type: 'put_ok' });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_put', key, name }, { timeout: 300 });
    node.reply(msg, res ? { type: 'put_ok' } : { type: 'error', code: 'unavailable' });
  });

  node.on('get', async (msg) => {
    const { key } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      node.reply(msg, { type: 'get_ok', name: users[key] ? users[key].name : null });
      return;
    }
    const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
    node.reply(msg, res ? { type: 'get_ok', name: res.name } : { type: 'error', code: 'unavailable' });
  });

  // Version 1 reads only the fields it knows (key, name) and ignores the rest.
  node.on('fwd_put', (msg) => {
    const rec = (users[msg.body.key] ??= {});
    rec.name = msg.body.name;
    node.reply(msg, { type: 'fwd_put_ok' });
  });

  node.on('fwd_get', (msg) => {
    const rec = users[msg.body.key];
    node.reply(msg, { type: 'fwd_get_ok', name: rec ? rec.name : null });
  });
}

// ---------------------------------------------------------------- version 2
// Version 2 forwards in a new format, fwd2, and the home answers the client
// itself, which saves a hop:
//
//   { type: 'fwd2', op: 'put', key, name, client, reply_to }
//   { type: 'fwd2', op: 'get', key, client, reply_to }
//
// client and reply_to are the client's msg.src and msg.body.msg_id. The home
// replies with node.send(client, { type: 'get_ok', name, in_reply_to: reply_to })
// (or put_ok); the forwarding node does not reply at all.
function runVersion2() {
  runVersion1(); // fwd_put and fwd_get stay: version-1 nodes still send them

  // Peers known to run version 2. Memory only: after a restart we know
  // nothing, and ask again.
  const modern = new Set();

  // Handshake. A version-2 peer answers; a version-1 peer has no handler for
  // hello, so the rpc resolves to null and the peer stays "old".
  async function greet(peer) {
    const res = await node.rpc(peer, { type: 'hello', version: 2 }, { timeout: 300 });
    if (res && res.version >= 2) modern.add(peer);
  }
  // A hello we receive is evidence as well: this is how the nodes that
  // upgraded earlier learn about us.
  node.on('hello', (msg) => {
    if (msg.body.version >= 2) modern.add(msg.src);
    node.reply(msg, { type: 'hello_ok', version: 2 });
  });
  for (const peer of node.peers) greet(peer);

  function write(key, name) {
    const rec = (users[key] ??= {});
    rec.name = name;
  }
  function read(key) {
    return users[key] ? users[key].name : null;
  }

  node.on('put', async (msg) => {
    const { key, name } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      write(key, name);
      node.reply(msg, { type: 'put_ok' });
    } else if (modern.has(owner)) {
      node.send(owner, { type: 'fwd2', op: 'put', key, name, client: msg.src, reply_to: msg.body.msg_id });
    } else {
      const res = await node.rpc(owner, { type: 'fwd_put', key, name }, { timeout: 300 });
      node.reply(msg, res ? { type: 'put_ok' } : { type: 'error', code: 'unavailable' });
    }
  });

  node.on('get', async (msg) => {
    const { key } = msg.body;
    const owner = home(key);
    if (owner === node.id) {
      node.reply(msg, { type: 'get_ok', name: read(key) });
    } else if (modern.has(owner)) {
      node.send(owner, { type: 'fwd2', op: 'get', key, client: msg.src, reply_to: msg.body.msg_id });
    } else {
      const res = await node.rpc(owner, { type: 'fwd_get', key }, { timeout: 300 });
      node.reply(msg, res ? { type: 'get_ok', name: res.name } : { type: 'error', code: 'unavailable' });
    }
  });

  node.on('fwd2', (msg) => {
    const { op, key, name, client, reply_to } = msg.body;
    modern.add(msg.src);
    if (op === 'put') {
      write(key, name);
      node.send(client, { type: 'put_ok', in_reply_to: reply_to });
    } else {
      node.send(client, { type: 'get_ok', name: read(key), in_reply_to: reply_to });
    }
  });
}

if (version === 1) runVersion1();
else runVersion2();
