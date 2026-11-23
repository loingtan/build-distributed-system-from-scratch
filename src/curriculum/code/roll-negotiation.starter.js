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
  runVersion1();
  // TODO: use fwd2 with every peer that runs version 2, and fwd_put / fwd_get
  // with the others. Version 1 has no handler for fwd2: a fwd2 sent to a
  // version-1 node is simply lost. How does a node find out which of its
  // peers run version 2? Remember that a restart wipes everything but
  // node.disk, and that peers are upgraded after you as well as before.
  // Calling node.on again for a type replaces the earlier handler.
}

if (version === 1) runVersion1();
else runVersion2();
