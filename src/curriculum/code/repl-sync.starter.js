// This is the asynchronous solution from the previous task. It answers
// write_ok before any backup has the write, so a client that writes and then
// reads from another node can miss its own write.
//
// What is missing:
//  - the primary must hear back from every backup before it says write_ok
//    (node.rpc instead of node.send, and a reply from the backup)
//  - a lost message must be sent again
//  - if a backup stays silent, give up after about half a second and answer
//    { type: 'error', code: 'unavailable' }

const primary = node.nodeIds[0];
const isPrimary = node.id === primary;

const store = new Map(); // key -> { value, seq }
let seq = 0;

function apply(key, value, s) {
  const current = store.get(key);
  if (!current || s > current.seq) store.set(key, { value, seq: s });
}

node.on('write', async (msg) => {
  const { key, value } = msg.body;

  if (!isPrimary) {
    const res = await node.rpc(primary, { type: 'write', key, value }, { timeout: 500 });
    node.reply(msg, res ? { type: res.type } : { type: 'error', code: 'unavailable' });
    return;
  }

  seq += 1;
  apply(key, value, seq);
  node.reply(msg, { type: 'write_ok' });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', key, value, seq });
});

node.on('replicate', (msg) => {
  apply(msg.body.key, msg.body.value, msg.body.seq);
});

node.on('read', (msg) => {
  const entry = store.get(msg.body.key);
  node.reply(msg, { type: 'read_ok', value: entry ? entry.value : null });
});
