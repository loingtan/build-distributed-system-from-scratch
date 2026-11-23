// This is the asynchronous solution from the first task. It sends each write
// to each backup exactly once and keeps everything in memory.
//
// What is missing:
//  - a backup that missed messages (lost, partitioned, crashed) never gets
//    them again. With a per-key seq it cannot even tell that it missed some.
//    Keep an ordered log instead: the position in the log is the sequence
//    number, and "I have 17 entries" says exactly what to send next.
//  - nothing survives a crash. node.disk does:
//      node.disk.log ??= [];
//  - a restarted primary starts counting from 0 again, so backups ignore
//    its new writes as old ones.

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
