// Chain replication. The chain is node.nodeIds in order:
// the first node is the head, the last is the tail.
const chain = node.nodeIds;
const head = chain[0];
const tail = chain[chain.length - 1];
const next = chain[chain.indexOf(node.id) + 1] ?? null; // null at the tail

const log = [];          // writes in the head's order
const store = new Map(); // key -> value

function append(entry) {
  log.push(entry);
  store.set(entry.key, entry.value);
  // TODO: if this node is the tail, the write is now on every node:
  // answer the client that sent it. node.reply needs the original message,
  // which only the head has, so do by hand what node.reply does:
  //   node.send(entry.client, { type: 'write_ok', in_reply_to: entry.msg_id });
  // Otherwise: make sure next gets this entry, after all earlier ones.
}

node.on('write', (msg) => {
  if (node.id !== head) {
    node.reply(msg, { type: 'error', code: 'not_head', head });
    return;
  }
  const { key, value } = msg.body;
  append({ key, value, client: msg.src, msg_id: msg.body.msg_id });
  // TODO: the head must not answer. Remove this once the tail does.
  node.reply(msg, { type: 'write_ok' });
});

// TODO: a handler for entries arriving from the previous node. They may be
// lost, repeated or out of order; the log must not be.

node.on('read', (msg) => {
  if (node.id !== tail) {
    node.reply(msg, { type: 'error', code: 'not_tail', tail });
    return;
  }
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null });
});
