const store = new Map();

// The handler is synchronous, so each transaction runs start to finish before
// the next message is looked at. One node, one thread: serial by construction.
node.on('txn', (msg) => {
  const result = msg.body.txn.map(([op, key, value]) => {
    if (op === 'r') return ['r', key, store.get(key) ?? null];
    store.set(key, value);
    return ['w', key, value];
  });
  node.reply(msg, { type: 'txn_ok', txn: result });
});
