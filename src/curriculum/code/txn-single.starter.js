const store = new Map();

node.on('txn', (msg) => {
  // msg.body.txn is a list like [['r', 1, null], ['w', 1, 6], ['r', 1, null]]
  // TODO: run each operation in order against `store`.
  //   ['r', key, null]  -> ['r', key, <current value, or null>]
  //   ['w', key, value] -> store it, return it unchanged
  const result = [];
  node.reply(msg, { type: 'txn_ok', txn: result });
});
