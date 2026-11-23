// key -> array of messages. A message's offset is its index.
const logs = new Map();

node.on('send', (msg) => {
  const { key, msg: value } = msg.body;
  // TODO: append value to the log for key
  // reply { type: 'send_ok', offset }
});

node.on('poll', (msg) => {
  // msg.body.offsets is { k1: 3, k2: 0 }
  // TODO: reply { type: 'poll_ok', msgs: { k1: [[3, msg], [4, msg]], k2: [...] } }
});

node.on('commit_offsets', (msg) => {
  // msg.body.offsets is { k1: 5 }
  // TODO: remember them, reply { type: 'commit_offsets_ok' }
});

node.on('list_committed_offsets', (msg) => {
  // msg.body.keys is ['k1', 'k2']
  // TODO: reply { type: 'list_committed_offsets_ok', offsets: { k1: 5 } }
});
