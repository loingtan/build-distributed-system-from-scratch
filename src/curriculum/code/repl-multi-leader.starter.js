// Every node is a leader now: it accepts writes to any key, answers at once
// and then sends the write to the others. Several nodes may write the same
// key at the same moment; all nodes must end up with the same value.
const store = new Map(); // key -> value

// TODO: keep { value, ts } per key, with ts = [counter, node.id]. The higher
// counter wins; on a tie, the higher node id (compared as strings). Make the
// counter a Lamport clock: +1 for each write accepted here, and never below
// any counter this node has received.

node.on('write', (msg) => {
  const { key, value } = msg.body;
  store.set(key, value);
  // TODO: write_ok must carry the write's timestamp: { type: "write_ok", ts }.
  node.reply(msg, { type: 'write_ok' });
  for (const peer of node.peers) node.send(peer, { type: 'replicate', key, value });
});

node.on('replicate', (msg) => {
  // TODO: this keeps whichever write arrives last, and two nodes can see two
  // writes arrive in opposite orders. Keep the one with the higher timestamp.
  store.set(msg.body.key, msg.body.value);
});

// TODO: messages sent during a partition are lost for good. Have every node
// send its whole store to its peers now and then (node.every), and merge what
// it receives with the same rule.

node.on('read', (msg) => {
  const key = msg.body.key;
  node.reply(msg, { type: 'read_ok', value: store.has(key) ? store.get(key) : null });
});
