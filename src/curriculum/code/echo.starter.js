// This file runs once on every node in the cluster.
// `node` is your handle on the node: its id, its peers, the network.

node.on('echo', (msg) => {
  // msg.body looks like { type: 'echo', echo: 'hello 1', msg_id: 1 }
  // TODO: reply with { type: 'echo_ok', echo: <the same payload> }
});
