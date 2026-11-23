// State declared here lives for as long as the node does.

node.on('broadcast', (msg) => {
  // msg.body.message is an integer.
  // TODO: remember it, then reply { type: 'broadcast_ok' }
});

node.on('read', (msg) => {
  // TODO: reply { type: 'read_ok', messages: [every value seen so far] }
});
