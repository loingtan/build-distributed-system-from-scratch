const clock = {};
for (const id of node.nodeIds) clock[id] = 0;

node.on('event', (msg) => {
  clock[node.id] += 1;
  node.reply(msg, { type: 'event_ok', clock });
});

node.on('notify', (msg) => {
  clock[node.id] += 1;
  node.send(msg.body.to, { type: 'note', clock });
  node.reply(msg, { type: 'notify_ok', clock });
});

// Merge: for every node, keep the larger count. Afterwards our clock covers
// everything the sender knew about. Then count the receive itself.
node.on('note', (msg) => {
  for (const [id, n] of Object.entries(msg.body.clock)) {
    clock[id] = Math.max(clock[id], n);
  }
  clock[node.id] += 1;
});
