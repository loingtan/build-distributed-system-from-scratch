// A work queue on one node: this node is the broker.
// Producers send enqueue, consumers send dequeue.

node.on('enqueue', (msg) => {
  // msg.body is { type: 'enqueue', id: 'job-7', payload: { ... } }
  // TODO: remember the job as { id, payload }, behind the jobs already waiting.
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
  // TODO: hand out the job that has waited longest and forget it.
  // If nothing is waiting, the answer is job: null.
  node.reply(msg, { type: 'dequeue_ok', job: null });
});
