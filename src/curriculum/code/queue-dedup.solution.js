// One record per job id, kept for ever, with the job's current state:
//   'ready'     waiting in the queue
//   'inflight'  out with a consumer, timer running
//   'done'      acknowledged
// Because the record outlives the job, a repeated enqueue or ack finds it
// and changes nothing.
const VISIBILITY = 500;

const jobs = new Map();  // id -> { job, state, timer }
const ready = [];        // ids waiting, oldest first (may hold stale ids)

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  if (!jobs.has(id)) {
    jobs.set(id, { job: { id, payload }, state: 'ready', timer: null });
    ready.push(id);
  }
  // A retry gets the same answer: the producer is still waiting for one.
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
  // Skip ids that were acknowledged while they sat in the queue.
  let entry = null;
  while (ready.length > 0 && !entry) {
    const candidate = jobs.get(ready.shift());
    if (candidate.state === 'ready') entry = candidate;
  }
  if (!entry) {
    node.reply(msg, { type: 'dequeue_ok', job: null });
    return;
  }
  entry.state = 'inflight';
  entry.timer = node.after(VISIBILITY, () => {
    if (entry.state !== 'inflight') return;
    entry.state = 'ready';
    ready.unshift(entry.job.id);
  });
  node.reply(msg, { type: 'dequeue_ok', job: entry.job });
});

node.on('ack', (msg) => {
  const entry = jobs.get(msg.body.id);
  // Wherever the job is now (with this consumer, with another one, or back
  // in the queue), it is finished. A second ack finds it 'done' already.
  if (entry && entry.state !== 'done') {
    if (entry.timer) entry.timer.cancel();
    entry.state = 'done';
  }
  node.reply(msg, { type: 'ack_ok' });
});
