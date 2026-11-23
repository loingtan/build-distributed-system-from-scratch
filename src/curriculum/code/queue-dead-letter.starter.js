// The deduplicating queue from the last task. A job that no consumer can
// finish is handed out again every VISIBILITY ms, for ever.
//
// TODO: count how often each job has been handed out. When the timeout of
// hand-out number MAX_DELIVERIES expires, move the job to a dead-letter
// list instead of back into the queue.
// TODO: node.on('dead_letters', ...) -> { type: 'dead_letters_ok', jobs: [{ id, payload }, ...] }
const VISIBILITY = 500;
const MAX_DELIVERIES = 3;

const jobs = new Map();  // id -> { job, state, timer }
const ready = [];        // ids waiting, oldest first (may hold stale ids)

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  if (!jobs.has(id)) {
    jobs.set(id, { job: { id, payload }, state: 'ready', timer: null });
    ready.push(id);
  }
  node.reply(msg, { type: 'enqueue_ok' });
});

node.on('dequeue', (msg) => {
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
  if (entry && entry.state !== 'done') {
    if (entry.timer) entry.timer.cancel();
    entry.state = 'done';
  }
  node.reply(msg, { type: 'ack_ok' });
});
