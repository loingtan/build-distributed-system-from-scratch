// Job states: 'ready' -> 'inflight' -> 'done', or back to 'ready' after a
// timeout. New here: a job whose timeout expires for the MAX_DELIVERIES-th
// time becomes 'dead' and is parked where a human can look at it.
const VISIBILITY = 500;
const MAX_DELIVERIES = 3;

const jobs = new Map();  // id -> { job, state, timer, deliveries }
const ready = [];        // ids waiting, oldest first (may hold stale ids)
const dead = [];         // jobs that used up their attempts

node.on('enqueue', (msg) => {
  const { id, payload } = msg.body;
  if (!jobs.has(id)) {
    jobs.set(id, { job: { id, payload }, state: 'ready', timer: null, deliveries: 0 });
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
  entry.deliveries += 1;
  entry.timer = node.after(VISIBILITY, () => {
    if (entry.state !== 'inflight') return;
    if (entry.deliveries >= MAX_DELIVERIES) {
      // Out of attempts. Stop retrying, but do not throw the job away.
      entry.state = 'dead';
      dead.push(entry.job);
    } else {
      entry.state = 'ready';
      ready.unshift(entry.job.id);
    }
  });
  node.reply(msg, { type: 'dequeue_ok', job: entry.job });
});

node.on('ack', (msg) => {
  const entry = jobs.get(msg.body.id);
  if (entry && (entry.state === 'ready' || entry.state === 'inflight')) {
    if (entry.timer) entry.timer.cancel();
    entry.state = 'done';
  }
  node.reply(msg, { type: 'ack_ok' });
});

node.on('dead_letters', (msg) => {
  node.reply(msg, { type: 'dead_letters_ok', jobs: dead });
});
