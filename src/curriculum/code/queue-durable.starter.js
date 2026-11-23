// The queue with dead letters from the last task. All of its state lives in
// variables, so a broker crash wipes every job it had accepted.
//
// TODO: keep the state on node.disk (plain JSON only: objects and arrays,
// no Map, no timer handles). After a restart this file runs again from the
// top: jobs that were with a consumer at the crash have no timer any more,
// so give them one, or put them back in the queue.
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
