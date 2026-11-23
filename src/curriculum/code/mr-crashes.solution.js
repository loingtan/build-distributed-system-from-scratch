// MapReduce that survives worker crashes. Busy workers send heartbeats; the
// coordinator puts the task of a worker that went silent back in the queue,
// and every file is committed by writing it under a temporary name and
// renaming it in one atomic step.
const COORD = 'n1';
const LIMIT = 25; // fs accepts at most 25 lines per write or append
const POLL = 100; // an idle worker asks for work this often
const HEARTBEAT = 100; // a busy worker says "still alive" this often
const SILENT = 500; // a worker not heard from for this long is presumed dead

// ------------------------------------------------------- the job (given)

// FNV-1a, 32 bits. A key belongs to reduce partition hash(key) % R.
function hash(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h;
}

// Word count: map emits [word, '1'] for every word; reduce adds them up.
function mapFn(line) {
  return line.split(' ').filter(Boolean).map((w) => [w, '1']);
}
function reduceFn(key, values) {
  let n = 0;
  for (const v of values) n += Number(v);
  return key + ' ' + n;
}

// ------------------------------------------------------ fs helpers (given)

async function fsCall(body) {
  const res = await node.rpc('fs', body, { timeout: 3000 });
  if (!res || res.type === 'error') throw new Error('fs ' + body.type + ' failed: ' + JSON.stringify(res));
  return res;
}

async function readFile(name) {
  return (await fsCall({ type: 'read', name })).data;
}

// A file longer than LIMIT lines takes one write and then appends.
async function writeFile(name, lines) {
  await fsCall({ type: 'write', name, data: lines.slice(0, LIMIT) });
  for (let i = LIMIT; i < lines.length; i += LIMIT) {
    await fsCall({ type: 'append', name, data: lines.slice(i, i + LIMIT) });
  }
}

// Write the whole file under a name no other attempt uses, then rename it
// onto `name`. Readers see either nothing or the complete file.
let tmpSeq = 0;
async function commitFile(name, lines) {
  const tmp = 'tmp-' + node.id + '-' + ++tmpSeq + '-' + Math.floor(node.random() * 1e9);
  await writeFile(tmp, lines);
  await fsCall({ type: 'rename', from: tmp, to: name });
}

// Map task `id`: read one input split, partition its pairs by key into R
// intermediate files mr-<id>-<r>, one line "key value" per pair.
async function runMap(task) {
  const lines = await readFile(task.input);
  const buckets = [];
  for (let r = 0; r < task.reduces; r++) buckets.push([]);
  for (const line of lines) {
    for (const [k, v] of mapFn(line)) buckets[hash(k) % task.reduces].push(k + ' ' + v);
  }
  await Promise.all(buckets.map((b, r) => commitFile('mr-' + task.id + '-' + r, b)));
}

// Reduce task `id`: read partition `id` of every map task's output, group by
// key, and write one line per key, sorted by key, to out-<id>.
async function runReduce(task) {
  const names = [];
  for (let m = 0; m < task.maps; m++) names.push('mr-' + m + '-' + task.id);
  const parts = await Promise.all(names.map(readFile));
  const groups = new Map();
  for (const lines of parts) {
    for (const line of lines) {
      const i = line.indexOf(' ');
      const k = line.slice(0, i);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(line.slice(i + 1));
    }
  }
  const keys = [...groups.keys()].sort();
  await commitFile('out-' + task.id, keys.map((k) => reduceFn(k, groups.get(k))));
}

// ------------------------------------------------------------ coordinator

let job = null; // { msg, phase: 'map' | 'reduce' | 'done', maps: [task], reduces: [task] }
const lastHeard = {}; // worker -> node.now() of its latest message

function heard(worker) {
  lastHeard[worker] = node.now();
}

node.on('job', (msg) => {
  const { inputs, reduces } = msg.body;
  job = {
    msg,
    phase: 'map',
    maps: inputs.map((input, id) => ({ kind: 'map', id, input, reduces, state: 'idle' })),
    reduces: [],
  };
  for (let id = 0; id < reduces; id++) job.reduces.push({ kind: 'reduce', id, maps: inputs.length, state: 'idle' });
});

const phaseTasks = () => (job.phase === 'map' ? job.maps : job.reduces);

// A worker asking for work holds no task (it may have crashed and restarted):
// whatever we thought it was running goes back to idle.
function release(worker) {
  if (!job) return;
  for (const t of [...job.maps, ...job.reduces]) {
    if (t.state === 'running' && t.worker === worker) t.state = 'idle';
  }
}

node.on('get_task', (msg) => {
  heard(msg.src);
  release(msg.src);
  // Reduce tasks are handed out only once every map task is done: a reduce
  // needs a piece of every map task's output.
  const task = job && job.phase !== 'done' ? phaseTasks().find((t) => t.state === 'idle') : null;
  if (!task) {
    node.reply(msg, { type: 'get_task_ok', task: null });
    return;
  }
  task.state = 'running';
  task.worker = msg.src;
  const { kind, id, input, reduces, maps } = task;
  node.reply(msg, { type: 'get_task_ok', task: { kind, id, input, reduces, maps } });
});

node.on('heartbeat', (msg) => heard(msg.src));

// Re-execution: the task of a worker that went silent is up for grabs again.
if (node.id === COORD) {
  node.every(100, () => {
    if (!job || job.phase === 'done') return;
    for (const t of phaseTasks()) {
      if (t.state === 'running' && node.now() - (lastHeard[t.worker] ?? 0) > SILENT) t.state = 'idle';
    }
  });
}

node.on('task_done', (msg) => {
  heard(msg.src);
  node.reply(msg, { type: 'task_done_ok' });
  if (!job || job.phase === 'done' || msg.body.kind !== (job.phase === 'map' ? 'map' : 'reduce')) return;
  const task = phaseTasks()[msg.body.id];
  if (!task || task.state === 'done') return;
  task.state = 'done';
  if (!phaseTasks().every((t) => t.state === 'done')) return;
  if (job.phase === 'map') {
    job.phase = 'reduce';
  } else {
    job.phase = 'done';
    node.reply(job.msg, { type: 'job_ok', outputs: job.reduces.map((t) => 'out-' + t.id) });
  }
});

// ----------------------------------------------------------------- worker

if (node.id !== COORD) {
  let busy = false;
  let working = false;
  node.every(HEARTBEAT, () => {
    if (working) node.send(COORD, { type: 'heartbeat' });
  });
  node.every(POLL, async () => {
    if (busy) return;
    busy = true;
    for (;;) {
      const res = await node.rpc(COORD, { type: 'get_task' }, { timeout: 500 });
      if (!res || !res.task) break;
      const task = res.task;
      working = true;
      if (task.kind === 'map') await runMap(task);
      else await runReduce(task);
      working = false;
      await node.rpc(COORD, { type: 'task_done', kind: task.kind, id: task.id }, { timeout: 500 });
    }
    busy = false;
  });
}
