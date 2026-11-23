// MapReduce with backup tasks, from the previous task. It runs a single
// stage: it reads the first stage of the job and ignores the rest.
// TODO: run the stages one after another. The input splits of stage s + 1
// are the outputs of stage s (<output>-0, <output>-1, ...). Send each task
// its stage and fn, use the stage in intermediate file names, and reply
// job_ok with the last stage's outputs once its reduce phase is done.
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
// TODO: the second stage uses "bycount" (see the brief).
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
  await commitFile(task.output + '-' + task.id, keys.map((k) => reduceFn(k, groups.get(k))));
}

// ------------------------------------------------------------ coordinator

let job = null; // { msg, phase: 'map' | 'reduce' | 'done', maps: [task], reduces: [task] }
const lastHeard = {}; // worker -> node.now() of its latest message

function heard(worker) {
  lastHeard[worker] = node.now();
}

node.on('job', (msg) => {
  const { inputs } = msg.body;
  const { reduces } = msg.body.stages[0];
  job = {
    msg,
    phase: 'map',
    maps: inputs.map((input, id) => ({ kind: 'map', id, input, reduces, state: 'idle', workers: [] })),
    reduces: [],
  };
  for (let id = 0; id < reduces; id++) job.reduces.push({ kind: 'reduce', id, maps: inputs.length, output: msg.body.stages[0].output, state: 'idle', workers: [] });
});

const phaseTasks = () => (job.phase === 'map' ? job.maps : job.reduces);

// Take `worker` off every task it was running; a running task with nobody
// left on it goes back to idle.
function drop(task, worker) {
  task.workers = task.workers.filter((w) => w !== worker);
  if (task.state === 'running' && task.workers.length === 0) task.state = 'idle';
}

// A worker asking for work holds no task (it may have crashed and restarted).
function release(worker) {
  if (!job) return;
  for (const t of [...job.maps, ...job.reduces]) if (t.state === 'running') drop(t, worker);
}

// No idle task left in this phase: back up the oldest task that is running
// on exactly one other worker.
function pickBackup(worker) {
  let best = null;
  for (const t of phaseTasks()) {
    if (t.state !== 'running' || t.workers.length !== 1 || t.workers[0] === worker) continue;
    if (!best || t.started < best.started) best = t;
  }
  return best;
}

node.on('get_task', (msg) => {
  heard(msg.src);
  release(msg.src);
  // Reduce tasks are handed out only once every map task is done: a reduce
  // needs a piece of every map task's output.
  let task = null;
  if (job && job.phase !== 'done') task = phaseTasks().find((t) => t.state === 'idle') || pickBackup(msg.src);
  if (!task) {
    node.reply(msg, { type: 'get_task_ok', task: null });
    return;
  }
  if (task.state === 'idle') {
    task.state = 'running';
    task.started = node.now();
  }
  task.workers.push(msg.src);
  const { kind, id, input, reduces, maps, output } = task;
  node.reply(msg, { type: 'get_task_ok', task: { kind, id, input, reduces, maps, output } });
});

node.on('heartbeat', (msg) => heard(msg.src));

// Re-execution: the task of a worker that went silent is up for grabs again.
if (node.id === COORD) {
  node.every(100, () => {
    if (!job || job.phase === 'done') return;
    for (const t of phaseTasks()) {
      if (t.state !== 'running') continue;
      for (const w of t.workers) if (node.now() - (lastHeard[w] ?? 0) > SILENT) drop(t, w);
    }
  });
}

node.on('task_done', (msg) => {
  heard(msg.src);
  node.reply(msg, { type: 'task_done_ok' });
  if (!job || job.phase === 'done' || msg.body.kind !== (job.phase === 'map' ? 'map' : 'reduce')) return;
  const task = phaseTasks()[msg.body.id];
  // The first copy to finish completes the task; a duplicate or late
  // completion (a backup, or a worker presumed dead) changes nothing.
  if (!task || task.state === 'done') return;
  task.state = 'done';
  task.workers = [];
  if (!phaseTasks().every((t) => t.state === 'done')) return;
  if (job.phase === 'map') {
    job.phase = 'reduce';
  } else {
    job.phase = 'done';
    node.reply(job.msg, { type: 'job_ok', outputs: job.reduces.map((t) => t.output + '-' + t.id) });
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
