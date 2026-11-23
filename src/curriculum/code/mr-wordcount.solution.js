// MapReduce word count. n1 is the coordinator: it splits the job into map
// and reduce tasks and hands them to workers that ask. Every other node is a
// worker: it asks for a task, runs it against the fs service, reports it done
// and asks again.
const COORD = 'n1';
const LIMIT = 25; // fs accepts at most 25 lines per write or append
const POLL = 100; // an idle worker asks for work this often

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

// Map task `id`: read one input split, partition its pairs by key into R
// intermediate files mr-<id>-<r>, one line "key value" per pair.
async function runMap(task) {
  const lines = await readFile(task.input);
  const buckets = [];
  for (let r = 0; r < task.reduces; r++) buckets.push([]);
  for (const line of lines) {
    for (const [k, v] of mapFn(line)) buckets[hash(k) % task.reduces].push(k + ' ' + v);
  }
  await Promise.all(buckets.map((b, r) => writeFile('mr-' + task.id + '-' + r, b)));
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
  await writeFile('out-' + task.id, keys.map((k) => reduceFn(k, groups.get(k))));
}

// ------------------------------------------------------------ coordinator

let job = null; // { msg, phase: 'map' | 'reduce' | 'done', maps: [task], reduces: [task] }

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

node.on('get_task', (msg) => {
  // Reduce tasks are handed out only once every map task is done: a reduce
  // needs a piece of every map task's output.
  const task = job && job.phase !== 'done' ? phaseTasks().find((t) => t.state === 'idle') : null;
  if (!task) {
    node.reply(msg, { type: 'get_task_ok', task: null });
    return;
  }
  task.state = 'running';
  const { kind, id, input, reduces, maps } = task;
  node.reply(msg, { type: 'get_task_ok', task: { kind, id, input, reduces, maps } });
});

node.on('task_done', (msg) => {
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
  node.every(POLL, async () => {
    if (busy) return;
    busy = true;
    for (;;) {
      const res = await node.rpc(COORD, { type: 'get_task' }, { timeout: 500 });
      if (!res || !res.task) break;
      const task = res.task;
      if (task.kind === 'map') await runMap(task);
      else await runReduce(task);
      await node.rpc(COORD, { type: 'task_done', kind: task.kind, id: task.id }, { timeout: 500 });
    }
    busy = false;
  });
}
