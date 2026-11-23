// MapReduce word count. This starter is correct, but it runs the whole job
// on n1, one task after another, while the workers sit idle. Make n1 a
// coordinator that only hands out tasks, and let the other nodes do the
// reading, mapping, reducing and writing.
const COORD = 'n1';
const LIMIT = 25; // fs accepts at most 25 lines per write or append
const POLL = 100; // an idle worker should ask for work about this often

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

// TODO: instead of running the tasks here, keep one record per task
//   { kind: 'map' | 'reduce', id, state: 'idle' | 'running' | 'done' }
// and hand them out to workers that send { type: 'get_task' }:
//   - map tasks first; reduce tasks only once every map task is done
//   - reply { type: 'get_task_ok', task: null } when there is nothing to give
//   - on { type: 'task_done', kind, id }, mark the task done
//   - when the last reduce task is done, reply job_ok to the saved job message
node.on('job', async (msg) => {
  const { inputs, reduces } = msg.body;
  for (let id = 0; id < inputs.length; id++) {
    await runMap({ kind: 'map', id, input: inputs[id], reduces });
  }
  const outputs = [];
  for (let id = 0; id < reduces; id++) {
    await runReduce({ kind: 'reduce', id, maps: inputs.length });
    outputs.push('out-' + id);
  }
  node.reply(msg, { type: 'job_ok', outputs });
});

// ----------------------------------------------------------------- worker

// TODO: on every node but n1, every POLL ms while idle: ask n1 for a task,
// run it with runMap or runReduce, tell n1 it is done, and ask again.
