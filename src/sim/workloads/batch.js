// Workloads for the batch-processing (MapReduce) track.
//
// n1 is the coordinator; every other node is a worker. The input splits and
// every file the job writes live in a harness service called `fs`, so the
// checkers can see exactly who wrote what, and when. A client sends one `job`
// request to n1 and waits for `job_ok`.
//
// fs understands:
//   { type: 'read', name }               -> { type: 'read_ok', data: [lines] }
//   { type: 'write', name, data }        -> { type: 'write_ok' }   create or replace
//   { type: 'append', name, data }       -> { type: 'append_ok' }  create if missing
//   { type: 'rename', from, to }         -> { type: 'rename_ok' }  atomic, replaces `to`
// A write or append carries at most LIMIT lines. Errors come back as
// { type: 'error', code, text }.

import { check, j, plural } from '../workloads.js';

const COORD = 'n1';
const LIMIT = 25; // lines per write or append
const START = 100; // when the client sends the job
const MAX_ATTEMPTS = 3;
const ms = (t) => Math.round(t) + 'ms';

// FNV-1a, 32 bits. The partition of a key is fnv(key) % R.
export function fnv(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h;
}

// The two job functions. map(line) -> [[key, value]], reduce(key, values) -> output line.
const FUNCTIONS = {
  wordcount: {
    map: (line) => line.split(' ').filter(Boolean).map((w) => [w, '1']),
    reduce: (k, vs) => k + ' ' + vs.reduce((a, v) => a + Number(v), 0),
  },
  bycount: {
    map: (line) => {
      const [w, n] = line.split(' ');
      return [[n, w]];
    },
    reduce: (k, vs) => k + ' ' + [...vs].sort().join(' '),
  },
};

function reference(fn, files, R) {
  const parts = Array.from({ length: R }, () => new Map());
  for (const lines of files) {
    for (const line of lines) {
      for (const [k, v] of FUNCTIONS[fn].map(line)) {
        const p = parts[fnv(k) % R];
        if (!p.has(k)) p.set(k, []);
        p.get(k).push(v);
      }
    }
  }
  return parts.map((p) => [...p.keys()].sort().map((k) => FUNCTIONS[fn].reduce(k, p.get(k))));
}

const SYL = ['ka', 'lo', 'mi', 'ne', 'ru', 'ta', 'zi', 'po', 'be', 'du', 'fa', 'gi', 'ho', 'ju', 've', 'so', 'wa', 'ye'];

// `splits` input files in-0, in-1, ... of 10 lines of 10 words each. Word
// frequencies are skewed, so a few words are common and many are rare.
function makeInputs(sim, splits) {
  const vocab = [];
  const seen = new Set();
  while (vocab.length < 170) {
    let w = '';
    const k = 2 + Math.floor(sim.rand() * 2);
    for (let i = 0; i < k; i++) w += SYL[Math.floor(sim.rand() * SYL.length)];
    if (!seen.has(w)) {
      seen.add(w);
      vocab.push(w);
    }
  }
  const inputs = {};
  for (let m = 0; m < splits; m++) {
    const lines = [];
    for (let l = 0; l < 10; l++) {
      const words = [];
      for (let k = 0; k < 10; k++) {
        const r = sim.rand();
        words.push(vocab[Math.floor(r * r * vocab.length)]);
      }
      lines.push(words.join(' '));
    }
    inputs['in-' + m] = lines;
  }
  return inputs;
}

const same = (a, b) => !!a && !!b && a.length === b.length && a.every((x, i) => x === b[i]);

function describeDiff(name, got, want) {
  if (!got) return name + ' does not exist';
  if (got.length !== want.length) {
    const missing = want.find((l) => !got.includes(l));
    const extra = got.find((l) => !want.includes(l));
    return (
      name + ' has ' + plural(got.length, 'line') + ', expected ' + want.length +
      (missing ? ' (missing ' + j(missing) + ')' : '') + (extra ? ' (unexpected ' + j(extra) + ')' : '')
    );
  }
  const i = got.findIndex((l, k) => l !== want[k]);
  return name + ' line ' + (i + 1) + ' is ' + j(got[i]) + ', expected ' + j(want[i]);
}

// ------------------------------------------------------------------ the job

// params:
//   splits, reduces           size of the job (one stage)
//   stages                    [{ fn, reduces, output }] for a multi-stage job instead
//   crashes                   [{ phase: 'map' | 'reduce' | 'stage2', restartAfter }]
//   slow, slowDelay           a worker whose fs requests are delayed by slowDelay ms
//   deadline                  ms after the job is sent by which job_ok must arrive
//   atomic                    check that outputs never appear half-written
//   rate                      node-to-node messages allowed per second, on average
function runJob(sim, params) {
  const { splits = 6, crashes = [], slow = null, slowDelay = 0, deadline = 2500, atomic = false, rate = 150 } = params;
  const multi = Array.isArray(params.stages);
  const stages = multi ? params.stages : [{ fn: 'wordcount', reduces: params.reduces ?? 3, output: 'out' }];

  const inputs = makeInputs(sim, splits);
  const inputNames = Object.keys(inputs);
  const expected = new Map(); // output name -> lines
  let files = inputNames.map((n) => inputs[n]);
  const stageInputs = new Set(); // outputs of a stage that feed the next one
  stages.forEach((st, s) => {
    const parts = reference(st.fn, files, st.reduces);
    parts.forEach((lines, r) => {
      expected.set(st.output + '-' + r, lines);
      if (s < stages.length - 1) stageInputs.add(st.output + '-' + r);
    });
    files = parts;
  });
  const last = stages[stages.length - 1];
  const finalNames = Array.from({ length: last.reduces }, (_, r) => last.output + '-' + r);
  const outPrefixes = stages.map((st) => st.output + '-');

  // ------------------------------------------------------------ fs service
  const fs = new Map(Object.entries(inputs).map(([k, v]) => [k, [...v]]));
  const errors = [];
  const fromCoord = [];
  const inputReaders = new Set();
  const attempts = new Map(); // input-like name -> reads, output name -> commits
  const mode = new Map(); // worker -> 'map' | 'reduce' | 'stage2', from its latest read
  const crashed = [];
  const plan = crashes.map((c) => ({ ...c, done: false }));
  let halfSeen = null;
  const bump = (k) => attempts.set(k, (attempts.get(k) ?? 0) + 1);

  const setFile = (name, lines, how, src, t) => {
    fs.set(name, lines);
    if (!expected.has(name)) return;
    if (how !== 'append') bump('commit:' + name);
    const want = expected.get(name);
    if (!halfSeen && !same(lines, want) && lines.length < want.length) {
      halfSeen = { name, t, src, how, have: lines.length, want: want.length };
    }
  };

  const okLines = (d) => Array.isArray(d) && d.every((x) => typeof x === 'string');

  if (slow) {
    sim.intercept = ({ src, dest }) => (src === slow && dest === 'fs' ? slowDelay : undefined);
  }

  sim.service('fs', (msg, reply) => {
    const t = sim.now();
    const b = msg.body;
    const src = msg.src;
    const answer = (body) => sim.at(t + 4 + sim.rand() * 10, () => reply(body));
    const fail = (code, text) => {
      errors.push({ t, src, req: { type: b.type, name: b.name, from: b.from, to: b.to }, code, text });
      reply({ type: 'error', code, text });
    };
    if (src === COORD) fromCoord.push({ t, type: b.type, name: b.name ?? b.from });

    if (b.type === 'read') {
      if (typeof b.name !== 'string' || !fs.has(b.name)) return fail('not_found', 'no file named ' + j(b.name));
      if (inputs[b.name]) {
        mode.set(src, 'map');
        inputReaders.add(src);
        bump('read:' + b.name);
      } else if (stageInputs.has(b.name)) {
        mode.set(src, 'stage2');
        bump('read:' + b.name);
      } else if (!expected.has(b.name)) {
        mode.set(src, 'reduce');
      }
      return answer({ type: 'read_ok', data: [...fs.get(b.name)] });
    }
    if (b.type === 'write' || b.type === 'append') {
      if (typeof b.name !== 'string' || !b.name) return fail('bad_request', b.type + ' needs a name');
      if (!okLines(b.data)) return fail('bad_request', b.type + ' needs data: an array of strings (lines)');
      if (b.data.length > LIMIT) return fail('too_large', b.type + ' carries ' + b.data.length + ' lines; the limit is ' + LIMIT + ' per request');
      if (inputs[b.name]) return fail('read_only', b.name + ' is an input file');
      const lines = b.type === 'write' ? [...b.data] : [...(fs.get(b.name) ?? []), ...b.data];
      setFile(b.name, lines, b.type, src, t);
      // A worker that crashes in the middle of writing a file.
      const c = plan.find(
        (p) =>
          !p.done && src !== COORD && src !== slow && mode.get(src) === p.phase &&
          b.type === (p.on ?? (p.phase === 'map' ? 'append' : 'write')) &&
          !crashed.some((x) => x.id === src) && sim.isUp(src),
      );
      if (c) {
        c.done = true;
        crashed.push({ id: src, t, phase: c.phase, file: b.name, restarted: c.restartAfter != null });
        sim.crash(src);
        if (c.restartAfter != null) sim.at(t + c.restartAfter, () => sim.restart(src));
        return;
      }
      return answer({ type: b.type + '_ok' });
    }
    if (b.type === 'rename') {
      if (typeof b.from !== 'string' || !fs.has(b.from)) return fail('not_found', 'no file named ' + j(b.from));
      if (typeof b.to !== 'string' || !b.to) return fail('bad_request', 'rename needs from and to');
      if (inputs[b.from] || inputs[b.to]) return fail('read_only', 'input files cannot be renamed or replaced');
      const lines = fs.get(b.from);
      fs.delete(b.from);
      setFile(b.to, lines, 'rename', src, t);
      return answer({ type: 'rename_ok' });
    }
    fail('bad_request', 'fs understands read, write, append and rename, not ' + j(b.type));
  });

  // ------------------------------------------------------------- the client
  const job = { res: undefined, done: null, okAt: null, snapshot: null };
  const req = multi ? { type: 'job', inputs: inputNames, stages } : { type: 'job', inputs: inputNames, reduces: stages[0].reduces };
  const snapshot = () => new Map([...expected.keys()].map((n) => [n, fs.has(n) ? [...fs.get(n)] : null]));
  sim.onSend = (msg) => {
    if (msg.src === COORD && msg.kind === 'client' && msg.body.type === 'job_ok' && !job.snapshot) {
      job.snapshot = snapshot();
      job.okAt = sim.now();
    }
  };
  sim.at(START, async () => {
    const res = await sim.clientRpc(COORD, req, { timeout: sim.duration - START - 100 });
    job.res = res;
    job.done = sim.now();
  });

  // --------------------------------------------------------------- checks
  return () => {
    const checks = [];
    const progress = () => {
      const have = [...expected.keys()].filter((n) => same(fs.get(n), expected.get(n)));
      const read = inputNames.filter((n) => attempts.get('read:' + n));
      return (
        ' By the end, ' + read.length + ' of ' + inputNames.length + ' input splits had been read and ' +
        (have.length ? 'these outputs were complete: ' + have.join(', ') : 'no output was complete') + '.' +
        (crashed.length ? ' Crashed: ' + crashed.map((c) => c.id + ' at ' + ms(c.t) + ' while writing ' + c.file + (c.restarted ? ' (restarted later)' : ' (stayed down)')).join('; ') + '.' : '')
      );
    };

    const res = job.res;
    const finishedIn = job.done !== null && res ? job.done - START : null;
    let finishDetail = '';
    if (!res) {
      finishDetail =
        'The client sent ' + j({ type: 'job', inputs: '[' + inputNames.length + ' splits]' }) + ' to n1 at ' + ms(START) + ' and got no job_ok.' + progress() +
        (slow
          ? ' ' + slow + ' is slow (each of its fs requests takes ' + slowDelay + 'ms longer) and the job waits for its tasks. Near the end of a phase, give idle workers backup copies of tasks still in progress.'
          : crashed.length
            ? ' A worker that crashed will never report its task done: when a worker goes silent, give its task to another worker.'
            : ' Hand out map tasks, then reduce tasks once every map task is done, and reply when every reduce task is done.');
    } else if (res.type !== 'job_ok') {
      finishDetail = 'n1 answered the job with ' + j(res) + '; expected { type: "job_ok", outputs: ' + j(finalNames) + ' }.';
    } else if (!Array.isArray(res.outputs) || res.outputs.join(',') !== finalNames.join(',')) {
      finishDetail = 'job_ok lists outputs ' + j(res.outputs) + '; expected ' + j(finalNames) + '.';
    } else if (finishedIn > deadline) {
      finishDetail =
        'job_ok arrived ' + ms(finishedIn) + ' after the job was sent; the limit is ' + ms(deadline) + '.' +
        (slow ? ' ' + slow + ' is slow (each of its fs requests takes ' + slowDelay + 'ms longer) and the job waits for its tasks. Near the end of a phase, give idle workers backup copies of tasks still in progress, and take whichever copy finishes first.' : progress());
    }
    checks.push(check('The job finishes within ' + ms(deadline), !finishDetail, finishDetail));

    // Exact output: when job_ok was sent, and again at the end.
    let outDetail = '';
    if (job.snapshot) {
      for (const [name, want] of expected) {
        const got = job.snapshot.get(name);
        if (!same(got, want)) {
          outDetail = 'When n1 sent job_ok at ' + ms(job.okAt) + ', ' + describeDiff(name, got, want) + '.' +
            (got && got.length > want.length ? ' Lines counted twice usually mean two attempts of a task wrote into the same file, or a reduce read an intermediate file that two attempts appended to.' : '') +
            (!got || got.length < want.length ? ' Reply job_ok only once every reduce task has committed its output; a late or duplicate completion must not count as another task done.' : '');
          break;
        }
      }
    }
    if (!outDetail) {
      for (const [name, want] of expected) {
        const got = fs.get(name);
        if (job.snapshot && !same(got, want)) {
          outDetail = 'Correct when job_ok was sent, but at the end of the run ' + describeDiff(name, got, want) + '. A late attempt of a task (a straggler, or a worker presumed dead) overwrote a finished output. Commit with an atomic rename of a complete file so that any attempt leaves the same, complete result.';
          break;
        }
      }
    }
    const extra = [...fs.keys()].find((n) => outPrefixes.some((p) => n.startsWith(p)) && !expected.has(n));
    if (!outDetail && extra) outDetail = 'There is a file named ' + j(extra) + ', but the job has outputs ' + j([...expected.keys()]) + ' only. Use temporary names that do not start with an output prefix.';
    checks.push(check('The output is exactly right', job.snapshot ? !outDetail : res === undefined || !res, outDetail));

    if (atomic) {
      checks.push(
        check(
          'Outputs never appear half-written',
          !halfSeen,
          halfSeen &&
            'At ' + ms(halfSeen.t) + ' ' + halfSeen.src + ' ' + (halfSeen.how === 'rename' ? 'renamed an incomplete file onto ' : 'wrote directly to ') + halfSeen.name +
            ', which then held ' + halfSeen.have + ' of its ' + halfSeen.want + ' lines. Anyone reading outputs as they appear would see a partial result, and a worker that crashes here leaves it behind. Write the whole file under a temporary name, then rename it to ' + halfSeen.name + ' in one step.',
        ),
      );
    }

    checks.push(
      check(
        'The coordinator leaves the data to the workers',
        fromCoord.length === 0,
        fromCoord.length &&
          'n1 sent ' + plural(fromCoord.length, 'request') + ' to fs (the first: ' + fromCoord[0].type + ' ' + j(fromCoord[0].name) + ' at ' + ms(fromCoord[0].t) +
          '). The coordinator only hands out tasks and tracks them; workers read the input, run map and reduce, and write the files.',
      ),
    );

    const workers = sim.nodeIds.filter((id) => id !== COORD);
    const readers = [...inputReaders].filter((id) => id !== COORD);
    const need = Math.min(2, workers.length);
    checks.push(
      check(
        'Workers share the map work',
        readers.length >= need,
        'Only ' + (readers.length ? readers.join(', ') : 'nobody') + ' ran map tasks (read input splits), with ' + plural(workers.length, 'worker') + ' available. Give each worker that asks a task of its own.',
      ),
    );

    const firstErr = errors[0];
    checks.push(
      check(
        'Every fs request succeeds',
        !firstErr,
        firstErr &&
          'At ' + ms(firstErr.t) + ' ' + firstErr.src + ' sent ' + j(firstErr.req) + ' and fs answered ' + firstErr.code + ': ' + firstErr.text + '.' +
          (firstErr.code === 'not_found' && firstErr.req.type === 'read' ? ' A task read a file that was not (yet) committed: start reduce tasks only after every map task is done.' : '') +
          (errors.length > 1 ? ' (' + (errors.length - 1) + ' more errors.)' : ''),
      ),
    );

    let many = null;
    for (const [name, n] of attempts) if (n > MAX_ATTEMPTS && (!many || n > many.n)) many = { name, n };
    checks.push(
      check(
        'No task runs more than ' + MAX_ATTEMPTS + ' times',
        !many,
        many &&
          (many.name.startsWith('commit:') ? many.name.slice(7) + ' was committed ' : 'Input ' + many.name.slice(5) + ' was read ') + many.n + ' times. Re-run a task only when its worker went silent, and give at most one backup copy of a task at a time.',
      ),
    );

    const allowed = Math.round((rate * sim.duration) / 1000);
    checks.push(
      check(
        'Node-to-node messages stay within budget',
        sim.stats.nodeMsgs <= allowed,
        'Nodes sent ' + sim.stats.nodeMsgs + ' messages to each other in ' + ms(sim.duration) + '; the budget is ' + rate + ' per second (' + allowed + '). An idle worker should ask for work every 100ms or so, not in a tight loop.',
      ),
    );
    return checks;
  };
}

export const mrWordCount = (sim, params) => runJob(sim, { deadline: 2500, ...params });
export const mrCrashes = (sim, params) => runJob(sim, { deadline: 3000, atomic: true, ...params });
export const mrStragglers = (sim, params) => runJob(sim, { deadline: 1600, atomic: true, ...params });
export const mrPipeline = (sim, params) => runJob(sim, { deadline: 3000, atomic: true, ...params });
