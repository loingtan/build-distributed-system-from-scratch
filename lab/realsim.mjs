// Real-time counterpart of src/sim/engine.js.
//
// Same `sim` API for workloads and checkers, but every node is a real OS
// process (optionally inside its own Docker container) speaking JSON lines
// on stdin/stdout, Maelstrom-style:
//
//   runner -> node   {"src":"c1","dest":"n1","body":{...},"id":17}
//   node -> runner   {"src":"n1","dest":"n2","body":{...},"cause":17}
//
// `id` names the event that delivered a message; a node echoes it back as
// `cause` on everything it sends while handling that event. That is how
// checkers can still tell which sends belong to one handler run.
//
// The network between nodes stays under the runner's control: latency, loss,
// partitions and per-message interception work exactly as in the browser
// simulator. Time, scheduling, processes, crashes and disks are real.

import { performance } from 'node:perf_hooks';
import fs from 'node:fs';
import path from 'node:path';
import { mulberry32 } from '../src/sim/engine.js';

const clone = (v) => JSON.parse(JSON.stringify(v));
const same = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

function assertJson(value, where = 'body') {
  if (value === null || value === undefined) return;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return;
  if (t === 'number') {
    if (!Number.isFinite(value)) throw new Error(`Message ${where} is ${value}; messages must be plain JSON.`);
    return;
  }
  if (Array.isArray(value)) return value.forEach((v, i) => assertJson(v, `${where}[${i}]`));
  if (t === 'object') return Object.keys(value).forEach((k) => assertJson(value[k], `${where}.${k}`));
  throw new Error(`Message ${where} is a ${t}; messages must be plain JSON.`);
}

/**
 * @param {object} o
 * @param {(id: string, dataDir: string) => Machine} o.makeMachine  creates one machine per node
 * @param {string} o.workDir  host directory for node data dirs
 */
export async function realSimulate({
  makeMachine,
  workDir,
  nodeCount = 1,
  duration = 5000,
  seed = 1,
  latency = [2, 12],
  dropRate = 0,
  kv = false,
  workload,
  params = {},
  maxEvents = 60000,
  traceCap = 6000,
  bootTimeout = 20000,
  clockSkew = 0,
  signal = null, // aborting ends the test early (its result is discarded by the caller)
}) {
  const rand = mulberry32(seed);
  const nodeIds = Array.from({ length: nodeCount }, (_, i) => `n${i + 1}`);
  const nodeSet = new Set(nodeIds);
  const trace = [];
  const logs = [];
  const errors = [];
  const ops = [];
  const partitions = [];
  const downs = [];
  const pauses = [];
  const services = new Map();
  const pendingClient = new Map();
  const downDisk = new Map();
  const stats = { nodeMsgs: 0, clientMsgs: 0, kvMsgs: 0, dropped: 0 };
  const timers = new Set();
  let groupOf = null;
  let start = null;
  let finished = false;
  let storm = false;
  let order = 0;
  let nextMsgId = 1;
  let eventCounter = 0;
  let currentEvent = 0;
  let timerEvents = 0;

  const now = () => (start === null ? 0 : performance.now() - start);
  const later = (ms, fn) => {
    const h = setTimeout(() => {
      timers.delete(h);
      if (!finished) fn();
    }, Math.max(0, ms));
    timers.add(h);
    return h;
  };
  const withEvent = (id, fn) => {
    const prev = currentEvent;
    currentEvent = id;
    try {
      return fn();
    } finally {
      currentEvent = prev;
    }
  };

  const log = (source, level, text) => {
    if (logs.length < 400) logs.push({ t: now(), source, level, text: String(text).slice(0, 2000) });
    else if (logs.length === 400) logs.push({ t: now(), source: 'sim', level: 'warn', text: 'Log limit reached; later lines are hidden.' });
  };
  const fail = (source, where, message) => {
    if (errors.length < 50) errors.push({ source, t: now(), where, message: String(message) });
    log(source, 'error', `${where}: ${message}`);
  };
  const guard = (source, where, fn) => {
    try {
      const r = fn();
      if (r && typeof r.then === 'function') r.then(null, (e) => fail(source, where, e?.message ?? e));
    } catch (e) {
      fail(source, where, e?.message ?? e);
    }
  };
  const isBlocked = (a, b) => groupOf !== null && groupOf.get(a) !== groupOf.get(b);

  // ------------------------------------------------------------------ nodes
  const dataDirOf = (id) => path.join(workDir, id);
  const diskFile = (id) => path.join(dataDirOf(id), 'disk.json');
  const readDisk = (id) => {
    try {
      return JSON.parse(fs.readFileSync(diskFile(id), 'utf8'));
    } catch {
      return {};
    }
  };
  const writeDisk = (id, value) => {
    const tmp = diskFile(id) + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(value ?? {}));
    fs.renameSync(tmp, diskFile(id));
  };

  const nodes = new Map();
  for (const id of nodeIds) {
    fs.mkdirSync(dataDirOf(id), { recursive: true });
    const machine = makeMachine(id, dataDirOf(id));
    const node = { id, machine, state: 'down', incarnation: 0, queue: [], killing: null, errTail: [], warnedStdout: false, booted: null, paused: null };
    nodes.set(id, node);
    machine.on('out', (inc, line) => onOut(node, inc, line));
    machine.on('err', (inc, line) => onErr(node, inc, line));
    machine.on('exit', (inc, info) => onExit(node, inc, info));
  }

  function boot(node) {
    node.incarnation += 1;
    node.state = 'booting';
    node.queue = [];
    const inc = node.incarnation;
    let resolveBoot;
    node.booted = new Promise((r) => (resolveBoot = r));
    node.resolveBoot = resolveBoot;
    node.machine.start(inc);
    const init = {
      type: 'init',
      msg_id: 0,
      node_id: node.id,
      node_ids: nodeIds,
      data_dir: node.machine.dataDirInside,
      epoch_ms: Math.round(performance.timeOrigin + (start ?? performance.now())),
      clock_offset_ms: clockSkew ? Math.round((rand() * 2 - 1) * clockSkew) : 0,
      kv,
    };
    node.machine.write(inc, JSON.stringify({ src: 'lab', dest: node.id, body: init, id: 0 }));
    const h = setTimeout(() => {
      if (node.incarnation === inc && node.state === 'booting') {
        fail(node.id, 'startup', `did not answer init within ${Math.round(bootTimeout / 1000)}s. ${tail(node)}`);
        resolveBoot(false);
      }
    }, bootTimeout);
    node.booted.then(() => clearTimeout(h));
  }

  const tail = (node) => (node.errTail.length ? `Last output on stderr:\n${node.errTail.join('\n')}` : '');

  function onOut(node, inc, line) {
    if (inc !== node.incarnation || finished) return;
    let env;
    try {
      env = JSON.parse(line);
    } catch {
      env = null;
    }
    if (!env || typeof env !== 'object' || !env.body || typeof env.body !== 'object') {
      if (!node.warnedStdout) {
        node.warnedStdout = true;
        log(node.id, 'warn', 'Wrote something that is not a protocol message to stdout. stdout carries messages only; print logs to stderr.');
      }
      log(node.id, 'log', line);
      return;
    }
    if (env.body.type === 'init_ok') {
      if (node.state === 'booting') {
        node.state = 'up';
        const queued = node.queue;
        node.queue = [];
        for (const m of queued) writeTo(node, m);
        node.resolveBoot(true);
      }
      return;
    }
    if (env.body.type === 'error' && env.body.in_reply_to === 0) return;
    guard(node.id, 'send', () => transmit(node.id, env.dest, env.body, Number.isFinite(env.cause) ? env.cause : 0));
  }

  function onErr(node, inc, line) {
    if (finished) return;
    if (line.startsWith('!error ')) {
      try {
        const e = JSON.parse(line.slice(7));
        fail(node.id, e.where ?? 'handler', e.message ?? line);
        return;
      } catch {
        // fall through and log the raw line
      }
    }
    node.errTail.push(line);
    if (node.errTail.length > 12) node.errTail.shift();
    if (inc === node.incarnation) log(node.id, 'log', line);
  }

  function onExit(node, inc, info) {
    if (inc !== node.incarnation || finished) return;
    if (node.state === 'down') return; // we killed it
    const how = info.signal ? `was killed by ${info.signal}` : `exited with code ${info.code}`;
    fail(node.id, 'process', `${how}. ${tail(node)}`);
    node.state = 'down';
    downs.push({ node: node.id, start: now(), end: null, unexpected: true });
    node.resolveBoot?.(false);
  }

  function writeTo(node, msg) {
    node.machine.write(node.incarnation, JSON.stringify({ src: msg.src, dest: msg.dest, body: msg.body, id: msg.id }));
  }

  // ---------------------------------------------------------------- network
  function transmit(src, dest, body, cause) {
    assertJson(body);
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      throw new Error('A message body must be an object, for example { type: "hello" }.');
    }
    const destIsNode = nodeSet.has(dest);
    const srcIsNode = nodeSet.has(src);
    if (!destIsNode && dest !== 'kv' && !services.has(dest) && !/^(c\d+|probe)$/.test(dest)) {
      throw new Error(`Unknown destination "${dest}". Known nodes: ${nodeIds.join(', ')}.`);
    }
    if (dest === 'kv' && !kv) throw new Error('This challenge has no kv service.');
    const isService = (x) => x === 'kv' || services.has(x);
    const kind = srcIsNode && destIsNode ? 'node' : isService(src) || isService(dest) ? 'kv' : 'client';
    const msg = { src, dest, body: clone(body), t0: now(), t1: null, kind, dropped: false, s0: order++, s1: null, cause };
    if (kind === 'node') stats.nodeMsgs++;
    else if (kind === 'kv') stats.kvMsgs++;
    else if (src !== 'probe' && dest !== 'probe') stats.clientMsgs++;

    let lat = latency[0] + rand() * (latency[1] - latency[0]);
    let lost = kind === 'node' && (isBlocked(src, dest) || rand() < dropRate);
    if (sim.intercept && !lost && src !== 'probe' && dest !== 'probe') {
      const verdict = withEvent(cause, () => sim.intercept({ src, dest, body: msg.body, kind, t: now() }));
      if (verdict === 'drop') lost = true;
      else if (typeof verdict === 'number' && verdict > 0) lat += verdict;
    }
    if (lost) {
      msg.dropped = true;
      msg.t1 = now() + lat;
      stats.dropped++;
    } else {
      later(lat, () => deliver(msg));
    }
    if (src !== 'probe' && dest !== 'probe' && trace.length < traceCap) trace.push(msg);
    if (sim.onSend) withEvent(cause, () => sim.onSend(msg));
    return msg;
  }

  function deliver(msg) {
    if (++eventCounter > maxEvents) {
      storm = true;
      finish();
      return;
    }
    const id = eventCounter;
    msg.t1 = now();
    msg.s1 = order++;
    msg.id = id;
    withEvent(id, () => {
      if (sim.onDeliver) sim.onDeliver(msg);
      if (nodeSet.has(msg.dest)) {
        const node = nodes.get(msg.dest);
        if (node.state === 'down') {
          msg.dropped = true;
          stats.dropped++;
        } else if (node.state === 'booting') {
          node.queue.push(msg);
        } else {
          writeTo(node, msg);
        }
      } else if (msg.dest === 'kv') {
        kvHandle(msg, id);
      } else if (services.has(msg.dest)) {
        const reply = (body) => transmit(msg.dest, msg.src, { ...body, in_reply_to: msg.body.msg_id }, id);
        guard('harness', `service "${msg.dest}"`, () => services.get(msg.dest)({ src: msg.src, dest: msg.dest, body: clone(msg.body) }, reply));
      } else {
        const done = pendingClient.get(msg.body.in_reply_to);
        if (done) {
          pendingClient.delete(msg.body.in_reply_to);
          done(msg.body);
        }
      }
    });
  }

  // A linearizable key-value service, reachable from every node at all times.
  const kvStore = new Map();
  function kvHandle(msg, id) {
    const b = msg.body;
    const reply = (body) => transmit('kv', msg.src, { ...body, in_reply_to: b.msg_id }, id);
    if (b.type === 'read') reply({ type: 'read_ok', value: kvStore.has(b.key) ? kvStore.get(b.key) : null });
    else if (b.type === 'write') {
      kvStore.set(b.key, b.value);
      reply({ type: 'write_ok' });
    } else if (b.type === 'cas') {
      const current = kvStore.has(b.key) ? kvStore.get(b.key) : null;
      if (same(current, b.from)) {
        kvStore.set(b.key, b.to);
        reply({ type: 'cas_ok' });
      } else reply({ type: 'error', text: `expected ${JSON.stringify(b.from ?? null)}, found ${JSON.stringify(current)}` });
    } else reply({ type: 'error', text: `kv does not understand "${b.type}"` });
  }

  function endPause(node) {
    if (!node.paused) return;
    node.paused = null;
    const span = pauses.findLast((s) => s.node === node.id);
    if (span && span.end === null) span.end = now();
  }

  // -------------------------------------------------------------- sim API
  const sim = {
    nodeIds,
    duration,
    stats,
    ops,
    onSend: null,
    onDeliver: null,
    intercept: null,
    rand,
    real: true,
    now,
    eventId: () => currentEvent,
    pick: (arr) => arr[Math.floor(rand() * arr.length)],
    at: (t, fn) => later(start + t - performance.now(), () => withEvent(-(++timerEvents), () => guard('harness', 'workload', fn))),
    sleep: (ms) => new Promise((r) => later(ms, r)),
    partition(groups) {
      sim.heal();
      groupOf = new Map();
      groups.forEach((g, i) => g.forEach((id) => groupOf.set(id, i)));
      partitions.push({ start: now(), end: null, groups: groups.map((g) => [...g]) });
      log('sim', 'info', `Network partition: ${groups.map((g) => g.join(' ')).join('  |  ')}`);
    },
    heal() {
      if (groupOf === null) return;
      groupOf = null;
      partitions[partitions.length - 1].end = now();
      log('sim', 'info', 'Network healed');
    },
    crash(id) {
      const node = nodes.get(id);
      if (!node || node.state === 'down') return;
      node.state = 'down';
      node.queue = [];
      endPause(node);
      node.killing = node.machine.kill(node.incarnation);
      downs.push({ node: id, start: now(), end: null });
      log('sim', 'info', `${id} crashed (process killed)`);
    },
    restart(id) {
      const node = nodes.get(id);
      if (!node || node.state !== 'down') return;
      const span = downs.findLast((d) => d.node === id);
      if (span && span.end === null) span.end = now();
      log('sim', 'info', `${id} restarting`);
      const killing = node.killing ?? Promise.resolve();
      node.state = 'booting'; // counts as up for isUp(); messages queue until init_ok
      node.queue = [];
      // Resolves once the node has run its startup code and answered init.
      return killing.then(() => {
        if (finished) return false;
        if (downDisk.has(id)) {
          writeDisk(id, downDisk.get(id));
          downDisk.delete(id);
        }
        const queued = node.queue;
        boot(node);
        node.queue = queued;
        return node.booted;
      });
    },
    isUp: (id) => !!nodes.get(id) && nodes.get(id).state !== 'down',
    // SIGSTOP the node's process group, SIGCONT it `ms` later. Messages keep
    // being written to its stdin and wait there; its timers fire late.
    pause(id, ms) {
      const node = nodes.get(id);
      if (!node || node.state !== 'up' || node.paused) return;
      const inc = node.incarnation;
      const p = { inc };
      node.paused = p;
      node.machine.pause(inc);
      pauses.push({ node: id, start: now(), end: null });
      log('sim', 'info', `${id} paused for ${Math.round(ms)}ms (SIGSTOP)`);
      later(Math.max(0, Number(ms) || 0), () => {
        if (node.paused !== p) return;
        endPause(node);
        node.machine.resume(inc);
        log('sim', 'info', `${id} resumed (SIGCONT)`);
      });
    },
    isPaused: (id) => !!nodes.get(id)?.paused,
    disk(id) {
      const node = nodes.get(id);
      if (!node) return undefined;
      if (node.state === 'down') {
        if (!downDisk.has(id)) downDisk.set(id, readDisk(id));
        return downDisk.get(id);
      }
      return readDisk(id);
    },
    service(name, handler) {
      services.set(name, handler);
    },
    clientRpc(nodeId, body, { timeout = 1000, client = 'c1', probe = false } = {}) {
      const msg_id = nextMsgId++;
      const src = probe ? 'probe' : client;
      const op = { node: nodeId, client: src, req: body, invoke: now(), complete: null, res: null, msg: null };
      if (!probe) ops.push(op);
      return new Promise((resolve) => {
        pendingClient.set(msg_id, (res) => {
          op.complete = now();
          op.res = res;
          resolve(res);
        });
        op.msg = transmit(src, nodeId, { ...body, msg_id }, currentEvent);
        later(timeout, () => {
          if (pendingClient.delete(msg_id)) resolve(null);
        });
      });
    },
  };

  // ------------------------------------------------------------------ run
  let finishChecks = null;
  let resolveRun;
  const done = new Promise((r) => (resolveRun = r));

  function finish() {
    if (finished) return;
    let checks;
    try {
      checks = finishChecks ? finishChecks() : [];
    } catch (e) {
      checks = [{ name: 'Checker ran', ok: false, detail: `The checker itself failed: ${e?.message ?? e}` }];
    }
    finished = true;
    for (const h of timers) clearTimeout(h);
    timers.clear();
    const end = now();
    const engineChecks = [];
    if (errors.length) {
      const e = errors[0];
      const more = errors.length > 1 ? ` (+${errors.length - 1} more, see Logs)` : '';
      engineChecks.push({ name: 'Nodes run without errors', ok: false, detail: `${e.source} failed in ${e.where} at ${Math.round(e.t)}ms: ${e.message}${more}` });
    }
    if (storm) {
      engineChecks.push({ name: 'No message storm', ok: false, detail: `Stopped after ${maxEvents.toLocaleString()} message deliveries at ${Math.round(end)}ms. Nodes are probably re-sending the same messages forever.` });
    }
    const all = storm ? engineChecks : [...engineChecks, ...checks];
    const completed = ops.filter((o) => o.complete !== null).map((o) => o.complete - o.invoke).sort((a, b) => a - b);
    resolveRun({
      ok: all.every((c) => c.ok),
      checks: all,
      stats: {
        ...stats,
        ops: ops.length,
        unanswered: ops.length - completed.length,
        msgsPerOp: ops.length ? stats.nodeMsgs / ops.length : 0,
        p50: completed.length ? completed[Math.floor(completed.length / 2)] : 0,
        max: completed.length ? completed[completed.length - 1] : 0,
      },
      trace: trace.filter((m) => m.t1 !== null).map(({ src, dest, body, t0, t1, kind, dropped }) => ({ src, dest, body, t0, t1, kind, dropped })),
      traceTruncated: trace.length >= traceCap,
      logs,
      nodeIds,
      kv,
      serviceIds: [...(kv ? ['kv'] : []), ...services.keys()],
      downs: downs.map((d) => ({ ...d, end: d.end ?? end })),
      pauses: pauses.map((p) => ({ ...p, end: p.end ?? end })),
      partitions: partitions.map((p) => ({ ...p, end: p.end ?? end })),
      duration: end,
      real: true,
    });
  }

  if (signal) {
    if (signal.aborted) finished = true;
    else signal.addEventListener('abort', () => finish(), { once: true });
  }
  try {
    // Boot every node and wait until all have answered init; time 0 starts then.
    start = performance.now();
    for (const node of nodes.values()) boot(node);
    const booted = await Promise.all([...nodes.values()].map((n) => n.booted));
    if (booted.some((ok) => !ok)) {
      finish();
    } else {
      start = performance.now();
      finishChecks = workload(sim, params);
      later(duration, finish);
    }
    return await done;
  } finally {
    finished = true;
    for (const h of timers) clearTimeout(h);
    await Promise.all([...nodes.values()].map((n) => n.machine.dispose()));
  }
}
