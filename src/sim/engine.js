// Deterministic cluster simulator.
//
// Runs the learner's code once per node, on a virtual clock, with a network
// that adds latency, drops messages and can be partitioned. Everything is
// driven by one seeded random source, so a given (code, test) pair always
// produces the same run.

export function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Promise continuations are microtasks. After each simulated event we yield
// one macrotask so that every `await` chain it woke up has finished before
// the virtual clock moves on.
const hop = (() => {
  if (typeof setImmediate === 'function') return () => new Promise((r) => setImmediate(r));
  const channel = new MessageChannel();
  let wake = null;
  channel.port1.onmessage = () => wake && wake();
  return () =>
    new Promise((r) => {
      wake = r;
      channel.port2.postMessage(0);
    });
})();

class Heap {
  constructor() {
    this.items = [];
  }
  get size() {
    return this.items.length;
  }
  less(a, b) {
    return a.time < b.time || (a.time === b.time && a.seq < b.seq);
  }
  push(item) {
    const h = this.items;
    h.push(item);
    let i = h.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.less(h[i], h[p])) break;
      [h[i], h[p]] = [h[p], h[i]];
      i = p;
    }
  }
  pop() {
    const h = this.items;
    const top = h[0];
    const last = h.pop();
    if (h.length) {
      h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < h.length && this.less(h[l], h[m])) m = l;
        if (r < h.length && this.less(h[r], h[m])) m = r;
        if (m === i) break;
        [h[i], h[m]] = [h[m], h[i]];
        i = m;
      }
    }
    return top;
  }
}

function assertJson(value, path = 'body') {
  if (value === null || value === undefined) return;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return;
  if (t === 'number') {
    if (!Number.isFinite(value)) throw new Error(`Message ${path} is ${value}; messages must be plain JSON.`);
    return;
  }
  if (t === 'function' || t === 'symbol' || t === 'bigint') {
    throw new Error(`Message ${path} is a ${t}; messages must be plain JSON.`);
  }
  if (Array.isArray(value)) {
    value.forEach((v, i) => assertJson(v, `${path}[${i}]`));
    return;
  }
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    const name = value.constructor?.name ?? 'object';
    throw new Error(
      `Message ${path} is a ${name}. Messages travel as JSON, so convert it first (for example [...mySet]).`,
    );
  }
  for (const k of Object.keys(value)) assertJson(value[k], `${path}.${k}`);
}

const clone = (v) => JSON.parse(JSON.stringify(v));
const same = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

const SHADOWED = ['node', 'console', 'setTimeout', 'setInterval', 'clearTimeout', 'clearInterval'];

export async function simulate({
  code,
  nodeCount = 1,
  duration = 5000,
  seed = 1,
  latency = [2, 12],
  dropRate = 0,
  kv = false,
  workload,
  params = {},
  maxEvents = 60000,
  clockSkew = 0, // each node's clock is off by a fixed amount in [-clockSkew, +clockSkew] ms
  traceCap = 6000,
}) {
  const rand = mulberry32(seed);
  const heap = new Heap();
  let now = 0;
  let seq = 0;
  let order = 0;
  let nextMsgId = 1;
  let storm = false;

  const nodeIds = Array.from({ length: nodeCount }, (_, i) => `n${i + 1}`);
  // Only draw random numbers for skew when asked, so tests without skew keep
  // exactly the same random sequence (and outcomes) as before.
  const clockOffsets = new Map(clockSkew ? nodeIds.map((id) => [id, Math.round((rand() * 2 - 1) * clockSkew)]) : []);
  const nodeSet = new Set(nodeIds);
  const nodes = new Map();
  const trace = [];
  const logs = [];
  const errors = [];
  const ops = [];
  const partitions = [];
  const downs = [];
  const pauses = [];
  // Nodes that are paused right now: id -> { held: [{ run, msg }] }. A paused
  // node keeps its memory, but nothing of it runs: deliveries to it and its own
  // timers are held and run, in their original order, when it resumes.
  const paused = new Map();
  const disks = new Map(nodeIds.map((id) => [id, {}]));
  const services = new Map();
  const pendingClient = new Map();
  const stats = { nodeMsgs: 0, clientMsgs: 0, kvMsgs: 0, dropped: 0 };
  let groupOf = null;

  const schedule = (time, fn) => heap.push({ time, seq: seq++, fn });

  const log = (source, level, text) => {
    if (logs.length < 400) logs.push({ t: now, source, level, text });
    else if (logs.length === 400) logs.push({ t: now, source: 'sim', level: 'warn', text: 'Log limit reached; later lines are hidden.' });
  };

  const fail = (source, where, e) => {
    const message = String(e?.message ?? e);
    if (errors.length < 50) errors.push({ source, t: now, where, message });
    log(source, 'error', `${where}: ${message}`);
  };

  const guard = (source, where, fn) => {
    try {
      const r = fn();
      if (r && typeof r.then === 'function') r.then(null, (e) => fail(source, where, e));
    } catch (e) {
      fail(source, where, e);
    }
  };

  const isBlocked = (a, b) => groupOf !== null && groupOf.get(a) !== groupOf.get(b);

  // Wrap something that belongs to node `id` (a timer, a delivery) so that it
  // waits while the node is paused.
  const gate = (id, fn, msg = null) => {
    const run = () => {
      const p = paused.get(id);
      if (p) p.held.push({ run, msg });
      else fn();
    };
    return run;
  };

  const sim = {
    nodeIds,
    duration,
    stats,
    ops,
    onSend: null,
    onDeliver: null,
    intercept: null,
    rand,
    now: () => now,
    // Index of the event being processed; lets a checker tell whether two sends belong to one handler run.
    eventId: () => events,
    pick: (arr) => arr[Math.floor(rand() * arr.length)],
    at: (t, fn) => schedule(t, () => guard('harness', 'workload', fn)),
    sleep: (ms) => new Promise((r) => schedule(now + ms, r)),
    partition(groups) {
      sim.heal();
      groupOf = new Map();
      groups.forEach((g, i) => g.forEach((id) => groupOf.set(id, i)));
      partitions.push({ start: now, end: null, groups: groups.map((g) => [...g]) });
      log('sim', 'info', `Network partition: ${groups.map((g) => g.join(' ')).join('  |  ')}`);
    },
    heal() {
      if (groupOf === null) return;
      groupOf = null;
      partitions[partitions.length - 1].end = now;
      log('sim', 'info', 'Network healed');
    },
    // Kill a node: its memory, timers and in-flight work are gone. Only node.disk survives.
    crash(id) {
      const node = nodes.get(id);
      if (!node || node.inst.dead) return;
      node.inst.dead = true;
      const p = paused.get(id);
      if (p) {
        // Messages waiting for a paused process die with it.
        paused.delete(id);
        const span = pauses.findLast((s) => s.node === id);
        if (span && span.end === null) span.end = now;
        for (const h of p.held) {
          if (h.msg) {
            h.msg.dropped = true;
            stats.dropped++;
          }
        }
      }
      try {
        assertJson(disks.get(id), 'node.disk');
        disks.set(id, clone(disks.get(id)));
      } catch (e) {
        log(id, 'warn', `${e.message} Anything on node.disk that is not plain JSON is lost in a crash.`);
        disks.set(id, JSON.parse(JSON.stringify(disks.get(id)) ?? '{}'));
      }
      downs.push({ node: id, start: now, end: null });
      log('sim', 'info', `${id} crashed`);
    },
    // Start a crashed node again: the code runs from the top with the old node.disk.
    restart(id) {
      const node = nodes.get(id);
      if (!node || !node.inst.dead) return;
      const span = downs.findLast((d) => d.node === id);
      if (span) span.end = now;
      log('sim', 'info', `${id} restarted`);
      boot(id);
    },
    isUp: (id) => !!nodes.get(id) && !nodes.get(id).inst.dead,
    // Freeze a node for `ms` (a GC pause, a VM migration, SIGSTOP). It keeps its
    // memory; messages to it queue, its timers do not fire. When it resumes it
    // handles everything that piled up, late, and node.now() has jumped ahead.
    pause(id, ms) {
      const node = nodes.get(id);
      if (!node || node.inst.dead || paused.has(id)) return;
      const p = { held: [] };
      paused.set(id, p);
      pauses.push({ node: id, start: now, end: null });
      log('sim', 'info', `${id} paused for ${Math.round(ms)}ms`);
      schedule(now + Math.max(0, Number(ms) || 0), () => {
        if (paused.get(id) !== p) return;
        paused.delete(id);
        const span = pauses.findLast((s) => s.node === id);
        if (span) span.end = now;
        log('sim', 'info', `${id} resumed (${p.held.length} event${p.held.length === 1 ? '' : 's'} waiting)`);
        for (const h of p.held) schedule(now, h.run);
      });
    },
    isPaused: (id) => paused.has(id),
    disk: (id) => disks.get(id),
    // A fake external service run by the harness, e.g. a flaky upstream.
    // handler(msg, reply) is called for each message sent to `name`.
    service(name, handler) {
      services.set(name, handler);
    },
    // One request from a simulated client. Resolves with the reply body, or
    // null if nothing came back in time. `probe` requests are harness
    // measurements: they are kept out of the history and the timeline.
    clientRpc(nodeId, body, { timeout = 1000, client = 'c1', probe = false } = {}) {
      const msg_id = nextMsgId++;
      const src = probe ? 'probe' : client;
      const op = { node: nodeId, client: src, req: body, invoke: now, complete: null, res: null, msg: null };
      if (!probe) ops.push(op);
      return new Promise((resolve) => {
        pendingClient.set(msg_id, (res) => {
          op.complete = now;
          op.res = res;
          resolve(res);
        });
        op.msg = transmit(src, nodeId, { ...body, msg_id });
        schedule(now + timeout, () => {
          if (pendingClient.delete(msg_id)) resolve(null);
        });
      });
    },
  };

  function transmit(src, dest, body) {
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
    const msg = { src, dest, body: clone(body), t0: now, t1: null, kind, dropped: false, s0: order++, s1: null };
    if (kind === 'node') stats.nodeMsgs++;
    else if (kind === 'kv') stats.kvMsgs++;
    else if (src !== 'probe' && dest !== 'probe') stats.clientMsgs++;

    let lat = latency[0] + rand() * (latency[1] - latency[0]);
    let lost = kind === 'node' && (isBlocked(src, dest) || rand() < dropRate);
    // A workload can stage exact interleavings: drop one message, or hold it back
    // so that later ones overtake it. Probe traffic is never intercepted.
    if (sim.intercept && !lost && src !== 'probe' && dest !== 'probe') {
      const verdict = sim.intercept({ src, dest, body: msg.body, kind, t: now });
      if (verdict === 'drop') lost = true;
      else if (typeof verdict === 'number' && verdict > 0) lat += verdict;
    }
    if (lost) {
      msg.dropped = true;
      msg.t1 = now + lat;
      stats.dropped++;
    } else {
      schedule(now + lat, () => deliver(msg));
    }
    if (src !== 'probe' && dest !== 'probe' && trace.length < traceCap) trace.push(msg);
    if (sim.onSend) sim.onSend(msg);
    return msg;
  }

  function deliver(msg) {
    msg.t1 = now;
    msg.s1 = order++;
    if (sim.onDeliver) sim.onDeliver(msg);
    if (nodeSet.has(msg.dest)) {
      const node = nodes.get(msg.dest);
      if (node.inst.dead) {
        // Nobody home: the message is lost.
        msg.dropped = true;
        stats.dropped++;
        return;
      }
      if (paused.has(msg.dest)) {
        // It arrived, but the process is frozen: it waits in the queue.
        gate(msg.dest, () => node.receive(msg), msg)();
        return;
      }
      node.receive(msg);
    } else if (msg.dest === 'kv') {
      kvHandle(msg);
    } else if (services.has(msg.dest)) {
      const reply = (body) => transmit(msg.dest, msg.src, { ...body, in_reply_to: msg.body.msg_id });
      guard('harness', `service "${msg.dest}"`, () => services.get(msg.dest)({ src: msg.src, dest: msg.dest, body: clone(msg.body) }, reply));
    } else {
      const done = pendingClient.get(msg.body.in_reply_to);
      if (done) {
        pendingClient.delete(msg.body.in_reply_to);
        done(msg.body);
      }
    }
  }

  // A linearizable key-value service, reachable from every node at all times.
  const kvStore = new Map();
  function kvHandle(msg) {
    const b = msg.body;
    const reply = (body) => transmit('kv', msg.src, { ...body, in_reply_to: b.msg_id });
    if (b.type === 'read') {
      reply({ type: 'read_ok', value: kvStore.has(b.key) ? kvStore.get(b.key) : null });
    } else if (b.type === 'write') {
      kvStore.set(b.key, b.value);
      reply({ type: 'write_ok' });
    } else if (b.type === 'cas') {
      const current = kvStore.has(b.key) ? kvStore.get(b.key) : null;
      if (same(current, b.from)) {
        kvStore.set(b.key, b.to);
        reply({ type: 'cas_ok' });
      } else {
        reply({ type: 'error', text: `expected ${JSON.stringify(b.from ?? null)}, found ${JSON.stringify(current)}` });
      }
    } else {
      reply({ type: 'error', text: `kv does not understand "${b.type}"` });
    }
  }

  function makeNode(id) {
    const handlers = new Map();
    const pending = new Map();
    const warned = new Set();
    // Set when this incarnation of the node crashes; everything it left
    // behind (timers, awaits) then goes quiet.
    const inst = { dead: false };

    const api = {
      id,
      disk: disks.get(id),
      nodeIds: [...nodeIds],
      peers: nodeIds.filter((n) => n !== id),
      on(type, fn) {
        if (typeof fn !== 'function') throw new Error(`node.on("${type}", …) needs a function.`);
        handlers.set(type, fn);
      },
      send(dest, body) {
        if (inst.dead) return;
        transmit(id, dest, body);
      },
      reply(req, body) {
        if (!req || !req.src || !req.body) throw new Error('node.reply(msg, body) needs the message you are replying to.');
        if (inst.dead) return;
        transmit(id, req.src, { ...body, in_reply_to: req.body.msg_id });
      },
      rpc(dest, body, { timeout = 1000 } = {}) {
        if (inst.dead) return new Promise(() => {});
        const msg_id = nextMsgId++;
        return new Promise((resolve) => {
          pending.set(msg_id, resolve);
          try {
            transmit(id, dest, { ...body, msg_id });
          } catch (e) {
            pending.delete(msg_id);
            throw e;
          }
          schedule(
            now + timeout,
            gate(id, () => {
              if (pending.delete(msg_id) && !inst.dead) resolve(null);
            }),
          );
        });
      },
      after(ms, fn) {
        let live = true;
        schedule(now + Math.max(0, Number(ms) || 0), gate(id, () => live && !inst.dead && guard(id, 'timer', fn)));
        return { cancel: () => (live = false) };
      },
      every(ms, fn) {
        const gap = Math.max(1, Number(ms) || 0);
        let live = true;
        const tick = gate(id, () => {
          if (!live || inst.dead) return;
          guard(id, 'timer', fn);
          schedule(now + gap, tick);
        });
        schedule(now + gap, tick);
        return { cancel: () => (live = false) };
      },
      // A node's own clock: simulated time plus this node's fixed skew.
      now: () => now + (clockOffsets.get(id) ?? 0),
      random: () => rand(),
      log(...args) {
        log(id, 'log', args.map((a) => (typeof a === 'string' ? a : safeJson(a))).join(' '));
      },
    };

    if (kv) {
      api.kv = {
        read: async (key) => (await api.rpc('kv', { type: 'read', key }, { timeout: 5000 }))?.value ?? null,
        write: async (key, value) => {
          await api.rpc('kv', { type: 'write', key, value }, { timeout: 5000 });
        },
        cas: async (key, from, to) =>
          (await api.rpc('kv', { type: 'cas', key, from: from ?? null, to }, { timeout: 5000 }))?.type === 'cas_ok',
      };
    }

    const receive = (msg) => {
      if (inst.dead) return;
      const body = msg.body;
      const irt = body.in_reply_to;
      if (irt != null && pending.has(irt)) {
        const resolve = pending.get(irt);
        pending.delete(irt);
        resolve(clone(body));
        return;
      }
      const handler = handlers.get(body.type);
      if (!handler) {
        if (irt == null && !warned.has(body.type)) {
          warned.add(body.type);
          log(id, 'warn', `No handler for "${body.type}" messages (from ${msg.src}); ignoring them.`);
        }
        return;
      }
      guard(id, `handler "${body.type}"`, () => handler({ src: msg.src, dest: msg.dest, body: clone(body) }));
    };

    return { api, receive, inst };
  }

  const result = (checks) => {
    const done = ops.filter((o) => o.complete !== null).map((o) => o.complete - o.invoke).sort((a, b) => a - b);
    return {
      ok: checks.every((c) => c.ok),
      checks,
      stats: {
        ...stats,
        ops: ops.length,
        unanswered: ops.length - done.length,
        msgsPerOp: ops.length ? stats.nodeMsgs / ops.length : 0,
        p50: done.length ? done[Math.floor(done.length / 2)] : 0,
        max: done.length ? done[done.length - 1] : 0,
      },
      // Messages still in flight when the run ended have no arrival time; leave them out.
      trace: trace
        .filter((m) => m.t1 !== null)
        .map(({ src, dest, body, t0, t1, kind, dropped }) => ({ src, dest, body, t0, t1, kind, dropped })),
      traceTruncated: trace.length >= traceCap,
      logs,
      nodeIds,
      kv,
      serviceIds: [...(kv ? ['kv'] : []), ...services.keys()],
      downs: downs.map((d) => ({ ...d, end: d.end ?? now })),
      pauses: pauses.map((d) => ({ ...d, end: d.end ?? now })),
      partitions: partitions.map((p) => ({ ...p, end: p.end ?? now })),
      duration: now,
    };
  };

  // Compile once, so a syntax error is reported once rather than per node.
  let factory;
  try {
    factory = new Function(...SHADOWED, `"use strict";\n${code}`);
  } catch (e) {
    return result([{ name: 'Code compiles', ok: false, detail: String(e?.message ?? e) }]);
  }

  function boot(id) {
    const node = makeNode(id);
    nodes.set(id, node);
    const { api } = node;
    const consoleShim = { log: api.log, info: api.log, warn: api.log, error: api.log, debug: api.log };
    const clear = (h) => h && typeof h.cancel === 'function' && h.cancel();
    guard(id, 'startup', () =>
      factory(api, consoleShim, (fn, ms) => api.after(ms, fn), (fn, ms) => api.every(ms, fn), clear, clear),
    );
  }
  for (const id of nodeIds) boot(id);

  const finish = workload(sim, params);

  let events = 0;
  while (heap.size) {
    const ev = heap.pop();
    if (ev.time > duration) break;
    now = ev.time;
    ev.fn();
    await hop();
    if (++events > maxEvents) {
      storm = true;
      break;
    }
  }
  if (!storm) now = duration;

  let checks;
  try {
    checks = finish();
  } catch (e) {
    checks = [{ name: 'Checker ran', ok: false, detail: `The checker itself failed: ${e?.message ?? e}` }];
  }

  const engineChecks = [];
  if (errors.length) {
    const e = errors[0];
    const more = errors.length > 1 ? ` (+${errors.length - 1} more, see Logs)` : '';
    engineChecks.push({
      name: 'Nodes run without errors',
      ok: false,
      detail: `${e.source} threw in ${e.where} at ${Math.round(e.t)}ms: ${e.message}${more}`,
    });
  }
  if (storm) {
    engineChecks.push({
      name: 'No message storm',
      ok: false,
      detail: `Stopped after ${maxEvents.toLocaleString()} events at ${Math.round(now)}ms. Nodes are probably re-sending the same messages forever.`,
    });
  }
  // After a storm the run was cut short, so the workload's own checks would only add noise.
  return result(storm ? engineChecks : [...engineChecks, ...checks]);
}

function safeJson(v) {
  try {
    if (v instanceof Set) return `Set(${JSON.stringify([...v])})`;
    if (v instanceof Map) return `Map(${JSON.stringify([...v])})`;
    const s = JSON.stringify(v);
    return s === undefined ? String(v) : s;
  } catch {
    return String(v);
  }
}
