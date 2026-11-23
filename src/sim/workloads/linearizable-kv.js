// Workloads for the linearizable key-value store track.
//
// Clients talk to the cluster with:
//
//   put { key, value }        -> put_ok
//   get { key }               -> get_ok { value }
//   cas { key, from, to }     -> cas_ok | cas_failed
//   any of the three          -> not_leader { leader }
//   status                    -> status_ok { role, term, leader }   (used to find the leader)
//   log                       -> log_ok { entries, commit }         (task 3: counts log entries)
//
// From task 2 on, every request also carries client_id and seq.
//
// Every value written is unique, so each value a get returns can be traced
// to the one operation that wrote it. At the end the history of each key is
// checked for linearizability with the Wing–Gong search as improved by Lowe
// (the algorithm behind Knossos and Porcupine): look for one total order of
// the operations that respects real time and the rules of a register with
// compare-and-set. Linearizability is local, so keys are checked one by one.

import { check, j, plural } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;
const show = (v) => (v === null || v === undefined ? 'null' : JSON.stringify(v));

const START = 1500; // clients start here
const STRICT_FROM = 2000; // requests sent from here on must be answered
const WAIT = 500; // a client waits this long for one reply
const PATIENCE = 3000; // with sessions, a client retries one request this long
const LIMIT = 2000; // while healthy, every request is answered within this
const SETTLE = 2500; // time the cluster gets to recover from a fault

// ------------------------------------------------------ linearizability

// One key is a register: it holds a value, null until written. Ops are
// { kind, value (put), from, to (cas), out (get result), ok (cas result),
//   invoke, complete } with complete === null when no reply came.
function step(state, o) {
  if (o.kind === 'put') return [true, o.value];
  if (o.kind === 'get') return [o.out === state, state];
  const match = state === o.from;
  if (o.complete === null) return [true, match ? o.to : state]; // outcome unknown: either is fine
  if (o.ok) return [match, match ? o.to : state];
  return [!match, state];
}

const SEARCH_CAP = 400000;

// True when some order of ops, consistent with real time, explains every
// reply. Ops without a reply may take effect at any point after they were
// sent, or never.
export function linearizable(input) {
  const ops = input.filter((o) => o.complete !== null || o.kind !== 'get'); // an unanswered get constrains nothing
  const n = ops.length;
  if (n === 0) return true;
  const ev = [];
  for (let i = 0; i < n; i++) {
    ev.push({ t: ops[i].invoke, call: 1, i });
    ev.push({ t: ops[i].complete === null ? Infinity : ops[i].complete, call: 0, i });
  }
  // Calls before returns at the same instant: touching operations count as concurrent.
  ev.sort((a, b) => a.t - b.t || b.call - a.call || a.i - b.i);
  const N = ev.length;
  const END = N + 1;
  const next = new Int32Array(N + 2);
  const prev = new Int32Array(N + 2);
  for (let k = 0; k <= N; k++) {
    next[k] = k + 1;
    prev[k + 1] = k;
  }
  const callAt = new Int32Array(n);
  const retAt = new Int32Array(n);
  ev.forEach((e, k) => {
    if (e.call) callAt[e.i] = k + 1;
    else retAt[e.i] = k + 1;
  });
  const lift = (i) => {
    const c = callAt[i];
    const r = retAt[i];
    next[prev[c]] = next[c];
    prev[next[c]] = prev[c];
    next[prev[r]] = next[r];
    prev[next[r]] = prev[r];
  };
  const unlift = (i) => {
    const c = callAt[i];
    const r = retAt[i];
    next[prev[r]] = r;
    prev[next[r]] = r;
    next[prev[c]] = c;
    prev[next[c]] = c;
  };

  const seen = new Set();
  const stack = [];
  let state = null;
  let done = 0n;
  let entry = next[0];
  while (entry !== END) {
    const e = ev[entry - 1];
    if (e.call) {
      const [ok, ns] = step(state, ops[e.i]);
      if (ok) {
        const nd = done | (1n << BigInt(e.i));
        const key = nd.toString(36) + ':' + show(ns);
        if (!seen.has(key)) {
          if (seen.size > SEARCH_CAP) return true; // too many interleavings to decide: give the benefit of the doubt
          seen.add(key);
          stack.push(entry, state);
          state = ns;
          done = nd;
          lift(e.i);
          entry = next[0];
          continue;
        }
      }
      entry = next[entry];
    } else {
      // The earliest outstanding reply: its op had to be ordered by now.
      if (e.t === Infinity) return true; // only unanswered ops are left
      if (!stack.length) return false;
      const s0 = stack.pop();
      const en = stack.pop();
      const i = ev[en - 1].i;
      state = s0;
      done &= ~(1n << BigInt(i));
      unlift(i);
      entry = next[en];
    }
  }
  return true;
}

// The history as a client would have seen it at time t.
function prefix(ops, t) {
  return ops.filter((o) => o.invoke <= t).map((o) => (o.complete !== null && o.complete > t ? { ...o, complete: null } : o));
}

// The first reply that no order can explain: binary search on the reply times.
function culprit(ops) {
  const times = [...new Set(ops.filter((o) => o.complete !== null).map((o) => o.complete))].sort((a, b) => a - b);
  let lo = 0;
  let hi = times.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (linearizable(prefix(ops, times[mid]))) lo = mid + 1;
    else hi = mid;
  }
  return ops.find((o) => o.complete === times[lo]);
}

// ------------------------------------------------------------ workload

function kv(sim, p) {
  const {
    scenario = 'stable',
    sessions = false,
    clients = 4,
    keys = 4,
    reads = 0.45, // share of gets
    cas = 0.25, // share of cas; the rest are puts
    gap = 50,
    dup = 0, // chance that a write is sent as two copies at once
    lateDup = 0, // chance, after a request, that an old put of this client is delivered again
    dropReplies = 0, // chance that a reply to a client is lost
    logBound = false,
    diskLimit = 0,
  } = p;
  const ids = sim.nodeIds;
  const keyNames = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  const stopAt = sim.duration - 1000;
  const strictEnd = sim.duration - LIMIT - 400;

  const problem = { silent: null, shape: null, reply: null, copies: null };
  const lives = new Map(ids.map((n) => [n, 0]));
  const statuses = [];
  const quiet = []; // { from, until }: faults; requests in flight here may go unanswered
  const progress = []; // { from, to, what, reads }: something must be answered in here
  const cuts = []; // { from, to, minority }
  const readOnly = []; // { from, to }: clients send no writes in here
  const stages = []; // staged scenarios that must have run: { name, fired, why }
  const inReadOnly = (t) => readOnly.some((w) => t >= w.from && t < w.to);

  // ------------------------------------------------------------ status

  const seeStatus = (node, t, res, life) => {
    const sameLife = lives.get(node) === life && sim.isUp(node);
    if (!res) {
      if (sameLife) problem.silent ??= `${node} did not answer { type: "status" } sent at ${ms(t)}. Keep the status handler: the tests use it to find the leader.`;
      return;
    }
    if (res.type !== 'status_ok' || !Number.isInteger(res.term) || typeof res.role !== 'string') {
      problem.shape ??= `${node} replied ${j(res)} to { type: "status" }; expected { type: "status_ok", role, term, leader }.`;
      return;
    }
    statuses.push({ t, node, role: res.role, term: res.term });
  };
  const probe = (node) => {
    if (!sim.isUp(node)) return;
    const t = sim.now();
    const life = lives.get(node);
    sim.clientRpc(node, { type: 'status' }, { probe: true, timeout: 300 }).then((res) => seeStatus(node, t, res, life));
  };
  for (let t = 100; t < sim.duration - 400; t += 100) sim.at(t, () => ids.forEach(probe));

  const currentLeader = () => {
    const recent = statuses.filter((o) => o.t >= sim.now() - 250 && o.role === 'leader' && sim.isUp(o.node));
    recent.sort((a, b) => b.term - a.term || b.t - a.t);
    return recent[0]?.node ?? ids[0];
  };
  // The status samples of a node just before and just after time t.
  const around = (node, t) => {
    let before = null;
    let after = null;
    for (const s of statuses) {
      if (s.node !== node || Math.abs(s.t - t) > 150) continue;
      if (s.t <= t && (!before || s.t > before.t)) before = s;
      if (s.t > t && (!after || s.t < after.t)) after = s;
    }
    return { before, after };
  };
  const followers = (leader) => {
    const rest = ids.filter((n) => n !== leader);
    for (let i = rest.length - 1; i > 0; i--) {
      const k = Math.floor(sim.rand() * (i + 1));
      [rest[i], rest[k]] = [rest[k], rest[i]];
    }
    return rest;
  };

  // ------------------------------------------------------------ faults

  const crash = (nodes) => {
    for (const n of nodes) {
      if (!sim.isUp(n)) continue;
      sim.crash(n);
      lives.set(n, lives.get(n) + 1);
    }
  };
  const restart = (nodes) => {
    for (const n of nodes) {
      sim.restart(n);
      probe(n);
    }
  };
  const cut = (minority, until) => {
    sim.partition([minority, ids.filter((n) => !minority.includes(n))]);
    cuts.push({ from: sim.now(), to: until, minority });
  };
  const cutOff = (node, t) => cuts.find((c) => c.minority.includes(node) && t >= c.from && t <= c.to);

  // Staging hooks. attemptOf maps each client request's msg_id to its op.
  const attemptOf = new Map();
  const hooks = []; // ({ src, dest, body, op }) => 'drop' | undefined
  sim.intercept = ({ src, dest, body, kind }) => {
    if (kind !== 'client' || !ids.includes(src)) return undefined;
    const op = attemptOf.get(body.in_reply_to);
    for (const h of hooks) {
      const verdict = h({ src, dest, body, op });
      if (verdict) return verdict;
    }
    if (dropReplies && sim.rand() < dropReplies) {
      if (op) op.drops = (op.drops ?? 0) + 1;
      return 'drop';
    }
    return undefined;
  };

  // ------------------------------------------------------------ clients

  const hist = []; // logical operations
  const seqOf = new Map();
  const hint = new Map(); // client -> node that last gave it an answer
  const lastSeen = new Map(); // client -> key -> value
  let counter = 0;
  let writeAttempts = 0;

  const up = () => ids.filter((n) => sim.isUp(n));
  const randomNode = (avoid) => {
    const c = up().filter((n) => n !== avoid);
    return c.length ? c[Math.floor(sim.rand() * c.length)] : ids[0];
  };
  const startNode = (client) => {
    const h = hint.get(client);
    return h && sim.isUp(h) && sim.rand() >= 0.25 ? h : randomNode(null);
  };

  function newOp(client, kind, key, extra = {}) {
    const op = { id: hist.length, client, kind, key, invoke: sim.now(), complete: null, out: null, ok: null, node: null, attempts: 0, lateDups: [], seq: null, ...extra };
    const body = { type: kind, key };
    if (kind === 'put') body.value = op.value;
    if (kind === 'cas') {
      body.from = op.from;
      body.to = op.to;
    }
    if (sessions) {
      op.seq = (seqOf.get(client) ?? 0) + 1;
      seqOf.set(client, op.seq);
      body.client_id = client;
      body.seq = op.seq;
    }
    op.body = body;
    hist.push(op);
    return op;
  }

  const definite = (kind, res) =>
    kind === 'get' ? res.type === 'get_ok' : kind === 'put' ? res.type === 'put_ok' : res.type === 'cas_ok' || res.type === 'cas_failed';
  const expected = (kind) =>
    kind === 'get' ? '{ type: "get_ok", value }' : kind === 'put' ? '{ type: "put_ok" }' : '{ type: "cas_ok" } or { type: "cas_failed" }';

  // Send one attempt (two identical copies at once for a duplicated write).
  async function attempt(client, target, op, copies) {
    const calls = [];
    for (let i = 0; i < copies; i++) {
      const p = sim.clientRpc(target, op.body, { client, timeout: WAIT });
      attemptOf.set(sim.ops[sim.ops.length - 1].msg.body.msg_id, op);
      if (op.kind !== 'get') writeAttempts++;
      calls.push(p.then((res) => ({ res, t: sim.now() })));
    }
    const outs = await Promise.all(calls);
    const good = outs.filter((o) => o.res && definite(op.kind, o.res)).sort((a, b) => a.t - b.t);
    if (good.length === 2) {
      const [a, b] = good;
      const same = a.res.type === b.res.type && show(a.res.value) === show(b.res.value);
      if (!same) {
        const bare = ({ in_reply_to, ...r }) => r;
        problem.copies ??= `${client} sent two copies of ${j(op.body)} to ${target} at ${ms(op.invoke)}. One was answered ${j(bare(a.res))}, the other ${j(bare(b.res))}: the request took effect twice. Deduplicate on (client_id, seq) when the entry is applied, and answer a duplicate with the reply the original got.`;
      }
    }
    if (good.length) return { res: good[0].res, t: good[0].t, definite: true };
    const other = outs.find((o) => o.res);
    return { res: other ? other.res : null, definite: false };
  }

  // One request, the way a client library does it: follow not_leader
  // hints; on silence, give up (no sessions: a retry might apply it twice)
  // or retry elsewhere with the same seq (sessions).
  async function request(client, op, opts = {}) {
    await tryRequest(client, op, opts);
    if (op.complete === null) op.gaveUp = sim.now();
  }
  async function tryRequest(client, op, { pinned = null, once = false } = {}) {
    const started = sim.now();
    let target = pinned ?? startNode(client);
    const copies = op.kind !== 'get' && dup && sim.rand() < dup ? 2 : 1;
    while (sim.now() - started < PATIENCE) {
      if (op.kind !== 'get' && op.attempts > 0 && inReadOnly(sim.now())) return; // abandoned; outcome unknown
      op.attempts++;
      const out = await attempt(client, target, op, copies);
      if (out.definite) {
        op.complete = out.t;
        op.node = target;
        op.res = out.res;
        if (op.kind === 'get') op.out = out.res.value ?? null;
        if (op.kind === 'cas') op.ok = out.res.type === 'cas_ok';
        if (!pinned) hint.set(client, target);
        return;
      }
      if (out.res && out.res.type !== 'not_leader') {
        problem.reply ??= `${target} replied ${j(out.res)} to ${j(op.body)}; expected ${expected(op.kind)}, or { type: "not_leader", leader } from a node that is not the leader.`;
        return;
      }
      if (once) return;
      if (out.res) {
        const h = out.res.leader;
        target = pinned ?? (typeof h === 'string' && h !== target && ids.includes(h) && sim.isUp(h) ? h : randomNode(target));
        await sim.sleep(20);
        continue;
      }
      if (hint.get(client) === target) hint.delete(client);
      if (!sessions) return;
      target = pinned ?? randomNode(target);
    }
  }

  function remember(client, op) {
    if (op.complete === null) return;
    const m = lastSeen.get(client) ?? new Map();
    lastSeen.set(client, m);
    if (op.kind === 'get') m.set(op.key, op.out);
    else if (op.kind === 'put') m.set(op.key, op.value);
    else if (op.ok) m.set(op.key, op.to);
  }

  function pickOp(client, key, forceRead) {
    const r = sim.rand();
    if (forceRead || r < reads) return newOp(client, 'get', key);
    if (r < reads + cas) {
      const known = lastSeen.get(client)?.get(key);
      const from = known === undefined || sim.rand() < 0.15 ? Math.max(0, counter - Math.floor(sim.rand() * 6)) || null : known;
      return newOp(client, 'cas', key, { from, to: ++counter });
    }
    return newOp(client, 'put', key, { value: ++counter });
  }

  // An old put of this client, delivered again after the client moved on.
  async function replayOld(client) {
    const old = hist.filter((o) => o.client === client && o.kind === 'put' && o.complete !== null && o.complete < sim.now() - 150);
    if (old.length < 2) return null;
    const o = old[Math.floor(sim.rand() * (old.length - 1))]; // never the newest
    const target = hint.get(client) && sim.isUp(hint.get(client)) ? hint.get(client) : randomNode(null);
    o.lateDups.push(sim.now());
    writeAttempts++;
    await sim.clientRpc(target, o.body, { client, timeout: 300 });
    return o.key;
  }

  for (let c = 1; c <= clients; c++) {
    const client = `c${c}`;
    sim.at(START + c * 23, async () => {
      let follow = null; // read this key next
      while (sim.now() < stopAt) {
        const ro = inReadOnly(sim.now());
        const key = follow ?? keyNames[Math.floor(sim.rand() * keys)];
        const op = pickOp(client, key, ro || follow !== null);
        follow = null;
        await request(client, op);
        remember(client, op);
        if (lateDup && !inReadOnly(sim.now()) && sim.now() < stopAt && sim.rand() < lateDup) follow = await replayOld(client);
        await sim.sleep(gap * (0.5 + sim.rand()));
      }
    });
  }

  // Old puts from every client, delivered again to the leader, each followed
  // by a read of its key from another client.
  let burstSeq = 0;
  const replayBurst = (before, extra = []) => {
    const leader = currentLeader();
    const replaced = (o) => hist.some((x) => x.key === o.key && x.kind !== 'get' && x.complete !== null && (x.kind === 'put' || x.ok) && x.invoke > o.complete && x.complete < before);
    for (const client of [...Array.from({ length: clients }, (_, i) => `c${i + 1}`), ...extra]) {
      const old = hist.filter((o) => o.client === client && o.kind === 'put' && o.complete !== null && o.complete < before - 300 && replaced(o));
      if (!old.length) continue;
      const o = old[Math.floor(sim.rand() * old.length)];
      o.lateDups.push(sim.now());
      writeAttempts++;
      sim.clientRpc(leader, o.body, { client, timeout: 300 }).then(async () => {
        await sim.sleep(30);
        const reader = `c${7 + (burstSeq++ % 2)}`;
        await request(reader, newOp(reader, 'get', o.key));
      });
    }
  };

  // A client that keeps reading from one node it believes is the leader.
  const pinnedReader = (client, node, from, to, key = null) => {
    sim.at(from, async () => {
      while (sim.now() < to) {
        const op = newOp(client, 'get', key ?? keyNames[Math.floor(sim.rand() * keys)]);
        await request(client, op, { pinned: node, once: true });
        await sim.sleep(60);
      }
    });
  };
  // A client that keeps reading one key through the normal client logic.
  const reader = (client, from, to, keyFn) => {
    sim.at(from, async () => {
      while (sim.now() < to) {
        const op = newOp(client, 'get', keyFn());
        await request(client, op);
        await sim.sleep(60);
      }
    });
  };

  // ---------------------------------------------------------- scenarios

  // The reply to one cas is lost and the leader that sent it crashes, at that
  // instant (the entry is committed but no follower knows yet) or `delay` ms
  // later (followers have applied it). The client retries with the same seq.
  const lostReply = (t, n, delay = 0) => {
    const client = 'c6';
    const key = `s${n}`;
    const stage = { name: `lost reply at ${ms(t)}`, fired: false, why: null, at: null, node: null, op: null };
    stages.push(stage);
    quiet.push({ from: t, until: t + SETTLE + 500 });
    sim.at(t, async () => {
      const first = newOp(client, 'put', key, { value: ++counter });
      await request(client, first);
      if (first.complete === null) {
        stage.why = `${client}'s put to ${key} at ${ms(t)} was never answered, so the scenario could not start.`;
        return;
      }
      const op = newOp(client, 'cas', key, { from: first.value, to: ++counter, staged: true });
      stage.op = op;
      hooks.push(({ src, body, op: o }) => {
        if (stage.fired || o !== op || body.type !== 'cas_ok') return undefined;
        stage.fired = true;
        stage.at = sim.now();
        stage.node = src;
        sim.at(sim.now() + delay, () => crash([src]));
        sim.at(sim.now() + delay + 1500, () => restart([src]));
        return 'drop';
      });
      await request(client, op);
      if (!stage.fired) stage.why = `${client} sent ${j(op.body)} at ${ms(op.invoke)}, but no node answered it with cas_ok${op.res ? ` (the answer was ${j(op.res)})` : ''}, so its reply could not be lost.`;
      await request(client, newOp(client, 'get', key));
    });
  };

  switch (scenario) {
    case 'leader-crash':
      quiet.push({ from: 3000, until: 3000 + SETTLE });
      sim.at(3000, () => {
        const l = currentLeader();
        crash([l]);
        sim.at(4500, () => restart([l]));
      });
      break;
    case 'leader-cut': {
      const heal = 6000;
      quiet.push({ from: 3000, until: heal + SETTLE });
      progress.push({ from: 3000 + SETTLE, to: heal, within: SETTLE, what: 'the partition at 3000ms' });
      sim.at(3000, () => {
        const l = currentLeader();
        cut([l], heal);
        pinnedReader('c8', l, 3100, heal - 100);
      });
      sim.at(heal, () => sim.heal());
      break;
    }
    case 'lost-reply':
      lostReply(3000, 1);
      lostReply(6000, 2, 150);
      break;
    case 'cut-restart':
      quiet.push({ from: 2800, until: 4300 + SETTLE });
      sim.at(2800, () => cut([currentLeader()], 4300));
      sim.at(4300, () => sim.heal());
      quiet.push({ from: 6800, until: 7300 + SETTLE });
      sim.at(6800, () => crash(ids));
      sim.at(7300, () => restart(ids));
      break;
    case 'noop': {
      // Crash the leader the moment it acknowledges a write; then no writes
      // for 2s, while a reader asks for the key that was just written.
      const stage = { name: 'leader crash after a write', fired: false, why: null, at: null, node: null, op: null };
      stages.push(stage);
      sim.at(3000, () => {
        hooks.push(({ src, body, op }) => {
          if (stage.fired || !op || op.kind === 'get' || !(body.type === 'put_ok' || body.type === 'cas_ok')) return undefined;
          stage.fired = true;
          stage.at = sim.now();
          stage.node = src;
          stage.op = op;
          crash([src]);
          readOnly.push({ from: stage.at, to: stage.at + 2500 });
          quiet.push({ from: stage.at, until: stage.at + 3000 + SETTLE });
          progress.push({ from: stage.at + 300, to: stage.at + 2500, what: `the crash of ${src} at ${ms(stage.at)}`, reads: true });
          reader('c8', stage.at + 1, stage.at + 2500, () => op.key);
          sim.at(stage.at + 3000, () => restart([src]));
          return undefined; // the client does get its put_ok
        });
      });
      sim.at(4000, () => {
        if (!stage.fired) stage.why = 'Between 3000ms and 4000ms no write was acknowledged, so the leader could not be crashed right after one.';
      });
      break;
    }
    case 'lagging-follower': {
      // A follower misses many writes; later it must be the one that helps
      // the survivor commit.
      let leader = null;
      quiet.push({ from: 6000, until: 6000 + SETTLE });
      progress.push({ from: 6000 + SETTLE, to: 9000, within: SETTLE, what: 'the leader crash at 6000ms (only the follower that fell behind and one other node are running)' });
      sim.at(2500, () => {
        leader = currentLeader();
        cut([followers(leader)[0]], 4500);
      });
      sim.at(4500, () => sim.heal());
      sim.at(6000, () => {
        leader = currentLeader();
        crash([leader]);
      });
      sim.at(9000, () => restart([leader]));
      break;
    }
    case 'full-restart':
      // After the restart, clients only read for 1.5s while old puts from
      // before the crash arrive again: nothing newer from those clients has
      // been applied since the restart, so only a remembered session table
      // can tell they are old.
      quiet.push({ from: 4500, until: 5000 + SETTLE });
      sim.at(4500, () => crash(ids));
      sim.at(5000, () => restart(ids));
      readOnly.push({ from: 5000, to: 6500 });
      // c5 writes twice early on and then goes quiet, so by the crash its
      // session lives only in the snapshot.
      sim.at(2000, async () => {
        await request('c5', newOp('c5', 'put', 'q1', { value: ++counter }));
        await sim.sleep(50);
        await request('c5', newOp('c5', 'put', 'q1', { value: ++counter }));
      });
      for (const t of [5700, 5950, 6200]) sim.at(t, () => replayBurst(4500, ['c5']));
      break;
    default:
  }

  // ------------------------------------------------------- disk and log

  const disk = { max: new Map(), samples: 0 };
  if (diskLimit) {
    for (let t = 200; t < sim.duration; t += 100) {
      sim.at(t, () => {
        disk.samples++;
        for (const n of ids) {
          let size = 0;
          try {
            size = JSON.stringify(sim.disk(n) ?? {}).length;
          } catch {
            size = Infinity;
          }
          const prev = disk.max.get(n);
          if (!prev || size > prev.size) disk.max.set(n, { size, t: sim.now() });
        }
      });
    }
  }

  const finalLogs = new Map();
  if (logBound) {
    sim.at(sim.duration - 300, () => {
      for (const n of ids) {
        if (!sim.isUp(n)) continue;
        sim.clientRpc(n, { type: 'log' }, { probe: true, timeout: 200 }).then((res) => finalLogs.set(n, res));
      }
    });
  }

  // ------------------------------------------------------------- checks

  return () => {
    const checks = [
      check('Every node answers status requests', !problem.silent, problem.silent),
      check('Replies are well formed', !problem.shape && !problem.reply, problem.shape ?? problem.reply),
    ];
    if (problem.shape || problem.reply) return checks;

    // Liveness: while nothing is broken, every request is answered in time.
    // A request is excused if a fault started while it was outstanding, or if
    // the run ended before it could finish.
    const faultDuring = (a, b) => quiet.some((q) => q.from <= b && q.until >= a);
    const counted = hist.filter((o) => {
      if (o.client === 'c8' || o.staged || o.invoke < STRICT_FROM) return false;
      const end = Math.min(o.complete ?? o.gaveUp ?? Infinity, o.invoke + LIMIT);
      if (o.complete === null && o.invoke > strictEnd) return false;
      return !faultDuring(o.invoke, end);
    });
    // Each reply the test itself dropped costs the client one wait.
    const late = counted.filter((o) => (o.complete === null ? (o.drops ?? 0) < 4 : o.complete - o.invoke > LIMIT + WAIT * (o.drops ?? 0)));
    const first = late[0];
    checks.push(
      check(
        sessions ? 'Every request is answered within 2s while the cluster is healthy' : 'Every request is answered while the cluster is healthy',
        counted.length > 0 && late.length === 0,
        first
          ? `${plural(late.length, 'request')} of ${counted.length} sent while no node was down or cut off ${late.length === 1 ? 'was' : 'were'} not answered${sessions ? ' within 2s' : ''}. First: ${first.client} sent ${j(first.body)} at ${ms(first.invoke)}${first.complete === null ? (sessions ? ' and retried it for 3s without a definite reply.' : ` and heard nothing within ${WAIT}ms.`) : ` and got its answer only at ${ms(first.complete)}.`} The leader must answer put, get and cas once the entry is committed and applied.`
          : hist.some((o) => o.complete !== null)
            ? 'No request was sent while the cluster was healthy.'
            : `No request was ever answered.${sessions ? ' If gets wait for something, check that it can happen even when no write arrives.' : ''}`,
      ),
    );
    for (const w of progress) {
      const got = hist.some((o) => o.complete !== null && o.complete > w.from && o.complete < w.to && (w.reads ? o.kind === 'get' : o.client !== 'c8'));
      checks.push(
        check(
          w.reads ? 'Reads are answered within 2.5s of losing the leader, with no writes arriving' : `Requests are answered again within ${w.within / 1000}s of ${w.what.split(' (')[0]}`,
          got,
          w.reads
            ? `No get was answered between ${ms(w.from)} and ${ms(w.to)}. After ${w.what}, clients sent only gets. A new leader does not know which entries are committed until it commits one from its own term, so it cannot answer reads until then: append an empty (no-op) entry as soon as it is elected.`
            : `No request was answered between ${ms(w.from)} and ${ms(w.to)}, although a majority of nodes could reach each other after ${w.what}. They must elect a leader and keep committing.`,
        ),
      );
    }
    for (const s of stages) checks.push(check(`The ${s.name} scenario ran`, s.fired, s.why ?? `The ${s.name} scenario never happened.`));

    // Exactly once: direct evidence that a request took effect twice.
    let twice = problem.copies;
    if (sessions) {
      for (const c of hist) {
        if (twice) break;
        if (c.kind !== 'cas' || c.complete === null || c.ok) continue;
        const seen = hist.find((o) => o.key === c.key && o.complete !== null && ((o.kind === 'get' && o.out === c.to) || (o.kind === 'cas' && o.ok && o.from === c.to)));
        if (seen) {
          twice = `${c.client}'s ${j(c.body)} was answered cas_failed at ${ms(c.complete)}${c.attempts > 1 ? ` after ${plural(c.attempts - 1, 'retry').replace('retrys', 'retries')}` : ''}, yet ${c.to} is in ${c.key}: ${seen.client}'s ${seen.kind} at ${ms(seen.complete)} saw it. Only that cas writes ${c.to}, so it did take effect, and a second copy of it was applied again and failed. Keep, in the state machine, each client's last seq and its reply; when an entry with that client_id and seq is applied again, skip it and answer with the saved reply.`;
        }
      }
      for (const w of hist) {
        if (twice) break;
        if (w.kind !== 'put' || !w.lateDups.length || w.complete === null) continue;
        for (const g of hist) {
          if (g.kind !== 'get' || g.key !== w.key || g.complete === null || g.out !== w.value) continue;
          const x = hist.find((o) => o.key === w.key && o.complete !== null && (o.kind === 'put' || (o.kind === 'cas' && o.ok)) && o.invoke > w.complete && o.complete < g.invoke);
          const d = x && w.lateDups.filter((t) => t > x.invoke && t < g.complete).pop();
          if (d) {
            twice = `${w.client}'s put of ${w.value} to ${w.key} (seq ${w.seq}) was acknowledged at ${ms(w.complete)}, and ${x.client}'s ${x.kind} replaced it at ${ms(x.complete)}. The old request arrived again as a delayed duplicate (last at ${ms(d)}), and the get at ${ms(g.invoke)} returned ${w.value}: it took effect a second time. A request whose seq is not above the last one applied for that client must not change the store.${diskLimit ? ' The session table is part of the state machine: it belongs in the snapshot, or a node restarted from the snapshot forgets it.' : ''}`;
            break;
          }
        }
      }
      checks.push(check('Each request takes effect once', !twice, twice));
    }

    // Linearizability, key by key.
    let bad = null;
    const byKey = new Map();
    for (const o of hist) {
      if (!byKey.has(o.key)) byKey.set(o.key, []);
      byKey.get(o.key).push(o);
    }
    for (const [key, ops] of byKey) {
      if (linearizable(ops)) continue;
      bad = explain(key, ops, culprit(ops));
      break;
    }
    const answered = hist.filter((o) => o.complete !== null).length;
    checks.push(
      check(
        'Every operation appears to take effect at one instant (linearizable)',
        !bad && answered > 0,
        bad ?? 'No operation was ever answered.',
      ),
    );

    if (logBound) {
      const res = [...finalLogs.values()].filter((r) => r && r.type === 'log_ok' && Number.isInteger(r.commit));
      const commit = Math.max(0, ...res.map((r) => r.commit));
      const maxTerm = Math.max(0, ...statuses.map((s) => s.term));
      const gets = sim.ops.filter((o) => o.req.type === 'get').length;
      const allowed = writeAttempts + maxTerm + 5;
      checks.push(check('The final log inspection completed', res.length > 0, `No node answered { type: "log" } with { type: "log_ok", entries, commit } at ${ms(sim.duration - 300)}.`));
      checks.push(
        check(
          'Gets are not written to the log',
          res.length === 0 || commit <= allowed,
          `The log holds ${commit} committed entries, but clients sent only ${writeAttempts} write requests (put and cas, counting every retry and duplicate) in ${plural(maxTerm, 'term')}, alongside ${gets} gets. A get changes nothing, so it should not cost a log entry: confirm leadership with a round of heartbeats instead (read-index).`,
        ),
      );
    }

    if (diskLimit) {
      let worst = null;
      for (const [n, m] of disk.max) if (!worst || m.size > worst.size) worst = { node: n, ...m };
      checks.push(
        check(
          `node.disk stays under ${diskLimit.toLocaleString('en-US')} bytes`,
          disk.samples > 0 && worst && worst.size <= diskLimit,
          worst
            ? `${worst.node}'s node.disk reached ${worst.size.toLocaleString('en-US')} bytes of JSON at ${ms(worst.t)}. Once applied, log entries are only needed to rebuild the state machine: save a snapshot of it (with the session table) and drop the entries it covers.`
            : 'The disks were never measured.',
        ),
      );
    }
    return checks;
  };

  // ------------------------------------------------------------ details

  function describe(o) {
    const when = o.complete === null ? `sent at ${ms(o.invoke)}, never answered` : `${ms(o.invoke)}–${ms(o.complete)}${o.node ? `, answered by ${o.node}` : ''}`;
    if (o.kind === 'get') return `${o.client}'s get of ${o.key} (${when})${o.complete === null ? '' : ` returned ${show(o.out)}`}`;
    if (o.kind === 'put') return `${o.client}'s put of ${o.value} to ${o.key} (${when})`;
    return `${o.client}'s cas on ${o.key} from ${show(o.from)} to ${o.to} (${when}${o.complete === null ? '' : `, ${o.ok ? 'cas_ok' : 'cas_failed'}`})`;
  }

  function context(c) {
    if (!c.node || c.complete === null) return '';
    const cutNow = cutOff(c.node, c.complete);
    if (cutNow) return ` ${c.node} had been cut off from the majority since ${ms(cutNow.from)}; the others had elected a new leader and moved on without it.`;
    const { before, after } = around(c.node, c.complete);
    const lead = [before, after].find((x) => x && x.role === 'leader');
    if (!lead && (before || after)) return ` ${c.node} was a ${(before ?? after).role}, not the leader, when it answered: its store can be behind.`;
    const s = stages.find((st) => st.fired && st.op && st.op.key === c.key && st.at < c.invoke && st.name.startsWith('leader crash'));
    if (s) return ` ${s.node} acknowledged ${describe(s.op).replace(/^\S+ /, 'the ')} and crashed at once. A new leader does not know that entry is committed until it commits one of its own term.`;
    if (lead) {
      const since = statuses.find((x) => x.node === c.node && x.role === 'leader' && x.term === lead.term);
      if (since && c.invoke - since.t < 1500) return ` ${c.node} had only just become leader (term ${lead.term}, first seen as leader at ${ms(since.t)}). A new leader does not know which of its entries are committed until it commits one of its own term.`;
    }
    return '';
  }

  function explain(key, ops, c) {
    const writes = ops.filter((o) => o.kind !== 'get');
    const valueOf = (o) => (o.kind === 'put' ? o.value : o.to);
    const took = (o) => o.complete !== null && (o.kind === 'put' || o.ok);
    if (!c) return `No order of the operations on ${key} explains the replies.`;
    if (c.kind === 'get') {
      const v = c.out;
      if (v !== null) {
        const w = writes.find((o) => valueOf(o) === v);
        if (!w) return `${describe(c)}, but no client ever wrote ${show(v)} to ${key}.`;
        if (w.invoke > c.complete) return `${describe(c)}, but ${show(v)} was only written later, by ${describe(w)}.`;
        const x = w.complete !== null && writes.find((o) => took(o) && o.invoke > w.complete && o.complete < c.invoke);
        if (x) {
          const d = w.lateDups.filter((t) => t > x.invoke && t < c.complete).pop();
          return `${describe(c)}. ${show(v)} was written by ${describe(w)}, but ${describe(x)} replaced it before this get was sent.${d ? ` The old put was delivered again (last at ${ms(d)}) and took effect a second time.` : ` A get must reflect every write that completed before it was sent.${context(c)}`}`;
        }
      } else {
        const x = writes.find((o) => took(o) && o.complete < c.invoke);
        if (x) return `${describe(c)}, but ${describe(x)} had completed before this get was sent.${context(c)}`;
      }
    }
    if (c.kind === 'cas' && !c.ok) {
      const seen = ops.find((o) => o.complete !== null && ((o.kind === 'get' && o.out === c.to) || (o.kind === 'cas' && o.ok && o.from === c.to)));
      if (seen) return `${describe(c)}, yet ${c.to} later appeared in ${key}: ${describe(seen)}. Only that cas writes ${c.to}, so it did take effect.`;
    }
    const around = ops
      .filter((o) => o !== c && o.invoke <= c.complete && (o.complete === null || o.complete >= c.invoke - 300))
      .slice(-4)
      .map(describe);
    return `No order of the operations on ${key} explains ${describe(c)}.${around.length ? ` Around it: ${around.join('; ')}.` : ''}${context(c)}${sessions ? ' A write applied twice (a retry or a delayed duplicate) is a common cause.' : ''}`;
  }
}

// Task 1: get, put and cas through the log; clients give up on silence.
export function kvOps(sim, params) {
  return kv(sim, { ...params, sessions: false });
}

// Task 2: client sessions; retries, duplicates and lost replies.
export function kvSessions(sim, params) {
  return kv(sim, { ...params, sessions: true });
}

// Task 3: reads that are never stale and stay off the log.
export function kvReads(sim, params) {
  return kv(sim, { ...params, sessions: true, logBound: true });
}

// Task 4: log compaction; node.disk must stay small.
export function kvSnapshots(sim, params) {
  return kv(sim, { ...params, sessions: true, diskLimit: params.diskLimit ?? 6000 });
}
