// Workloads for the load-balancing track.
//
// In every task the learner's node n1 is a load balancer. The harness runs a
// pool of fake backends as services (b1, b2, ...), tells the node which of
// them make up the pool with a `set_backends` message, and sends client `get`
// requests that the node must forward to one backend and relay back. Every
// backend records each request and health check it receives; those records
// are how the checkers measure what each backend was asked to do.

import { check, j, plural, unanswered } from '../workloads.js';

const LB = 'n1';
const DEADLINE = 1000; // ms a client waits for its reply
const ms = (t) => `${Math.round(t)}ms`;
const pct = (x) => `${Math.round(x * 100)}%`;
const list = (ids) => '[' + ids.join(', ') + ']';
const isOk = (r) => !!r.res && r.res.type === 'get_ok';

// ------------------------------------------------------------- the pool

// Registers the backends and watches the traffic around them.
//
// latency(id, t, key, hit) -> ms a working backend takes to answer a get
// status(id, t)            -> 'up' | 'dead' (answers nothing) | 'broken' (answers every message with an error at once)
//
// Each get a node sends to a backend is recorded as
// { backend, key, sent, s, arrived, outcome, heard }: when the node sent it
// (`s` is the global send order, used to tell which pool the node was using),
// when it reached the backend, what the backend did, and when the answer
// reached the node.
function makePool(sim, ids, { latency, status = () => 'up' }) {
  const secret = 's' + Math.floor(sim.rand() * 1e6);
  const pool = {
    ids,
    calls: [],
    probes: new Map(ids.map((id) => [id, []])), // arrival times of health checks
    sets: [], // set_backends requests: { list, at, msgId, ack: { t, s } | null, res }
    served: new Map(), // key -> Set of backends that received a get for it
    value: (key, id) => key + '@' + id + '#' + secret,
    callsTo: (id) => pool.calls.filter((c) => c.backend === id),
  };
  const byMsg = new Map();
  const isBackend = new Set(ids);
  const isNode = new Set(sim.nodeIds);
  const caches = new Map(ids.map((id) => [id, new Set()]));

  const record = (backend, body, sent, s) => {
    const call = { backend, key: body.key, sent, s, arrived: null, outcome: null, heard: null };
    pool.calls.push(call);
    if (!pool.served.has(call.key)) pool.served.set(call.key, new Set());
    pool.served.get(call.key).add(backend);
    if (body.msg_id != null) byMsg.set(body.msg_id, call);
    return call;
  };

  sim.onSend = (msg) => {
    if (isBackend.has(msg.dest) && isNode.has(msg.src) && msg.body.type === 'get') {
      record(msg.dest, msg.body, msg.t0, msg.s0);
    } else if (isNode.has(msg.src) && msg.kind === 'client' && msg.body.in_reply_to != null) {
      const set = pool.sets.find((x) => x.msgId === msg.body.in_reply_to);
      if (set && !set.ack && msg.body.type === 'set_backends_ok') set.ack = { t: msg.t0, s: msg.s0 };
    }
  };
  sim.onDeliver = (msg) => {
    if (isBackend.has(msg.src)) {
      const call = byMsg.get(msg.body.in_reply_to);
      if (call && call.heard === null) call.heard = sim.now();
    }
  };

  for (const id of ids) {
    sim.service(id, (msg, reply) => {
      const t = sim.now();
      const st = status(id, t);
      const type = msg.body.type;
      if (type === 'health') {
        pool.probes.get(id).push(t);
        if (st === 'dead') return;
        if (st === 'broken') return reply({ type: 'error', code: 'unavailable' });
        sim.at(t + 1 + sim.rand() * 15, () => {
          if (status(id, sim.now()) === 'up') reply({ type: 'health_ok' });
        });
        return;
      }
      if (type !== 'get') {
        reply({ type: 'error', code: 'bad_request', text: 'a backend understands { type: "get", key } and { type: "health" }' });
        return;
      }
      // A get sent with node.send has no msg_id: match it to the oldest call of that key still on its way here.
      const call =
        (msg.body.msg_id != null && byMsg.get(msg.body.msg_id)) ||
        pool.calls.find((c) => c.backend === id && c.key === msg.body.key && c.arrived === null) ||
        record(id, msg.body, t, Infinity);
      call.arrived = t;
      if (st === 'dead') {
        call.outcome = 'dead';
        return;
      }
      if (st === 'broken') {
        call.outcome = 'error';
        reply({ type: 'error', code: 'unavailable' });
        return;
      }
      const cache = caches.get(id);
      const hit = cache.has(call.key);
      cache.add(call.key);
      call.miss = !hit;
      call.outcome = 'ok';
      sim.at(t + Math.max(1, latency(id, t, call.key, hit)), () => {
        // A backend that dies while working on a request never answers it.
        if (status(id, sim.now()) !== 'up') {
          call.outcome = 'lost';
          return;
        }
        reply({ type: 'get_ok', key: call.key, value: pool.value(call.key, id), backend: id });
      });
    });
  }
  return pool;
}

// Tell the node which backends make up the pool, at time `at`.
function setBackends(sim, pool, at, backends) {
  const set = { list: backends, at, msgId: null, ack: null, res: undefined };
  pool.sets.push(set);
  sim.at(at, () => {
    sim.clientRpc(LB, { type: 'set_backends', backends: [...backends] }, { timeout: 500, client: 'c9' }).then((res) => (set.res = res));
    const op = sim.ops[sim.ops.length - 1];
    set.msgId = op && op.msg ? op.msg.body.msg_id : null;
    set.s = op && op.msg ? op.msg.s0 : Infinity;
  });
  return set;
}

// The pool the node was using for a call: the last set_backends acknowledged before it was sent.
function phaseOf(pool, call) {
  let k = 0;
  pool.sets.forEach((set, i) => {
    const boundary = set.ack ? set.ack.s : set.s ?? Infinity;
    if (boundary < call.s) k = i;
  });
  return k;
}

// One client request. The record is pushed at once and filled in when (if) the reply arrives.
function ask(sim, results, key, client) {
  const r = { node: LB, key, req: { type: 'get', key }, res: null, invoked: sim.now(), done: null };
  results.push(r);
  sim.clientRpc(LB, r.req, { timeout: DEADLINE, client }).then((res) => {
    r.res = res;
    r.done = sim.now();
  });
  return r;
}

// Client traffic with unique keys r1, r2, ... Arrivals are `rps` per second on
// average from `from` to `to`, with random gaps (between 0 and twice the
// mean) so that requests sometimes bunch up; or `bursts` of `count` requests
// arriving at the same instant every `every` ms.
function traffic(sim, results, { from = 200, to, rps = 0, bursts = null }) {
  const plan = [];
  let n = 0;
  if (rps) {
    for (let t = from; t < to; t += (2 * sim.rand() * 1000) / rps) plan.push({ t, key: 'r' + ++n });
  }
  if (bursts) {
    let b = 0;
    for (let t = from; t < to; t += bursts.every) {
      b++;
      for (let i = 0; i < bursts.count; i++) plan.push({ t, key: 'r' + ++n, burst: b });
    }
  }
  plan.forEach((p, i) => sim.at(p.t, () => (ask(sim, results, p.key, 'c' + (1 + (i % 3))).burst = p.burst)));
  return plan;
}

const range = (r) => (sim) => r[0] + sim.rand() * (r[1] - r[0]);

// --------------------------------------------------------- common checks

function acknowledged(pool) {
  const bad = pool.sets.find((s) => !s.res || s.res.type !== 'set_backends_ok');
  return check(
    'Every set_backends is acknowledged',
    !bad,
    bad &&
      `At ${ms(bad.at)} the harness sent { type: "set_backends", backends: ${j(bad.list)} } and got ${bad.res ? j(bad.res) : 'no reply'}. Store the list and reply { type: "set_backends_ok" }.`,
  );
}

function everyReply(results) {
  return unanswered(
    'Every request gets a reply',
    results,
    `The client gives up after ${DEADLINE}ms. Forward the request with node.rpc(backend, { type: "get", key }) and pass the backend's answer back with node.reply(msg, res).`,
  );
}

function rightAnswers(results, pool) {
  const bad = results.find((r) => {
    if (!isOk(r)) return false;
    const from = pool.served.get(r.key);
    return !from || ![...from].some((id) => r.res.value === pool.value(r.key, id));
  });
  const from = bad && pool.served.get(bad.key);
  return check(
    "Replies are the backends' answers",
    !bad,
    bad &&
      `The client asked for ${j(bad.key)} and got ${j(bad.res)}, but ${from ? `the value from ${[...from].join(' or ')} was ${j(pool.value(bad.key, [...from][0]))}` : 'no backend was ever asked for that key'}. ${from ? 'Several requests are in flight at once: reply to each client with the answer to its own request (awaiting node.rpc inside the handler keeps them paired).' : 'Every reply must come from a backend.'}`,
  );
}

// Keys are unique per request, so the calls for a key are the calls made for that request.
function callsPerRequest(results, pool, max) {
  const count = new Map();
  for (const c of pool.calls) count.set(c.key, (count.get(c.key) ?? 0) + 1);
  const none = results.find((r) => r.res && !count.get(r.key));
  const many = results.find((r) => (count.get(r.key) ?? 0) > max);
  const name = max === 1 ? 'Each request goes to exactly one backend' : `At most ${max} backend calls per request`;
  return check(
    name,
    !none && !many,
    none
      ? `The client got ${j(none.res)} for ${j(none.key)}, but no backend was asked. Forward every request.`
      : many &&
          `${j(many.key)} was sent to ${pool.calls
            .filter((c) => c.key === many.key)
            .map((c) => `${c.backend} at ${ms(c.sent)}`)
            .join(', ')}. ${max === 1 ? 'Send each request to one backend only.' : 'Try once, and on a failure retry once on another backend; never more.'}`,
  );
}

function onlyMembers(pool) {
  const bad = pool.calls.find((c) => {
    const set = pool.sets[phaseOf(pool, c)];
    return set && !set.list.includes(c.backend);
  });
  const set = bad && pool.sets[phaseOf(pool, bad)];
  return check(
    'Only backends in the pool get requests',
    !bad,
    bad &&
      `At ${ms(bad.sent)} ${j(bad.key)} was sent to ${bad.backend}, but the pool was ${list(set.list)} (set at ${ms(set.at)}). Use the list from the latest set_backends.`,
  );
}

// Calls sent in [a, b): count per backend and in total.
function counts(pool, a, b, filter = () => true) {
  const per = new Map();
  let total = 0;
  for (const c of pool.calls) {
    if (c.sent < a || c.sent >= b || !filter(c)) continue;
    per.set(c.backend, (per.get(c.backend) ?? 0) + 1);
    total++;
  }
  return { per, total, of: (id) => per.get(id) ?? 0 };
}

const percentile = (values, p) => {
  if (!values.length) return 0;
  const s = [...values].sort((x, y) => x - y);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))];
};

// ---------------------------------------------------------- round robin

export function roundRobin(sim, { phases, latency = {}, rps = 0, bursts = null }) {
  const ids = [...new Set(phases.flatMap((p) => p.backends))].sort();
  const pool = makePool(sim, ids, { latency: (id) => range(latency[id] ?? [5, 40])(sim) });
  phases.forEach((p) => setBackends(sim, pool, p.at, p.backends));
  const results = [];
  traffic(sim, results, { to: sim.duration - 700, rps, bursts });

  return () => {
    const checks = [acknowledged(pool), everyReply(results), rightAnswers(results, pool), callsPerRequest(results, pool, 1), onlyMembers(pool)];
    let uneven = null;
    pool.sets.forEach((set, k) => {
      if (uneven) return;
      const mine = pool.calls.filter((c) => phaseOf(pool, c) === k);
      const per = set.list.map((id) => ({ id, n: mine.filter((c) => c.backend === id).length }));
      const hi = Math.max(...per.map((x) => x.n));
      const lo = Math.min(...per.map((x) => x.n));
      if (mine.length >= set.list.length * 3 && hi - lo > 2) uneven = { set, per, total: mine.length };
    });
    checks.push(
      check(
        'Requests are spread evenly',
        !uneven,
        uneven &&
          `While the pool was ${list(uneven.set.list)} (from ${ms(uneven.set.ack ? uneven.set.ack.t : uneven.set.at)}), the ${uneven.total} requests went ${uneven.per.map((x) => `${x.id}: ${x.n}`).join(', ')}. Counts may differ by at most 2. Round robin gives the backends turns in order: ${uneven.set.list.slice(0, 3).join(', ')}, ${uneven.set.list[0]}, … Keep a counter, and use it modulo the current pool size.`,
      ),
    );
    return checks;
  };
}

// --------------------------------------------- least outstanding requests

const SLOW_SHARE = 0.12;
const FAST_P90 = 150;
const RECOVERED_SHARE = 0.15;
const BURST_MAX = 5;

// slow: [{ id, from, to, range }] a backend that is slow during [from, to).
export function leastOutstanding(sim, { backends, slow = [], rps = 0, bursts = null, base = [10, 40] }) {
  const slowAt = (id, t) => slow.find((s) => s.id === id && t >= s.from && t < s.to);
  const pool = makePool(sim, backends, {
    latency: (id, t) => {
      const s = slowAt(id, t);
      return range(s ? s.range : base)(sim);
    },
  });
  setBackends(sim, pool, 0, backends);
  const results = [];
  const end = sim.duration - 700;
  traffic(sim, results, { to: end, rps, bursts });

  return () => {
    const checks = [acknowledged(pool), everyReply(results), rightAnswers(results, pool), callsPerRequest(results, pool, 1), onlyMembers(pool)];
    for (const s of slow) {
      const a = Math.max(200, s.from + 200);
      const b = Math.min(s.to, end);
      const w = counts(pool, a, b);
      const share = w.total ? w.of(s.id) / w.total : 0;
      checks.push(
        check(
          `The slow backend gets little traffic (${s.id}, ${ms(a)}–${ms(b)})`,
          share <= SLOW_SHARE,
          `From ${ms(a)} to ${ms(b)} ${s.id} took ${s.range[0]}–${s.range[1]}ms per request while the others took ${base[0]}–${base[1]}ms, yet it received ${w.of(s.id)} of ${w.total} requests (${pct(share)}); at most ${pct(SLOW_SHARE)} may go to it. Round robin, like a random pick, gives every backend an equal share whatever its speed. Count the requests in flight to each backend and send each new one to the backend with the fewest.`,
        ),
      );
      const during = results.filter((r) => r.invoked >= a && r.invoked < b && r.res);
      const lat = during.map((r) => r.done - r.invoked);
      const p90 = percentile(lat, 0.9);
      checks.push(
        check(
          `90% of requests are answered within ${FAST_P90}ms (${ms(a)}–${ms(b)})`,
          p90 <= FAST_P90,
          `Of the ${during.length} requests sent between ${ms(a)} and ${ms(b)}, the slowest 10% took ${ms(p90)} or more. A request that waits behind the slow backend is a request that could have gone to a fast one.`,
        ),
      );
      if (s.to < end - 800) {
        const after = counts(pool, s.to + 500, end);
        const back = after.total ? after.of(s.id) / after.total : 0;
        checks.push(
          check(
            `${s.id} gets traffic again once it is fast`,
            back >= RECOVERED_SHARE,
            `${s.id} was fast again from ${ms(s.to)}, but from ${ms(s.to + 500)} to ${ms(end)} it received ${after.of(s.id)} of ${after.total} requests (${pct(back)}); at least ${pct(RECOVERED_SHARE)} are expected. Base every choice on the requests in flight right now, not on a backend's past.`,
          ),
        );
      }
    }
    if (bursts) {
      let worst = null;
      const byKey = new Map(pool.calls.map((c) => [c.key, c.backend]));
      const groups = new Map();
      for (const r of results) {
        if (!r.burst) continue;
        const id = byKey.get(r.key);
        if (!id) continue;
        const g = groups.get(r.burst) ?? new Map();
        g.set(id, (g.get(id) ?? 0) + 1);
        groups.set(r.burst, g);
      }
      for (const [b, g] of groups) {
        for (const [id, n] of g) if (!worst || n > worst.n) worst = { b, id, n, at: results.find((r) => r.burst === b).invoked };
      }
      checks.push(
        check(
          `A burst is spread out (at most ${BURST_MAX} of ${bursts.count} to one backend)`,
          !worst || worst.n <= BURST_MAX,
          worst &&
            `${bursts.count} requests arrived together at ${ms(worst.at)} and ${worst.n} of them went to ${worst.id}. Count a request as in flight the moment you send it, not when it comes back: the next request in the same burst must already see it.`,
        ),
      );
    }
    return checks;
  };
}

// ------------------------------------------------------ consistent hashing

const BALANCE = [0.6, 1.5];

// phases: [{ at, backends }]. Every key is requested `perPhase` times in each phase.
export function stickyRouting(sim, { phases, keys = 300, perPhase = 2 }) {
  const ids = [...new Set(phases.flatMap((p) => p.backends))].sort();
  // A backend keeps a cache of the keys it has served: a hit is fast, a miss is slow.
  const pool = makePool(sim, ids, { latency: (id, t, key, hit) => (hit ? 3 + sim.rand() * 7 : 40 + sim.rand() * 30) });
  phases.forEach((p) => setBackends(sim, pool, p.at, p.backends));
  const keyNames = Array.from({ length: keys }, (_, i) => 'user:' + (1000 + i));
  const results = [];
  phases.forEach((p, k) => {
    const from = p.at + 150;
    const to = (k + 1 < phases.length ? phases[k + 1].at : sim.duration - 500) - 150;
    const todo = [];
    for (let i = 0; i < perPhase; i++) todo.push(...keyNames);
    for (let i = todo.length - 1; i > 0; i--) {
      const x = Math.floor(sim.rand() * (i + 1));
      [todo[i], todo[x]] = [todo[x], todo[i]];
    }
    const gap = (to - from) / todo.length;
    todo.forEach((key, i) => sim.at(from + i * gap, () => (ask(sim, results, key, 'c' + (1 + (i % 3))).phase = k)));
  });

  return () => {
    const checks = [acknowledged(pool), everyReply(results), rightAnswers(results, pool)];
    // Each request goes to one backend: calls per key and phase never exceed requests per key and phase.
    const extra = results.length < pool.calls.length;
    checks.push(
      check(
        'Each request goes to exactly one backend',
        !extra,
        extra && `${results.length} client requests caused ${pool.calls.length} backend calls. Send each request to one backend only.`,
      ),
      onlyMembers(pool),
    );

    // owners[k]: key -> backend, from the calls of phase k.
    const owners = pool.sets.map(() => new Map());
    let split = null;
    const wandered = new Set(); // phase + key of every key sent to more than one backend in one phase
    for (const c of pool.calls) {
      const k = phaseOf(pool, c);
      const m = owners[k];
      if (m.has(c.key) && m.get(c.key).backend !== c.backend) {
        wandered.add(k + ' ' + c.key);
        if (!split) split = { k, key: c.key, first: m.get(c.key), second: c };
      }
      if (!m.has(c.key)) m.set(c.key, c);
    }
    const misses = pool.calls.filter((c) => c.outcome === 'ok' && c.miss).length;
    checks.push(
      check(
        'A key always goes to the same backend',
        !split,
        split &&
          `While the pool was ${list(pool.sets[split.k].list)}, ${j(split.key)} went to ${split.first.backend} at ${ms(split.first.sent)} and to ${split.second.backend} at ${ms(split.second.sent)} (${plural(wandered.size, 'key')} wandered like that; ${misses} of ${pool.calls.length} requests missed a backend's cache). Each backend caches the keys it has served, so a key that wanders misses the cache every time it lands somewhere new. Choose the backend from a hash of the key, not from load or turn order.`,
      ),
    );

    // Balance: keys per backend in every phase.
    let skew = null;
    owners.forEach((m, k) => {
      if (skew || !m.size) return;
      const members = pool.sets[k].list;
      const per = new Map(members.map((id) => [id, 0]));
      for (const c of m.values()) if (per.has(c.backend)) per.set(c.backend, per.get(c.backend) + 1);
      const fair = m.size / members.length;
      for (const [id, n] of per) {
        if (n < BALANCE[0] * fair || n > BALANCE[1] * fair) {
          skew = { k, id, n, fair, per };
          break;
        }
      }
    });
    checks.push(
      check(
        `Keys are shared fairly (${BALANCE[0]}× to ${BALANCE[1]}× an equal share)`,
        !skew,
        skew &&
          `With the pool ${list(pool.sets[skew.k].list)}, the ${owners[skew.k].size} keys went ${[...skew.per].map(([id, n]) => `${id}: ${n}`).join(', ')}. An equal share is ${Math.round(skew.fair)}; ${skew.id} has ${skew.n}. ${
            Math.max(...skew.per.values()) >= 0.9 * owners[skew.k].size
              ? 'When one backend wins nearly every key, the hash does not mix: "user:1/b1" and "user:1/b5" differ only in the last character, and a simple multiply-and-add hash keeps them in the same order for every key. Use the hash from the starter, which ends with a mixing step.'
              : 'With one hash point per backend the arcs of a ring come out very uneven: score every backend per key (rendezvous hashing), or give each backend about 100 points on the ring.'
          }`,
      ),
    );

    // Movement across each change.
    for (let k = 1; k < owners.length; k++) {
      const before = pool.sets[k - 1].list;
      const after = pool.sets[k].list;
      let moved = 0;
      let bad = null;
      let badCount = 0;
      for (const [key, c] of owners[k]) {
        const old = owners[k - 1].get(key);
        if (!old || old.backend === c.backend) continue;
        moved++;
        if (before.includes(c.backend) && after.includes(old.backend)) {
          badCount++;
          bad = bad ?? { key, from: old.backend, to: c.backend };
        }
      }
      const gone = before.filter((id) => !after.includes(id));
      const added = after.filter((id) => !before.includes(id));
      const what = [gone.length ? `${gone.join(', ')} left` : '', added.length ? `${added.join(', ')} joined` : ''].filter(Boolean).join(' and ') || 'the list was reordered';
      checks.push(
        check(
          `Only the keys that must move do (change at ${ms(pool.sets[k].at)}: ${what})`,
          !bad,
          bad &&
            `The pool went from ${list(before)} to ${list(after)}. ${j(bad.key)} moved from ${bad.from} to ${bad.to}, although both are in both pools (${badCount} of ${owners[k].size} keys moved like that; ${moved} moved in all). Every moved key is a cache miss on its new backend. With hash(key) % pool.length almost every key moves when the length changes; with a score per (key, backend) pair, or a ring, only the keys of a backend that left, or the keys a new backend wins, change hands.${what === 'the list was reordered' || after.length === before.length ? ' The order of the list must not matter either.' : ''}`,
        ),
      );
    }

    // Same set of backends => same mapping, whatever the order and history.
    let differ = null;
    for (let a = 0; a < owners.length && !differ; a++) {
      for (let b = a + 1; b < owners.length && !differ; b++) {
        const sa = [...pool.sets[a].list].sort().join();
        const sb = [...pool.sets[b].list].sort().join();
        if (sa !== sb) continue;
        for (const [key, c] of owners[b]) {
          const o = owners[a].get(key);
          if (o && o.backend !== c.backend) {
            differ = { a, b, key, from: o.backend, to: c.backend };
            break;
          }
        }
      }
    }
    if (owners.length > 1) {
      checks.push(
        check(
          'The same backends give the same mapping',
          !differ,
          differ &&
            `The pool was ${list(pool.sets[differ.a].list)} at ${ms(pool.sets[differ.a].at)} and ${list(pool.sets[differ.b].list)} at ${ms(pool.sets[differ.b].at)}: the same backends, yet ${j(differ.key)} went to ${differ.from} the first time and ${differ.to} the second. Derive everything from the key and the backend ids, never from positions in the list or from history.`,
        ),
      );
    }
    return checks;
  };
}

// ---------------------------------------------- health checks, slow start

const DETECT = 1000; // stop routing to a failed backend within this long
const RETURN = 1000; // route to a recovered backend again within this long
const PROBE_RATE = 10; // health checks per second per backend, at most
const HEALTHY_SHARE = 1 / 3; // of an equal share, for backends in service
// Slow start, in ms after a backend could first be known healthy, as multiples of an equal share:
const RAMP_EARLY = [0, 1500]; // at most WARM_MAX here
const WARM_MAX = 0.6;
const RAMP_MID = [1500, 3000]; // at least MID_MIN here (the ramp is under way)
const MID_MIN = 0.25;
const RAMP_LATE = [3500, 5500]; // at least FULL_MIN here (the ramp is over)
const FULL_MIN = 0.5;
const SLOW_RETURN = 2000;

// outages: [{ id, from, to, kind: 'dead' | 'broken' }]
// joins:   [{ id, at }] backends added to the pool later (slow-start task)
function serviceWorkload(sim, { backends, outages = [], joins = [], rps = 60, slowStart = false }) {
  const all = [...backends, ...joins.map((x) => x.id)];
  const statusOf = (id, t) => {
    const o = outages.find((x) => x.id === id && t >= x.from && t < x.to);
    return o ? o.kind : 'up';
  };
  const pool = makePool(sim, all, { latency: () => 5 + sim.rand() * 75, status: statusOf });
  setBackends(sim, pool, 0, backends);
  let members = [...backends];
  for (const x of joins) {
    members = [...members, x.id];
    setBackends(sim, pool, x.at, members);
  }
  const results = [];
  const end = sim.duration - 300 - DEADLINE;
  traffic(sim, results, { to: end, rps });

  return () => {
    const checks = [acknowledged(pool)];
    const failed = results.filter((r) => !isOk(r));
    const f = failed[0];
    const story = (r) =>
      pool.calls
        .filter((c) => c.key === r.key)
        .map((c) => `${c.backend} at ${ms(c.sent)} (${c.outcome === 'ok' ? 'answered' : c.outcome === 'error' ? 'answered with an error' : 'never answered'})`)
        .join(', ') || 'no backend';
    checks.push(
      check(
        `Every request succeeds within ${DEADLINE}ms`,
        !f,
        f &&
          `${plural(failed.length, 'request')} of ${results.length} did not get a get_ok in time. First: ${j(f.key)} sent at ${ms(f.invoked)} got ${f.res ? j(f.res) : 'no reply'}; it went to ${story(f)}. At least three backends were healthy throughout, so a request whose backend times out or answers with an error should be retried once on another backend.`,
      ),
      rightAnswers(results, pool),
      callsPerRequest(results, pool, 2),
    );

    for (const o of outages) {
      const late = pool.calls.filter((c) => c.backend === o.id && c.sent >= o.from + DETECT && c.sent < o.to);
      const probed = pool.probes.get(o.id).filter((t) => t >= o.from && t < o.to).length;
      const how = o.kind === 'dead' ? 'answered nothing at all' : 'answered every message at once with { type: "error" }';
      checks.push(
        check(
          `${o.id} is taken out of service within ${DETECT}ms (down ${ms(o.from)}–${ms(o.to)})`,
          late.length === 0,
          late.length &&
            `${o.id} ${how} from ${ms(o.from)}, yet ${plural(late.length, 'request')} went to it after ${ms(o.from + DETECT)}, the first at ${ms(late[0].sent)}. It received ${plural(probed, 'health check')} while it was down. ${o.kind === 'broken' ? 'An error answer comes back fast, so a balancer that counts requests in flight thinks a broken backend is the least busy one and sends it more. ' : ''}Send { type: "health" } to every backend every 200ms or so and stop routing to one after two failed checks in a row.`,
        ),
      );
      const limit = slowStart ? SLOW_RETURN : RETURN;
      const back = pool.calls.find((c) => c.backend === o.id && c.sent >= o.to);
      if (o.to + limit < end) {
        checks.push(
          check(
            `${o.id} is back in service within ${limit}ms of recovering`,
            back && back.sent <= o.to + limit,
            `${o.id} answered health checks again from ${ms(o.to)}, but ${back ? `its next request came only at ${ms(back.sent)}` : 'it never received another request'}. Keep probing backends you have taken out, and put one back as soon as a probe succeeds.`,
          ),
        );
      }
    }

    // Windows in which the set of backends in service is settled: each one should carry its weight.
    const events = [
      ...outages.flatMap((o) => [
        { t: o.from, settle: DETECT },
        { t: o.to, settle: slowStart ? RAMP_LATE[0] : RETURN },
      ]),
      ...joins.map((x) => ({ t: x.at, settle: slowStart ? RAMP_LATE[0] : RETURN })),
    ].sort((a, b) => a.t - b.t);
    const windows = [];
    let from = 300;
    for (const e of events) {
      windows.push([from, e.t]);
      from = Math.max(from, e.t + e.settle);
    }
    windows.push([from, end]);
    let starved = null;
    for (const [a, b] of windows) {
      if (b - a < 500 || starved) continue;
      const mid = (a + b) / 2;
      const inService = all.filter((id) => {
        const joined = joins.find((x) => x.id === id);
        return (!joined || joined.at <= a) && outages.every((o) => o.id !== id || b <= o.from || a >= o.to);
      });
      const w = counts(pool, a, b);
      const fair = w.total / inService.length;
      for (const id of inService) {
        if (w.of(id) < HEALTHY_SHARE * fair) {
          starved = { id, a, b, n: w.of(id), total: w.total, fair, inService, mid };
          break;
        }
      }
    }
    checks.push(
      check(
        'Healthy backends keep their share',
        !starved,
        starved &&
          `From ${ms(starved.a)} to ${ms(starved.b)} ${list(starved.inService)} were all healthy, but ${starved.id} received only ${starved.n} of ${starved.total} requests (an equal share is ${Math.round(starved.fair)}; at least a third of that is expected). Either the node took it out by mistake (a healthy backend can take about 40ms to answer a health check, so a much shorter probe timeout, or acting on a single failed probe, removes healthy backends), or it stopped choosing it: a request that timed out but was never subtracted from the in-flight count makes a backend look busy for ever.`,
      ),
    );

    const greedy = all.map((id) => ({ id, n: pool.probes.get(id).length })).sort((a, b) => b.n - a.n)[0];
    const allowed = Math.ceil((PROBE_RATE * sim.duration) / 1000) + 5;
    checks.push(
      check(
        `At most ${PROBE_RATE} health checks per second per backend`,
        greedy.n <= allowed,
        `${greedy.id} received ${greedy.n} health checks in ${ms(sim.duration)}; at most ${allowed} are allowed. Probe on a timer (every 200ms is plenty), not once per request.`,
      ),
    );

    if (slowStart) {
      // The anchor is the earliest moment the node could know the backend is usable:
      // the first health check it answers after recovering, or the set_backends that adds it.
      const inPool = (id, a) => all.filter((x) => x !== id && statusOf(x, a) === 'up' && !joins.some((jn) => jn.id === x && jn.at > a));
      const warmups = [
        ...outages.map((o) => {
          const probe = pool.probes.get(o.id).find((t) => t >= o.to);
          const first = pool.calls.find((c) => c.backend === o.id && c.sent >= o.to);
          const anchor = Math.min(probe ?? Infinity, first ? first.sent : Infinity);
          return { id: o.id, anchor: anchor === Infinity ? o.to : anchor, joined: false, why: `recovered at ${ms(o.to)}` };
        }),
        ...joins.map((x) => {
          const set = pool.sets.find((s) => s.at === x.at);
          return { id: x.id, anchor: set && set.ack ? set.ack.t : x.at, joined: true, why: `joined the pool at ${ms(x.at)}` };
        }),
      ];
      for (const w of warmups) {
        const n = inPool(w.id, w.anchor).length + 1;
        // The backend's share of calls sent in [a, b), as a multiple of an equal share.
        const share = (a, b) => {
          const c = counts(pool, a, b);
          return { n: c.of(w.id), total: c.total, x: c.total ? (c.of(w.id) * n) / c.total : 0 };
        };
        const [ea, eb] = [w.anchor + RAMP_EARLY[0], w.anchor + RAMP_EARLY[1]];
        const early = share(ea, eb);
        checks.push(
          check(
            `${w.id} warms up gently (${ms(ea)}–${ms(eb)})`,
            early.x <= WARM_MAX,
            `${w.id} ${w.why} and could first be known healthy at ${ms(w.anchor)}. In the next ${RAMP_EARLY[1] / 1000}s it received ${early.n} of ${early.total} requests: ${early.x.toFixed(2)} of an equal share, where at most ${WARM_MAX} is allowed. A backend that has just started is idle and answers fast, so least-outstanding routing sends it a full share at once. Give it a weight that grows from 0 to 1 over 3 seconds and send it that fraction of a share.`,
          ),
        );
        const [ma, mb] = [w.anchor + RAMP_MID[0], w.anchor + RAMP_MID[1]];
        const mid = share(ma, mb);
        checks.push(
          check(
            `${w.id} ramps up rather than waiting (${ms(ma)}–${ms(mb)})`,
            mid.x >= MID_MIN,
            `${w.id} ${w.why}. Between ${RAMP_MID[0] / 1000}s and ${RAMP_MID[1] / 1000}s later its weight should be between 0.5 and 1, but it received ${mid.n} of ${mid.total} requests: ${mid.x.toFixed(2)} of an equal share, where at least ${MID_MIN} is expected. Holding a backend back and then giving it everything at once is the same jolt, only later: let its share grow step by step.`,
          ),
        );
        const [la, lb] = [w.anchor + RAMP_LATE[0], Math.min(w.anchor + RAMP_LATE[1], end)];
        if (lb - la >= 500) {
          const late = share(la, lb);
          checks.push(
            check(
              `${w.id} reaches a full share (${ms(la)}–${ms(lb)})`,
              late.x >= FULL_MIN,
              `${w.id} ${w.why}. ${RAMP_LATE[0] / 1000}s later its ramp should be over, but from ${ms(la)} to ${ms(lb)} it received ${late.n} of ${late.total} requests: ${late.x.toFixed(2)} of an equal share, where at least ${FULL_MIN} is expected. The weight should reach 1 three seconds after the backend comes back, and stay there: set the start time when the backend comes back, not on every successful health check.`,
            ),
          );
        }
        if (w.joined) {
          const first = pool.calls.find((c) => c.backend === w.id && c.sent >= w.anchor);
          checks.push(
            check(
              `${w.id} is in service within ${SLOW_RETURN}ms of joining`,
              first && first.sent <= w.anchor + SLOW_RETURN,
              `${w.id} joined the pool at ${ms(w.anchor)} but ${first ? `received its first request only at ${ms(first.sent)}` : 'never received a request'}. Start its ramp as soon as it joins.`,
            ),
          );
        }
      }
    }
    return checks;
  };
}

export function healthChecks(sim, params) {
  return serviceWorkload(sim, { ...params, slowStart: false });
}

export function slowStart(sim, params) {
  return serviceWorkload(sim, { ...params, slowStart: true });
}
