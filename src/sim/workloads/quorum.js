// Workloads for the quorum replication track.
//
// Nodes are observed only through three client requests:
//   put { key, value }  -> put_ok | error
//   get { key }         -> get_ok { value } | error
//   local_get { key }   -> local_get_ok { value }   (this replica's own copy, no quorum)

import { check, j, plural, sample, split, unanswered } from '../workloads.js';

const quorumSize = (sim) => Math.floor(sim.nodeIds.length / 2) + 1;
const ms = (t) => `${Math.round(t)}ms`;

// Wraps the simulator's fault controls so the checker knows, for every
// request, how many replicas its coordinator could reach, and records the
// history of puts and gets.
function harness(sim) {
  let groups = null;
  const down = new Set();
  const changes = [];
  const puts = [];
  const gets = [];
  const probes = [];
  const busy = new Set();
  let counter = 0;

  const h = {
    puts,
    gets,
    probes,
    busy,
    partition(g) {
      groups = g;
      changes.push(sim.now());
      sim.partition(g);
    },
    heal() {
      groups = null;
      changes.push(sim.now());
      sim.heal();
    },
    crash(id) {
      down.add(id);
      changes.push(sim.now());
      sim.crash(id);
    },
    isDown: (id) => down.has(id),
    up: () => sim.nodeIds.filter((id) => !down.has(id)),
    // How many replicas (itself included) can this node talk to right now?
    reach(id) {
      if (down.has(id)) return 0;
      const side = groups ? groups.find((g) => g.includes(id)) : sim.nodeIds;
      return side.filter((x) => !down.has(x)).length;
    },
    steady: (t0, t1) => !changes.some((t) => t >= t0 && t <= t1),
    async put(node, key) {
      const value = `${key}-v${++counter}`;
      const req = { type: 'put', key, value };
      const op = { node, key, value, req, invoke: sim.now(), reach: h.reach(node), res: null, complete: null };
      puts.push(op);
      busy.add(key);
      op.res = await sim.clientRpc(node, req);
      op.complete = sim.now();
      op.steady = h.steady(op.invoke, op.complete);
      op.ok = op.res?.type === 'put_ok';
      busy.delete(key);
      return op;
    },
    async get(node, key) {
      const req = { type: 'get', key };
      const op = { node, key, req, invoke: sim.now(), reach: h.reach(node), res: null, complete: null };
      gets.push(op);
      op.res = await sim.clientRpc(node, req);
      op.complete = sim.now();
      op.steady = h.steady(op.invoke, op.complete);
      op.ok = op.res?.type === 'get_ok';
      return op;
    },
    // Ask every live replica for its own copy of `key`.
    async probe(key) {
      const req = { type: 'local_get', key };
      const t0 = sim.now();
      const asked = await Promise.all(
        h.up().map(async (node) => ({ node, req, res: await sim.clientRpc(node, req, { probe: true }) })),
      );
      // A node that crashed while the probe was on its way cannot answer.
      const rows = asked.filter((r) => !down.has(r.node));
      probes.push(...rows);
      return { rows, steady: h.steady(t0, sim.now()), at: t0 };
    },
  };
  return h;
}

const holding = (rows, value) => rows.filter((r) => r.res && r.res.value === value).map((r) => r.node);

// Checks every task shares: requests are answered, with the right types.
function replyChecks(h) {
  // A coordinator that crashed with the request in hand cannot answer it.
  const all = [...h.puts, ...h.gets].filter((o) => o.res || !h.isDown(o.node));
  const badPut = h.puts.find((o) => o.res && o.res.type !== 'put_ok' && o.res.type !== 'error');
  const badGet = h.gets.find((o) => o.res && o.res.type !== 'get_ok' && o.res.type !== 'error');
  const bad = badPut ?? badGet;
  const badProbe = h.probes.find((p) => !p.res || p.res.type !== 'local_get_ok' || p.res.value === undefined);
  return [
    unanswered(
      'Every request gets a reply',
      all,
      'A coordinator that cannot reach enough replicas must give up and reply { type: "error" } well inside one second, not wait forever.',
    ),
    check(
      'Replies are put_ok, get_ok or error',
      !bad,
      bad && `${bad.node} replied ${j(bad.res)} to ${j(bad.req)}.`,
    ),
    check(
      'local_get returns this replica\'s own copy',
      !badProbe,
      badProbe &&
        (badProbe.res
          ? `${badProbe.node} replied ${j(badProbe.res)} to ${j(badProbe.req)}. Expected { type: "local_get_ok", value }, with value null for a key it does not hold.`
          : `${badProbe.node} never answered ${j(badProbe.req)}. local_get must answer from local state without contacting anyone.`),
    ),
  ];
}

// Requests must succeed when a quorum is reachable and be refused when not.
function quorumChecks(h, sim, { reads = true } = {}) {
  const need = quorumSize(sim);
  const n = sim.nodeIds.length;
  const ops = reads ? [...h.puts, ...h.gets] : h.puts;
  const settled = ops.filter((o) => o.res && o.steady);
  const refused = settled.find((o) => o.reach >= need && !o.ok);
  const lied = settled.find((o) => o.reach < need && o.ok);
  return [
    check(
      'Requests succeed while a quorum is reachable',
      !refused,
      refused &&
        `${refused.node} could reach ${refused.reach} of ${n} replicas (itself included) at ${ms(refused.invoke)}, and ${need} are enough, but it answered ${j(refused.req)} with ${j(refused.res)}. Do not wait for all ${n}: reply as soon as ${need} have answered. Count this node as one of them.`,
    ),
    check(
      'Requests are refused when a quorum is out of reach',
      !lied,
      lied &&
        `${lied.node} could reach only ${lied.reach} of ${n} replicas at ${ms(lied.invoke)}, yet it answered ${j(lied.req)} with ${j(lied.res)}. ${
          lied.req.type === 'put'
            ? `The write is on fewer than ${need} replicas, so a later quorum read can miss it.`
            : `The other ${n - lied.reach} replicas may hold a newer write that this side has never seen.`
        } Reply { type: "error" } instead.`,
    ),
  ];
}

// A get must never return a value older than the last put that was
// acknowledged before the get began. A value counts as older only if its
// put finished before that put started: two puts that overlap may be
// ordered either way.
function freshnessChecks(h) {
  const byKey = new Map();
  for (const p of h.puts) {
    if (!byKey.has(p.key)) byKey.set(p.key, []);
    byKey.get(p.key).push(p);
  }
  let shape = null;
  let unknown = null;
  let stale = null;
  for (const g of h.gets) {
    if (!g.ok) continue;
    const v = g.res.value;
    if (v === undefined) {
      shape = shape ?? g;
      continue;
    }
    const writes = byKey.get(g.key) ?? [];
    let last = null;
    for (const p of writes) if (p.ok && p.complete < g.invoke) last = p;
    const source = v === null ? null : writes.find((p) => p.value === v && p.invoke <= g.complete);
    if (v !== null && !source) {
      unknown = unknown ?? { g, v };
      continue;
    }
    if (!last) continue;
    if (v === null || (source.ok && source.complete < last.invoke)) stale = stale ?? { g, v, last };
  }
  return [
    check('get_ok carries a value', !shape, shape && `${shape.node} replied ${j(shape.res)} to ${j(shape.req)}. Use value: null for a key that was never written.`),
    check(
      'Reads return values that were written',
      !unknown,
      unknown && `${unknown.g.node} answered ${j(unknown.g.req)} with ${j(unknown.v)} at ${ms(unknown.g.complete)}, which no client had written to that key.`,
    ),
    check(
      'A read sees the latest acknowledged write',
      !stale,
      stale &&
        `get "${stale.g.key}" via ${stale.g.node} at ${ms(stale.g.invoke)} returned ${j(stale.v)}, but put ${j(stale.last.value)} had already been acknowledged by ${stale.last.node} at ${ms(stale.last.complete)}. That write is on W replicas; a read that hears from R replicas with W + R > N must meet at least one of them. Check that the read waits for R answers, returns the one with the highest version, and that a new write always gets a version higher than every earlier one (a coordinator that missed some writes cannot trust its own counter).`,
    ),
  ];
}

// --------------------------------------------------------- quorum writes

// Each key is written once. After each put_ok every replica is asked for
// its own copy, to see how many really hold the value.
export function quorumWrites(sim, { ops = 40, fault = null }) {
  const h = harness(sim);
  const need = quorumSize(sim);
  const n = sim.nodeIds.length;
  const end = sim.duration - 900;
  const gap = (end - 100) / ops;

  if (fault === 'partition') {
    sim.at(sim.duration * 0.3, () => h.partition(split(sim)));
    sim.at(sim.duration * 0.62, () => h.heal());
  } else if (fault === 'crash') {
    const [minority, majority] = split(sim);
    sim.at(sim.duration * 0.3, () => minority.forEach((id) => h.crash(id)));
    sim.at(sim.duration * 0.62, () => h.crash(majority[0]));
  }

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const op = await h.put(sim.pick(h.up()), `k${i + 1}`);
      if (!op.ok) return;
      const seen = await h.probe(op.key);
      if (seen.steady && op.steady) op.holders = holding(seen.rows, op.value);
    });
  }
  sim.at(sim.duration - 400, async () => {
    if (fault) return;
    for (const op of h.puts) if (op.ok) op.final = holding((await h.probe(op.key)).rows, op.value);
  });

  return () => {
    const thin = h.puts.find((o) => o.holders && o.holders.length < need);
    const partial = h.puts.find((o) => o.final && o.final.length < n);
    return [
      ...replyChecks(h),
      check(
        'An acknowledged write is on at least W replicas',
        !thin,
        thin &&
          `${thin.node} answered put_ok for "${thin.key}" at ${ms(thin.complete)}, but right afterwards only ${plural(thin.holders.length, 'replica')} held it (${thin.holders.join(', ') || 'none'}); W is ${need} of ${n}. Send the write to every replica and count the acknowledgements before answering the client.`,
      ),
      ...quorumChecks(h, sim, { reads: false }),
      ...(fault
        ? []
        : [
            check(
              'On a healthy network every replica gets every write',
              !partial,
              partial &&
                `"${partial.key}" was acknowledged at ${ms(partial.complete)}, but at the end only ${partial.final.join(', ')} held it. W decides when to answer the client, not how many replicas receive the write: send it to all ${n}.`,
            ),
          ]),
    ];
  };
}

// ---------------------------------------------------------- quorum reads

// A mix of puts and gets over a few keys. Puts on one key never overlap,
// except in the race: two puts to each key through different nodes at once.
// Right after a partition heals, requests go through the nodes that were
// cut off, which is where stale copies and stale counters live.
export function quorumReads(sim, { ops = 120, keys = 4, fault = null, race = false }) {
  const h = harness(sim);
  const end = sim.duration - 300;
  const gap = (end - 100) / ops;
  const healAt = sim.duration * 0.6;
  let cutOff = [];

  if (fault === 'partition') {
    sim.at(sim.duration * 0.25, () => {
      const groups = split(sim);
      cutOff = groups[0];
      h.partition(groups);
    });
    sim.at(healAt, () => h.heal());
  } else if (fault === 'crash') {
    sim.at(sim.duration * 0.25, () => split(sim)[0].forEach((id) => h.crash(id)));
  }

  for (let i = 0; i < ops; i++) {
    const t = 100 + i * gap;
    sim.at(t, async () => {
      const afterHeal = fault === 'partition' && t > healAt && t < healAt + 700;
      const node = sim.pick(afterHeal ? cutOff : h.up());
      const key = `k${1 + Math.floor(sim.rand() * keys)}`;
      const write = sim.rand() < 0.45 && i < ops * 0.85 && !h.busy.has(key);
      if (write) await h.put(node, key);
      else await h.get(node, key);
    });
  }

  if (race) {
    sim.at(sim.duration * 0.85, () => {
      for (let k = 1; k <= keys; k++) {
        const [a, b] = split(sim).flat();
        h.put(a, `k${k}`);
        h.put(b, `k${k}`);
      }
    });
  }

  // With no faults every store reaches every replica, so at the end they
  // must all agree on the newest value, whatever order the stores arrived in.
  let final = null;
  sim.at(sim.duration - 400, async () => {
    if (fault) return;
    const names = Array.from({ length: keys }, (_, k) => `k${k + 1}`);
    final = await Promise.all(names.map(async (key) => ({ key, rows: (await h.probe(key)).rows })));
  });

  return () => {
    let regressed = null;
    for (const f of final ?? []) {
      const writes = h.puts.filter((p) => p.key === f.key);
      if (!writes.length || writes.some((p) => !p.ok)) continue;
      const last = writes.reduce((a, p) => (p.complete > a.complete ? p : a));
      // Values a replica may end on: the last put, or any put overlapping it.
      const allowed = new Set(writes.filter((p) => p.complete >= last.invoke).map((p) => p.value));
      const values = new Set(f.rows.map((x) => (x.res ? x.res.value : undefined)));
      const behind = f.rows.filter((x) => x.res && !allowed.has(x.res.value));
      if ((behind.length || values.size > 1) && !regressed) {
        regressed = { f, last, show: behind.length ? behind : f.rows, split: !behind.length };
      }
    }
    const r = regressed;
    return [
      ...replyChecks(h),
      ...quorumChecks(h, sim),
      ...freshnessChecks(h),
      ...(fault
        ? []
        : [
            check('The final inspection ran', final !== null, 'The run ended before every replica could be asked for its copy.'),
            check(
              'On a healthy network every replica ends on the same, newest version',
              !r,
              r &&
                (r.split
                  ? `At the end the replicas disagree on "${r.f.key}": ${sample(r.show.map((x) => `${x.node} holds ${j(x.res ? x.res.value : undefined)}`), 5)}. Two puts to this key ran at the same time and nothing was lost, so every replica received both; each kept whichever arrived last. Versions must give every pair of writes one order that all replicas agree on (counter first, node id to break a tie), and a replica must keep its copy unless the incoming version is higher.`
                  : `At the end ${sample(r.show.map((x) => `${x.node} holds ${j(x.res.value)}`))} for "${r.f.key}", but the last write was ${j(r.last.value)} (acknowledged at ${ms(r.last.complete)}), and nothing was lost on the way. Messages overtake one another: the store for an older write can arrive after a newer one. A replica must compare versions and keep its copy unless the incoming version is higher.`),
            ),
          ]),
    ];
  };
}

// ----------------------------------------------------------- read repair

// Write every key, cut off a minority, overwrite every key on the majority
// side, heal, then read each key once and look at every replica afterwards.
export function readRepair(sim, { keys = 8 }) {
  const h = harness(sim);
  const names = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  const cutAt = 300 + keys * 60;
  const healAt = cutAt + 300 + keys * 100;
  const SETTLE = 1200;
  const reads = [];
  let majority = sim.nodeIds;

  names.forEach((key, i) => sim.at(100 + i * 60, () => h.put(sim.pick(sim.nodeIds), key)));
  sim.at(cutAt, () => {
    const groups = split(sim);
    majority = groups[1];
    h.partition(groups);
  });
  names.forEach((key, i) => sim.at(cutAt + 100 + i * 100, () => h.put(sim.pick(majority), key)));
  sim.at(healAt, () => h.heal());
  names.forEach((key, i) =>
    sim.at(healAt + 200 + i * 150, async () => {
      const before = await h.probe(key);
      const op = await h.get(sim.pick(sim.nodeIds), key);
      await sim.sleep(SETTLE);
      reads.push({ op, before: before.rows, after: (await h.probe(key)).rows });
    }),
  );

  return () => {
    let unrepaired = null;
    for (const r of reads) {
      const writes = h.puts.filter((p) => p.key === r.op.key);
      const newest = writes[writes.length - 1];
      if (!r.op.ok || !newest || writes.some((p) => !p.ok)) continue;
      const behind = r.after.filter((x) => !x.res || x.res.value !== newest.value);
      if (behind.length && !unrepaired) unrepaired = { r, newest, behind };
    }
    const u = unrepaired;
    return [
      ...replyChecks(h),
      ...quorumChecks(h, sim),
      ...freshnessChecks(h),
      check(
        'After a read, every replica holds the newest version',
        !u,
        u &&
          `get "${u.r.op.key}" via ${u.r.op.node} completed at ${ms(u.r.op.complete)} and returned ${j(u.r.op.res.value)}. ${SETTLE}ms later ${sample(
            u.behind.map((x) => `${x.node} holds ${j(x.res ? x.res.value : undefined)}`),
          )}; the newest write is ${j(u.newest.value)}. The stale replicas were cut off when it was written. The read heard their old versions (or would have, had it kept listening after the first R answers): send the newest value, with its version, back to every replica that returned something older.`,
      ),
    ];
  };
}

// ---------------------------------------------------------- anti-entropy

// Writes land while some replicas are unreachable. Nobody reads. Some time
// after the trouble ends every replica must hold every key's newest value.
export function antiEntropy(sim, { keys = 10, scenario = 'partition', lossy = false }) {
  const h = harness(sim);
  const names = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  const SETTLE = 2500;
  let quietAt;
  let final = null;

  names.forEach((key, i) => sim.at(100 + i * 50, () => h.put(sim.pick(sim.nodeIds), key)));
  const t0 = 300 + keys * 50;

  if (scenario === 'lossy') {
    names.forEach((key, i) => sim.at(t0 + i * 80, () => h.put(sim.pick(sim.nodeIds), key)));
    names.forEach((key, i) => sim.at(t0 + (keys + i) * 80, () => h.put(sim.pick(sim.nodeIds), key)));
    quietAt = t0 + 2 * keys * 80 + 400;
  } else {
    let majority = sim.nodeIds;
    const round = (start) => names.forEach((key, i) => sim.at(start + 100 + i * 100, () => h.put(sim.pick(majority), key)));
    const span = 300 + keys * 100;
    let first;
    sim.at(t0, () => {
      first = split(sim);
      majority = first[1];
      h.partition(first);
    });
    round(t0);
    quietAt = t0 + span;
    if (scenario === 'flapping') {
      // Straight into a second partition that cuts off part of the old majority.
      const cut = Math.floor(sim.nodeIds.length / 2);
      sim.at(quietAt, () => {
        const second = [first[1].slice(0, cut), [...first[1].slice(cut), ...first[0]]];
        majority = second[1];
        h.partition(second);
      });
      round(quietAt);
      quietAt += span;
    }
    sim.at(quietAt, () => h.heal());
  }

  sim.at(quietAt + SETTLE, async () => {
    final = await Promise.all(names.map(async (key) => ({ key, rows: (await h.probe(key)).rows })));
    names.forEach((key, i) => sim.at(sim.now() + 20 + i * 20, () => h.get(sim.pick(sim.nodeIds), key)));
  });

  return () => {
    let diverged = null;
    for (const f of final ?? []) {
      const writes = h.puts.filter((p) => p.key === f.key);
      const acked = writes.filter((p) => p.ok);
      const newest = acked[acked.length - 1];
      if (!newest) continue;
      // A put that failed may still have landed on some replicas, with any version.
      const allowed = new Set([newest.value, ...writes.filter((p) => !p.ok).map((p) => p.value)]);
      const values = new Set(f.rows.map((x) => (x.res ? x.res.value : undefined)));
      const behind = f.rows.filter((x) => !x.res || !allowed.has(x.res.value));
      if ((behind.length || values.size > 1) && !diverged) {
        diverged = { f, newest, show: behind.length ? behind : f.rows };
      }
    }
    const d = diverged;
    return [
      ...replyChecks(h),
      ...(lossy ? [] : quorumChecks(h, sim)),
      ...freshnessChecks(h),
      check('The convergence check ran', final !== null, 'The run ended before the replicas could be inspected.'),
      check(
        'Every replica ends up with the newest value of every key',
        !d,
        d &&
          `${SETTLE}ms after ${scenario === 'lossy' ? 'the last write' : 'the network healed'}, replicas still disagree on "${d.f.key}": ${sample(
            d.show.map((x) => `${x.node} holds ${j(x.res ? x.res.value : undefined)}`),
            5,
          )}. The newest acknowledged write is ${j(d.newest.value)} (via ${d.newest.node} at ${ms(d.newest.complete)}). Nobody read this key, so read repair never ran. Replicas have to find the difference themselves: compare versions with a peer every few hundred milliseconds, or have the coordinator keep a hint and retry the missed replica until it answers.`,
      ),
    ];
  };
}
