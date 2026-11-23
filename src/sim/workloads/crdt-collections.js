// Workloads for the "Sets, registers and maps" track (state-based CRDTs).
//
// Client requests, per task:
//   G-Set, OR-Set:  add { element } -> add_ok
//                   remove { element } -> remove_ok          (OR-Set only)
//                   read -> read_ok { value: [element, ...] }
//   LWW register:   write { key, value } -> write_ok
//                   read { key } -> read_ok { value }         (null if never written)
//   MV register:    write { key, value } -> write_ok
//                   read { key } -> read_ok { values: [...] } (siblings; [] if never written)
//   Cart (OR-Map):  add { item, qty } -> add_ok
//                   remove { item } -> remove_ok
//                   read -> read_ok { value: { item: qty, ... } }
//
// Most tests follow a fixed script of "stories" (one element or key each, with
// a known correct outcome) on a timeline of healthy network, partition and
// heal, plus random background traffic that is only checked for convergence
// and for things that are always true.

import { check, j, plural, sample, split, unanswered } from '../workloads.js';

const sorted = (a) => [...a].map(String).sort();
const sameList = (a, b) => JSON.stringify(sorted(a)) === JSON.stringify(sorted(b));
const ms = (t) => `${Math.round(t)}ms`;
const isList = (v) => Array.isArray(v);
const isMap = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

// Records client operations and offers probe-based waiting.
function harness(sim) {
  const ops = [];
  const h = {
    ops,
    // A recorded client request. Resolves to the history entry.
    async op(node, req, client = 'c1') {
      const e = { node, req, res: null, invoke: sim.now(), complete: null };
      ops.push(e);
      e.res = await sim.clientRpc(node, req, { client });
      e.complete = sim.now();
      return e;
    },
    probe: (node, req) => sim.clientRpc(node, req, { probe: true, timeout: 400 }),
    // Poll `nodes` with `req` until every reply satisfies pred, or cap ms pass.
    // Resolves to { ok, last } where last maps node -> its last reply.
    async waitFor(nodes, req, pred, cap) {
      const end = sim.now() + cap;
      const last = {};
      for (;;) {
        const rs = await Promise.all(nodes.map((n) => h.probe(n, req)));
        nodes.forEach((n, i) => (last[n] = rs[i]));
        if (rs.every((r) => r && pred(r))) return { ok: true, last };
        if (sim.now() >= end) return { ok: false, last };
        await sim.sleep(60);
      }
    },
    // Fire requests from `make()` every `gap` ms between t0 and t1, without waiting for replies.
    noise(t0, t1, gap, make) {
      for (let t = t0; t < t1; t += gap) {
        sim.at(t, () => {
          const [node, req] = make();
          h.op(node, req, 'c2');
        });
      }
    },
  };
  return h;
}

// Sides of the scripted partition and the three actors used by the stories:
// a on the minority side, b and c on the majority side.
function actors(sim) {
  const [left, right] = split(sim);
  return { left, right, a: left[0], b: right[0], c: right[1] ?? right[0] };
}

function typeCheck(results, expected, valueOk, what) {
  const bad = results.find((r) => r.res && (r.res.type !== expected || (valueOk && !valueOk(r.res))));
  return check(
    'Replies have type "' + expected + '"' + (what ? ' ' + what : ''),
    !bad,
    bad && `${bad.node} replied ${j(bad.res)} to ${j(bad.req)}.`,
  );
}

// Every node answered the final read(s).
function finalCompleted(finals, expected) {
  const answered = finals.filter((f) => f.res).length;
  return check(
    'The final inspection completed',
    answered === expected,
    `Only ${answered} of ${expected} final reads were answered. The checks below are incomplete.`,
  );
}

// ================================================================== G-Set

export function gSet(sim, { ops = 90, partition, quiet = 1500 }) {
  const h = harness(sim);
  const reads = [];
  const finals = [];
  const opsEnd = sim.duration - quiet - 400;
  const gap = (opsEnd - 100) / ops;
  let next = 0;
  if (partition) {
    sim.at(partition[0], () => sim.partition(split(sim)));
    sim.at(partition[1], () => sim.heal());
  }

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      if (sim.rand() < 0.2) {
        const e = await h.op(node, { type: 'read' });
        reads.push(e);
      } else {
        // Mostly new elements; sometimes one that was already added (adds are idempotent).
        const element = next > 0 && sim.rand() < 0.2 ? 'e' + (1 + Math.floor(sim.rand() * next)) : 'e' + ++next;
        h.op(node, { type: 'add', element });
      }
    });
  }

  sim.at(sim.duration - 400, () => {
    for (const node of sim.nodeIds) h.op(node, { type: 'read' }, 'c3').then((e) => finals.push(e));
  });

  return () => {
    const addOps = h.ops.filter((o) => o.req.type === 'add');
    const allReads = [...reads, ...finals];
    const acked = new Set(addOps.filter((o) => o.res && o.res.type === 'add_ok').map((o) => o.req.element));
    const invoked = new Set(addOps.map((o) => o.req.element));
    const expected = sorted(acked);

    // A node's reads must include everything it acknowledged before the read began.
    let ownMissing = null;
    for (const r of reads) {
      if (!r.res || !isList(r.res.value)) continue;
      const have = new Set(r.res.value.map(String));
      const missed = addOps.find((o) => o.node === r.node && o.res && o.complete !== null && o.complete < r.invoke && !have.has(o.req.element));
      if (missed) {
        ownMissing = `${r.node} acknowledged add ${j(missed.req.element)} at ${ms(missed.complete)}, but its read at ${ms(r.invoke)} did not include it.`;
        break;
      }
    }
    // Reads at one node never lose an element.
    let shrank = null;
    const lastAt = new Map();
    for (const r of [...allReads].sort((x, y) => x.invoke - y.invoke)) {
      if (!r.res || !isList(r.res.value)) continue;
      const prev = lastAt.get(r.node);
      if (prev) {
        const now = new Set(r.res.value.map(String));
        const lost = prev.value.find((x) => !now.has(x));
        if (lost !== undefined && !shrank) shrank = `${r.node} returned ${j(lost)} at ${ms(prev.at)} but not at ${ms(r.invoke)}. A grow-only set never loses an element.`;
      }
      lastAt.set(r.node, { value: r.res.value.map(String), at: r.invoke });
    }
    let phantom = null;
    for (const r of allReads) {
      if (!r.res || !isList(r.res.value)) continue;
      const x = r.res.value.find((v) => !invoked.has(String(v)));
      if (x !== undefined) {
        phantom = `${r.node} returned ${j(x)}, which no client ever added.`;
        break;
      }
    }
    const wrong = finals.find((f) => f.res && (!isList(f.res.value) || !sameList(f.res.value, expected)));
    const describe = (f) => {
      if (!isList(f.res.value)) return `${f.node} replied ${j(f.res)}.`;
      const have = new Set(f.res.value.map(String));
      const missing = expected.filter((x) => !have.has(x));
      const extra = sorted(f.res.value).filter((x) => !acked.has(x));
      return `${f.node} has ${f.res.value.length} elements, ${expected.length} were acknowledged.` +
        (missing.length ? ` Missing: ${sample(missing)}.` : '') + (extra.length ? ` Unexpected: ${sample(extra)}.` : '') +
        ' A message sent once can be lost or cut off by a partition; something must resend it later.';
    };
    return [
      unanswered('Every add is acknowledged, even during a partition', addOps, 'Record the element locally and reply at once; replicate in the background.'),
      typeCheck(addOps, 'add_ok'),
      unanswered('Every read gets a reply', allReads),
      typeCheck(allReads, 'read_ok', (r) => isList(r.value), 'with value: [elements]'),
      check('A node reads its own acknowledged adds', !ownMissing, ownMissing),
      check('A node’s reads never lose an element', !shrank, shrank),
      check('Nothing appears that was never added', !phantom, phantom),
      finalCompleted(finals, sim.nodeIds.length),
      check(
        `All nodes converge on the ${plural(expected.length, 'acknowledged element')}`,
        finals.length === sim.nodeIds.length && !wrong,
        wrong ? describe(wrong) : 'Some nodes did not answer the final read.',
      ),
    ];
  };
}

// ================================================================= OR-Set

// Timeline (ms): stories on a healthy network from 100; partition at P;
// heal at H; post-heal stories; background traffic until N; final reads at F.
export function orSet(sim, { gap = 130 } = {}) {
  const h = harness(sim);
  const { left, right, a, b, c } = actors(sim);
  const all = sim.nodeIds;
  const P = 2300;
  const H = 3700;
  const N = sim.duration - 1900;
  const F = sim.duration - 400;
  const finals = [];
  const notes = []; // precondition failures, reported in the story checks
  const pool = ['p1', 'p2', 'p3', 'p4', 'p5', 'p6'];
  const add = (node, element) => h.op(node, { type: 'add', element });
  const remove = (node, element) => h.op(node, { type: 'remove', element });
  const has = (element) => (r) => isList(r.value) && r.value.map(String).includes(element);
  const lacks = (element) => (r) => isList(r.value) && !r.value.map(String).includes(element);
  const wait = async (nodes, pred, cap, what) => {
    const w = await h.waitFor(nodes, { type: 'read' }, pred, cap);
    if (!w.ok) notes.push(what + ' Last reads: ' + nodes.map((n) => n + '=' + j(w.last[n]?.value)).join(', ') + '.');
    return w.ok;
  };

  // --- healthy network
  sim.at(100, async () => {
    await add(a, 'x-local');
    await remove(a, 'x-local');
    await add(a, 'x-again');
    await remove(a, 'x-again');
    await add(a, 'x-again');
    await add(a, 'x-remote');
    await wait([b], has('x-remote'), 1200, `${b} never saw "x-remote" (added at ${a}) within 1.2s.`);
    await remove(b, 'x-remote');
    await add(a, 'x-readd');
    await remove(a, 'x-readd');
    await wait(all, lacks('x-readd'), 1200, `"x-readd" was removed at ${a} but some node still had it 1.2s later.`);
    await add(b, 'x-readd');
    await add(a, 'x-race');
    await add(b, 'x-cut-remove');
  });
  sim.at(1400, async () => {
    await wait(all, (r) => has('x-race')(r) && has('x-cut-remove')(r), P - 1500, `Before the partition at ${ms(P)}, not every node had "x-race" and "x-cut-remove".`);
  });

  // --- partition: a alone (or with the minority) against b and c
  sim.at(P, () => sim.partition([left, right]));
  sim.at(P + 100, async () => {
    remove(a, 'x-race'); // concurrent with the add at b below: neither side sees the other
    add(b, 'x-race');
    remove(a, 'x-cut-remove');
    add(a, 'x-both');
    add(b, 'x-both');
  });
  sim.at(H, () => sim.heal());

  // --- after the heal: c must see both copies of x-both before removing it
  sim.at(H + 100, async () => {
    await wait([c], has('x-both'), 1200, `${c} never saw "x-both" within 1.2s of the heal.`);
    await sim.sleep(1000);
    await remove(c, 'x-both');
  });

  h.noise(150, N, gap, () => {
    const node = sim.pick(all);
    const element = sim.pick(pool);
    return [node, { type: sim.rand() < 0.65 ? 'add' : 'remove', element }];
  });

  sim.at(F, () => {
    for (const node of all) h.op(node, { type: 'read' }, 'c3').then((e) => finals.push(e));
  });

  return () => {
    const adds = h.ops.filter((o) => o.req.type === 'add');
    const removes = h.ops.filter((o) => o.req.type === 'remove');
    const reads = h.ops.filter((o) => o.req.type === 'read');
    const done = finals.filter((f) => f.res && isList(f.res.value));
    const first = done[0];
    const diverged = done.find((f) => !sameList(f.res.value, first.res.value));
    const everAdded = new Set(adds.map((o) => o.req.element));
    const phantom = done.map((f) => ({ f, x: f.res.value.find((v) => !everAdded.has(String(v))) })).find((p) => p.x !== undefined);
    const final = (element) => done.filter((f) => !f.res.value.map(String).includes(element)).map((f) => f.node);
    const finalWith = (element) => done.filter((f) => f.res.value.map(String).includes(element)).map((f) => f.node);
    const expectAbsent = (element, why) => {
      const bad = finalWith(element);
      return bad.length ? `"${element}" is still present at ${sample(bad)}. ${why}` : null;
    };
    const expectPresent = (element, why) => {
      const bad = final(element);
      return bad.length ? `"${element}" is missing at ${sample(bad)}. ${why}` : null;
    };
    const story = (name, problems) => {
      const p = problems.filter(Boolean);
      return check(name, p.length === 0, p.join(' ') + (notes.length ? ' (Note: ' + notes[0] + ')' : ''));
    };

    // Background traffic: an add that starts after every remove of that element has finished must survive.
    let lostAdd = null;
    for (const e of pool) {
      const rs = removes.filter((o) => o.req.element === e);
      const lastRemove = Math.max(-1, ...rs.map((o) => (o.complete === null ? Infinity : o.complete)));
      const late = adds.find((o) => o.req.element === e && o.res && o.invoke > lastRemove);
      if (late && final(e).length) {
        lostAdd = `"${e}" was added at ${late.node} at ${ms(late.invoke)}, after every remove of it had finished, but it is missing at ${sample(final(e))}.`;
        break;
      }
    }

    return [
      unanswered('Every add and remove is acknowledged, even during a partition', [...adds, ...removes]),
      typeCheck(adds, 'add_ok'),
      typeCheck(removes, 'remove_ok'),
      typeCheck([...reads, ...finals], 'read_ok', (r) => isList(r.value), 'with value: [elements]'),
      finalCompleted(finals, all.length),
      check(
        'All nodes converge on the same set',
        done.length === all.length && !diverged,
        diverged
          ? `${first.node} has ${j(sorted(first.res.value))} but ${diverged.node} has ${j(sorted(diverged.res.value))}.`
          : 'Some nodes did not answer the final read.',
      ),
      check('Nothing appears that was never added', !phantom, phantom && `${phantom.f.node} returned ${j(phantom.x)}, which no client ever added.`),
      story('Removing an element removes it', [
        expectAbsent('x-local', `It was added and then removed at ${a}.`),
        expectAbsent('x-remote', `It was added at ${a} and removed at ${b} after ${b} had seen it. The remove must reach everyone, and gossip must not bring the element back.`),
      ]),
      story('A remove made during a partition sticks after the heal', [
        expectAbsent('x-cut-remove', `${a} removed it while cut off from ${b}, which still had it. After the heal ${b}'s copy must not bring it back: remember which adds were removed.`),
      ]),
      story('An element can be added again after a remove', [
        expectPresent('x-again', `It was added, removed and added again at ${a}. A remove deletes the adds it has seen, not the element forever.`),
        expectPresent('x-readd', `It was removed at ${a}, then added again at ${b}.`),
      ]),
      story('An add concurrent with a remove wins', [
        expectPresent('x-race', `During the partition ${a} removed it while ${b} added it again. The remove could not have seen ${b}'s new add, so that add must survive. Each add needs its own unique tag, even when the element is already present.`),
      ]),
      story('A remove deletes every add it has seen', [
        expectAbsent('x-both', `It was added at ${a} and at ${b} during the partition, and removed at ${c} more than a second after the heal. The remove must cover both adds.`),
      ]),
      check('An add made after every remove of it survives', !lostAdd, lostAdd),
    ];
  };
}

// ============================================================ LWW register

export function lwwRegister(sim, { gap = 140 } = {}) {
  const h = harness(sim);
  const { left, right, a, b, c } = actors(sim);
  const all = sim.nodeIds;
  const P = 2600;
  const H = 3900;
  const N = sim.duration - 1900;
  const F = sim.duration - 400;
  const noiseKeys = ['k1', 'k2', 'k3'];
  const keys = ['chain', 'race', 'tie', 'race2', 'mine', ...noiseKeys];
  const notes = [];
  const problems = { chain: [], mine: [], after: [] };
  const finals = [];
  const write = (node, key, value) => h.op(node, { type: 'write', key, value });
  const read = (node, key) => h.op(node, { type: 'read', key });
  const waitValue = async (nodes, key, value, cap, what) => {
    const w = await h.waitFor(nodes, { type: 'read', key }, (r) => r.value === value, cap);
    if (!w.ok) notes.push(what + ' Last reads of "' + key + '": ' + nodes.map((n) => n + '=' + j(w.last[n]?.value)).join(', ') + '.');
    return w.ok;
  };
  let seq = 0;

  // --- causal chain: each write is made by a node that has already seen the previous one
  sim.at(100, async () => {
    const chain = [a, b, c, a];
    for (let i = 0; i < chain.length; i++) {
      const value = 'c' + (i + 1);
      await write(chain[i], 'chain', value);
      const ok = await waitValue(all, 'chain', value, 1200, `After ${chain[i]} wrote "chain" = "${value}", not every node read it within 1.2s.`);
      if (!ok) problems.chain.push(`"chain" = "${value}" was written at ${chain[i]} after it had seen ${i ? '"c' + i + '"' : 'nothing'}, so it is newer, but not every node switched to it within 1.2s.`);
    }
  });

  // Background writes stop before the partition so every replica can settle first.
  h.noise(200, P - 500, gap, () => [sim.pick(all), { type: 'write', key: sim.pick(noiseKeys), value: 'w' + ++seq }]);
  h.noise(P + 300, N, gap, () => [sim.pick(all), { type: 'write', key: sim.pick(noiseKeys), value: 'w' + ++seq }]);

  // Settle: every node agrees on every key written so far.
  sim.at(P - 450, async () => {
    const settled = async () => {
      const rs = await Promise.all(all.flatMap((n) => noiseKeys.concat('chain').map((k) => h.probe(n, { type: 'read', key: k }))));
      const per = noiseKeys.length + 1;
      for (let i = per; i < rs.length; i++) if (!rs[i] || !rs[i - per] || rs[i].value !== rs[i % per].value) return false;
      return true;
    };
    while (sim.now() < P - 20 && !(await settled())) await sim.sleep(50);
  });

  sim.at(P, () => sim.partition([left, right]));
  // Both sides write the same key at the same moment, each after the same history.
  sim.at(P + 50, async () => {
    await Promise.all([write(a, 'race', 'left'), write(b, 'race', 'right')]);
    await Promise.all([write(a, 'tie', 'left'), write(b, 'tie', 'right')]);
    write(b, 'race2', 'R1');
    await write(a, 'race2', 'L1');
    await write(a, 'race2', 'L2');
    for (const v of ['m1', 'm2', 'm3']) {
      await write(a, 'mine', v);
      const r = await read(a, 'mine');
      if (r.res && r.res.value !== v) problems.mine.push(`${a} acknowledged "mine" = "${v}" and its next read returned ${j(r.res.value)}. Nobody else writes "mine".`);
    }
  });
  sim.at(H, () => sim.heal());

  // After the heal: once everyone agrees on "race", a new write must beat both racing values.
  sim.at(H + 100, async () => {
    const end = sim.now() + 1500;
    let values = [];
    for (;;) {
      const rs = await Promise.all(all.map((n) => h.probe(n, { type: 'read', key: 'race' })));
      values = rs.map((r) => r?.value);
      if (new Set(values).size === 1 && (values[0] === 'left' || values[0] === 'right')) break;
      if (sim.now() >= end) break;
      await sim.sleep(60);
    }
    if (new Set(values).size > 1) {
      problems.after.push(`1.5s after the heal the nodes still disagreed on "race": ${all.map((n, i) => n + '=' + j(values[i])).join(', ')}.`);
      await sim.sleep(300);
    }
    await write(c, 'race', 'after');
  });

  sim.at(F, () => {
    for (const node of all) for (const key of keys) h.op(node, { type: 'read', key }, 'c3').then((e) => finals.push(e));
  });

  return () => {
    const writes = h.ops.filter((o) => o.req.type === 'write');
    const reads = h.ops.filter((o) => o.req.type === 'read');
    const done = finals.filter((f) => f.res && f.res.type === 'read_ok');
    const finalOf = (key) => done.filter((f) => f.req.key === key);
    let diverged = null;
    for (const key of keys) {
      const fs = finalOf(key);
      const odd = fs.find((f) => f.res.value !== fs[0].res.value);
      if (odd) {
        diverged = `"${key}": ${fs.map((f) => f.node + '=' + j(f.res.value)).join(', ')}.` +
          (key === 'tie' ? ` "left" (${a}) and "right" (${b}) were written concurrently, probably with equal timestamps. Break ties by node id so every replica picks the same winner.` : '');
        break;
      }
    }
    let foreign = null;
    for (const f of done) {
      if (f.res.value === null || f.res.value === undefined) {
        if (writes.some((w) => w.req.key === f.req.key && w.res)) foreign ??= `${f.node} returned null for "${f.req.key}", which was written.`;
        continue;
      }
      if (!writes.some((w) => w.req.key === f.req.key && w.req.value === f.res.value)) foreign ??= `${f.node} returned ${j(f.res.value)} for "${f.req.key}", which was never written to that key.`;
    }
    const expect = (key, ok, msg) => {
      const bad = finalOf(key).filter((f) => !ok(f.res.value));
      return bad.length ? `${msg} Final reads: ${finalOf(key).map((f) => f.node + '=' + j(f.res.value)).join(', ')}.` : null;
    };
    const story = (name, list) => {
      const p = list.filter(Boolean);
      return check(name, p.length === 0, p.join(' ') + (notes.length && p.length ? ' (Note: ' + notes[0] + ')' : ''));
    };
    return [
      unanswered('Every write is acknowledged, even during a partition', writes),
      typeCheck(writes, 'write_ok'),
      unanswered('Every read gets a reply', reads),
      typeCheck(reads, 'read_ok', (r) => 'value' in r, 'with a value field'),
      finalCompleted(finals, all.length * keys.length),
      check('All nodes converge on the same value for every key', !diverged, diverged),
      check('Reads return a value written to that key', !foreign, foreign),
      story('A write beats every write its node had seen', [
        ...problems.chain,
        expect('chain', (v) => v === 'c4', `"chain" was written c1 (${a}), c2 (${b}), c3 (${c}), c4 (${a}), each after the previous one had reached everyone; it must end as "c4".`),
      ]),
      story('A replica reads back its own write', problems.mine),
      story('An overwritten value never comes back', [
        expect('race2', (v) => v === 'L2' || v === 'R1', `${a} wrote "race2" = "L1" and then "L2", so "L1" is older than "L2" and must lose. Use a clock that moves forward on every write.`),
      ]),
      story('A write after a merge beats the merged values', [
        ...problems.after,
        expect('race', (v) => v === 'after', `${c} wrote "race" = "after" once all nodes agreed on the racing value; it must win everywhere. Advance your clock past every timestamp you receive.`),
      ]),
    ];
  };
}

// ============================================================= MV register

export function mvRegister(sim, { gap = 140 } = {}) {
  const h = harness(sim);
  const { left, right, a, b, c } = actors(sim);
  const all = sim.nodeIds;
  const P = 2300;
  const H = 3700;
  const N = sim.duration - 1900;
  const F = sim.duration - 400;
  const noiseKeys = ['k1', 'k2', 'k3'];
  const keys = ['seq', 'conflict', 'resolve', 'sameside', ...noiseKeys];
  const problems = { seq: [], conflict: [], resolve: [], sameside: [] };
  const finals = [];
  const write = (node, key, value) => h.op(node, { type: 'write', key, value });
  const read = (node, key) => h.op(node, { type: 'read', key });
  const vals = (r) => (r && isList(r.values) ? sorted(r.values) : null);
  const waitValues = async (nodes, key, values, cap) => {
    const w = await h.waitFor(nodes, { type: 'read', key }, (r) => isList(r.values) && sameList(r.values, values), cap);
    return w.ok ? null : nodes.map((n) => n + '=' + j(vals(w.last[n]))).join(', ');
  };
  let seq = 0;

  sim.at(100, async () => {
    const chain = [a, b, c];
    for (let i = 0; i < chain.length; i++) {
      const v = 's' + (i + 1);
      await write(chain[i], 'seq', v);
      const bad = await waitValues(all, 'seq', [v], 1200);
      if (bad) problems.seq.push(`${chain[i]} wrote "seq" = "${v}" after seeing ${i ? '"s' + i + '"' : 'nothing'}, so it replaces it; 1.2s later the reads were ${bad}.`);
    }
  });

  sim.at(P, () => sim.partition([left, right]));
  sim.at(P + 100, async () => {
    write(a, 'conflict', 'left');
    write(b, 'conflict', 'right');
    write(a, 'resolve', 'L');
    write(b, 'resolve', 'R');
    (async () => {
      await write(b, 'sameside', 'R1');
      if (c !== b) {
        const bad = await waitValues([c], 'sameside', ['R1'], 800);
        if (bad) problems.sameside.push(`${c} did not see "R1" from ${b} within 0.8s (same side of the partition): ${bad}.`);
      }
      await write(c, 'sameside', 'R2');
    })();
    await write(a, 'sameside', 'L1');
    await write(a, 'sameside', 'L2');
    await sim.sleep(100);
    const r = await read(a, 'conflict');
    if (r.res && !sameList(vals(r.res) ?? [], ['left'])) problems.conflict.push(`During the partition ${a} could only have seen its own write, but read ${j(vals(r.res))}.`);
  });
  sim.at(H, () => sim.heal());

  sim.at(H + 100, async () => {
    const bad = await waitValues(all, 'resolve', ['L', 'R'], 1500);
    if (bad) problems.resolve.push(`1.5s after the heal, "resolve" should have read ["L","R"] everywhere (written concurrently at ${a} and ${b}): ${bad}.`);
    await write(c, 'resolve', 'merged');
  });

  h.noise(200, N, gap, () => [sim.pick(all), { type: 'write', key: sim.pick(noiseKeys), value: 'w' + ++seq }]);

  sim.at(F, () => {
    for (const node of all) for (const key of keys) h.op(node, { type: 'read', key }, 'c3').then((e) => finals.push(e));
  });

  return () => {
    const writes = h.ops.filter((o) => o.req.type === 'write');
    const reads = h.ops.filter((o) => o.req.type === 'read');
    const done = finals.filter((f) => f.res && isList(f.res.values));
    const finalOf = (key) => done.filter((f) => f.req.key === key);
    const show = (key) => finalOf(key).map((f) => f.node + '=' + j(vals(f.res))).join(', ');
    let diverged = null;
    for (const key of keys) {
      const fs = finalOf(key);
      if (fs.some((f) => !sameList(f.res.values, fs[0].res.values))) {
        diverged = `"${key}": ${show(key)}.`;
        break;
      }
    }
    let foreign = null;
    for (const f of done) {
      const x = f.res.values.find((v) => !writes.some((w) => w.req.key === f.req.key && w.req.value === v));
      if (x !== undefined) foreign ??= `${f.node} returned ${j(x)} for "${f.req.key}", which was never written to that key.`;
      else if (new Set(f.res.values).size !== f.res.values.length) foreign ??= `${f.node} returned ${j(f.res.values)} for "${f.req.key}": the same sibling twice.`;
    }
    const expect = (key, values, msg) =>
      finalOf(key).some((f) => !sameList(f.res.values, values)) ? `${msg} Expected ${j(values)}; final reads: ${show(key)}.` : null;
    const story = (name, list) => {
      const p = list.filter(Boolean);
      return check(name, p.length === 0, p.join(' '));
    };
    return [
      unanswered('Every write is acknowledged, even during a partition', writes),
      typeCheck(writes, 'write_ok'),
      unanswered('Every read gets a reply', reads),
      typeCheck(reads, 'read_ok', (r) => isList(r.values), 'with values: [...]'),
      finalCompleted(finals, all.length * keys.length),
      check('All nodes converge on the same siblings for every key', !diverged, diverged),
      check('Siblings are distinct values written to that key', !foreign, foreign),
      story('A write replaces every value its node had seen', [
        ...problems.seq,
        expect('seq', ['s3'], `"seq" was written s1 (${a}), s2 (${b}), s3 (${c}), each after the previous one had reached everyone.`),
      ]),
      story('Concurrent writes are both kept', [
        ...problems.conflict,
        expect('conflict', ['left', 'right'], `"left" (${a}) and "right" (${b}) were written on opposite sides of a partition; neither saw the other, so both must survive as siblings.`),
      ]),
      story('A write that has seen the siblings replaces them', [
        ...problems.resolve,
        expect('resolve', ['merged'], `${c} wrote "merged" after reading both siblings "L" and "R"; its version vector dominates both.`),
      ]),
      story('Only the latest write of each side survives', [
        ...problems.sameside,
        expect('sameside', ['L2', 'R2'], `${a} wrote "L1" then "L2"; on the other side ${b} wrote "R1" and ${c} then wrote "R2" after seeing it. "L1" and "R1" were overwritten.`),
      ]),
    ];
  };
}

// ======================================================== Cart (OR-Map)

export function cart(sim, { gap = 130 } = {}) {
  const h = harness(sim);
  const { left, right, a, b, c } = actors(sim);
  const all = sim.nodeIds;
  const P = 2300;
  const H = 3700;
  const N = sim.duration - 1900;
  const F = sim.duration - 400;
  const pool = ['p1', 'p2', 'p3', 'p4', 'p5'];
  const notes = [];
  const finals = [];
  const add = (node, item, qty) => h.op(node, { type: 'add', item, qty });
  const remove = (node, item) => h.op(node, { type: 'remove', item });
  const qtyIs = (item, q) => (r) => isMap(r.value) && r.value[item] === q;
  const wait = async (nodes, pred, cap, what) => {
    const w = await h.waitFor(nodes, { type: 'read' }, pred, cap);
    if (!w.ok) notes.push(what + ' Last reads: ' + nodes.map((n) => n + '=' + j(w.last[n]?.value)).join(', ') + '.');
  };

  sim.at(100, async () => {
    await add(a, 'apples', 2);
    await add(a, 'limes', 3);
    await add(b, 'limes', -1);
    await add(a, 'pears', 2);
    await remove(a, 'pears');
    await add(a, 'plums', 4);
    await wait([b], qtyIs('plums', 4), 1200, `${b} never saw plums = 4 (added at ${a}) within 1.2s.`);
    await remove(b, 'plums');
    await add(b, 'plums', 1);
    await add(a, 'figs', 3);
    await add(b, 'kiwis', 5);
  });
  sim.at(1400, async () => {
    await wait(all, (r) => qtyIs('figs', 3)(r) && qtyIs('kiwis', 5)(r) && qtyIs('apples', 2)(r), P - 1500, `Before the partition at ${ms(P)}, not every node had apples = 2, figs = 3 and kiwis = 5.`);
  });

  sim.at(P, () => sim.partition([left, right]));
  sim.at(P + 100, () => {
    add(a, 'apples', 1);
    add(b, 'apples', 3);
    remove(a, 'figs');
    add(b, 'figs', 2);
    remove(a, 'kiwis');
    add(a, 'dates', 1);
    add(b, 'dates', 1);
  });
  sim.at(H, () => sim.heal());
  sim.at(H + 100, async () => {
    await add(c, 'apples', 1);
    await wait([c], qtyIs('dates', 2), 1500, `${c} never saw dates = 2 within 1.5s of the heal.`);
    await remove(c, 'dates');
  });

  h.noise(150, N, gap, () => {
    const node = sim.pick(all);
    const item = sim.pick(pool);
    return [node, sim.rand() < 0.7 ? { type: 'add', item, qty: 1 + Math.floor(sim.rand() * 3) } : { type: 'remove', item }];
  });

  sim.at(F, () => {
    for (const node of all) h.op(node, { type: 'read' }, 'c3').then((e) => finals.push(e));
  });

  return () => {
    const adds = h.ops.filter((o) => o.req.type === 'add');
    const removes = h.ops.filter((o) => o.req.type === 'remove');
    const reads = h.ops.filter((o) => o.req.type === 'read');
    const done = finals.filter((f) => f.res && isMap(f.res.value));
    const canon = (m) => JSON.stringify(Object.keys(m).sort().map((k) => [k, m[k]]));
    const first = done[0];
    const diverged = done.find((f) => canon(f.res.value) !== canon(first.res.value));
    const everAdded = new Set(adds.map((o) => o.req.item));
    const phantom = done.map((f) => ({ f, x: Object.keys(f.res.value).find((k) => !everAdded.has(k)) })).find((p) => p.x !== undefined);
    const show = (item) => done.map((f) => f.node + '=' + (item in f.res.value ? j(f.res.value[item]) : 'absent')).join(', ');
    const expectQty = (item, q, why) => (done.some((f) => f.res.value[item] !== q) ? `${item} should be ${q}: ${why} Final reads: ${show(item)}.` : null);
    const expectAbsent = (item, why) => (done.some((f) => item in f.res.value) ? `${item} should be gone: ${why} Final reads: ${show(item)}.` : null);
    const story = (name, list) => {
      const p = list.filter(Boolean);
      return check(name, p.length === 0, p.join(' ') + (notes.length ? ' (Note: ' + notes[0] + ')' : ''));
    };

    // Background items: present if added after every remove, and the quantity is within bounds.
    let bgProblem = null;
    for (const item of pool) {
      const rs = removes.filter((o) => o.req.item === item);
      const lastRemove = Math.max(-1, ...rs.map((o) => (o.complete === null ? Infinity : o.complete)));
      const its = adds.filter((o) => o.req.item === item);
      const late = its.filter((o) => o.res && o.invoke > lastRemove);
      const low = late.reduce((s, o) => s + o.req.qty, 0);
      const high = its.reduce((s, o) => s + o.req.qty, 0);
      for (const f of done) {
        const q = f.res.value[item];
        if (late.length && q === undefined) bgProblem ??= `${item} was added at ${late[0].node} at ${ms(late[0].invoke)}, after every remove of it had finished, but is missing at ${f.node}.`;
        else if (q !== undefined && (q < low || q > high)) bgProblem ??= `${f.node} reports ${item} = ${j(q)}; adds made after its last remove total ${low} and all adds ever total ${high}.`;
      }
    }

    return [
      unanswered('Every add and remove is acknowledged, even during a partition', [...adds, ...removes]),
      typeCheck(adds, 'add_ok'),
      typeCheck(removes, 'remove_ok'),
      typeCheck([...reads, ...finals], 'read_ok', (r) => isMap(r.value), 'with value: { item: qty }'),
      finalCompleted(finals, all.length),
      check(
        'All nodes converge on the same cart',
        done.length === all.length && !diverged,
        diverged ? `${first.node} has ${j(first.res.value)} but ${diverged.node} has ${j(diverged.res.value)}.` : 'Some nodes did not answer the final read.',
      ),
      check('Only items that were added appear', !phantom, phantom && `${phantom.f.node} has ${j(phantom.x)}, which was never added.`),
      story('Concurrent quantity changes all count', [
        expectQty('apples', 7, `+2 at ${a}, then +1 at ${a} and +3 at ${b} on opposite sides of a partition, then +1 at ${c}. Quantities are counters, not last-writer-wins values.`),
        expectQty('limes', 2, `+3 at ${a}, then -1 at ${b}.`),
      ]),
      story('A remove clears the item and its quantity', [
        expectAbsent('pears', `added and then removed at ${a}.`),
        expectAbsent('kiwis', `${a} removed them during the partition while ${b} still had them; ${b}'s copy must not bring them back.`),
        expectAbsent('dates', `+1 at ${a} and +1 at ${b} during the partition, then removed at ${c} after it read dates = 2.`),
      ]),
      story('After a remove, the quantity starts again from zero', [
        expectQty('plums', 1, `4 were added at ${a}, ${b} removed plums after seeing 4, then added 1. The removed 4 must not come back.`),
      ]),
      story('An add concurrent with a remove wins, with only its own quantity', [
        expectQty('figs', 2, `figs were at 3 everywhere; during the partition ${a} removed them while ${b} added 2. The add survives the remove, the 3 that ${a} saw do not.`),
      ]),
      check('Background items: an add after every remove survives, with a sensible quantity', !bgProblem, bgProblem),
    ];
  };
}
