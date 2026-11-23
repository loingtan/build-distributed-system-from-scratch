// Workloads for the "Distributed transactions" track.
//
// Every node is a bank branch holding two accounts, `a` and `b`, with 100
// each. A client sends a `transfer` to any node; that node coordinates a
// transaction whose moves touch accounts on two or more nodes. At the end the
// checker asks every node for its balances and the ids of the transactions it
// applied, and compares the nodes with one another and with the replies.

import { check, j, plural, sample, unanswered, wrongType } from '../workloads.js';

const START = 100;
const ACCOUNTS = ['a', 'b'];
// The briefs promise clients an answer within one second; this is the slack.
const REPLY_TIMEOUT = 1200;

const name = (node, account) => `${node}.${account}`;
const signed = (n) => (n > 0 ? `+${n}` : String(n));

function shuffled(sim, items) {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i--) {
    const k = Math.floor(sim.rand() * (i + 1));
    [out[i], out[k]] = [out[k], out[i]];
  }
  return out;
}

// Shared machinery: sending transfers, watching balances, crashing nodes and
// producing the checks. Each exported workload only writes the script.
function bank(sim, { needSomeCommit = false } = {}) {
  const transfers = []; // in the order they were sent
  const races = []; // groups of transfers that competed for one account
  const negatives = [];
  const statuses = {};
  const downs = [];
  // What the balances should be if every reply so far was truthful. Only used
  // to choose interesting amounts; the verdict is computed from the nodes.
  const est = Object.fromEntries(sim.nodeIds.map((id) => [id, Object.fromEntries(ACCOUNTS.map((a) => [a, START]))]));
  let count = 0;

  async function transfer(coordinator, moves, { expect = 'auto', why = '', client = 'c1', mayBeLost = false } = {}) {
    const txn = `t${++count}`;
    const req = { type: 'transfer', txn, moves };
    const rec = { txn, node: coordinator, moves, req, res: null, invoke: sim.now(), expect, why, mayBeLost };
    transfers.push(rec);
    rec.res = await sim.clientRpc(coordinator, req, { timeout: REPLY_TIMEOUT, client });
    if (rec.res && rec.res.committed === true) for (const m of moves) est[m.node][m.account] += m.delta;
    return rec;
  }

  const move = (from, to, amount) => [
    { node: from[0], account: from[1], delta: -amount },
    { node: to[0], account: to[1], delta: amount },
  ];

  // Move everything the account should hold. It only commits if earlier
  // transactions were applied in full and left nothing locked or reserved.
  const drain = (coordinator, from, to, opts) => transfer(coordinator, move(from, to, Math.max(1, est[from[0]][from[1]])), opts);

  const richest = (node) => (est[node].a >= est[node].b ? 'a' : 'b');

  function crash(id) {
    if (!sim.isUp(id)) return;
    sim.crash(id);
    downs.push({ node: id, start: sim.now(), end: Infinity });
  }
  function restart(id) {
    if (sim.isUp(id)) return;
    sim.restart(id);
    const span = downs.findLast((d) => d.node === id);
    if (span) span.end = sim.now();
  }
  const wasDown = (id, from, to) => downs.some((d) => d.node === id && d.start <= to && d.end >= from);

  // Hooks on the message stream, used to cut the network or crash a node at a
  // precise point of a transaction without knowing the learner's protocol.
  const hooks = { send: null, deliver: null };
  sim.onSend = (m) => m.kind === 'node' && hooks.send && hooks.send(m);
  sim.onDeliver = (m) => m.kind === 'node' && hooks.deliver && hooks.deliver(m);

  // Sample balances while the test runs: a negative balance may be transient.
  for (let t = 60; t < sim.duration - 400; t += 150) {
    sim.at(t, () => {
      for (const id of sim.nodeIds) {
        if (!sim.isUp(id)) continue;
        sim.clientRpc(id, { type: 'status' }, { probe: true, timeout: 300 }).then((res) => {
          const balances = res && typeof res.balances === 'object' && res.balances;
          if (!balances) return;
          for (const a of Object.keys(balances)) {
            if (typeof balances[a] === 'number' && balances[a] < 0) negatives.push({ node: id, account: a, value: balances[a], t: Math.round(sim.now()) });
          }
        });
      }
    });
  }

  sim.at(sim.duration - 300, () => {
    for (const id of sim.nodeIds) {
      restart(id);
      sim.clientRpc(id, { type: 'status' }, { timeout: 250 }).then((res) => (statuses[id] = res));
    }
  });

  const describe = (t) =>
    `${t.txn} (${t.moves.map((m) => `${name(m.node, m.account)} ${signed(m.delta)}`).join(', ')}; sent to ${t.node} at ${Math.round(t.invoke)}ms)`;

  function finish() {
    const checks = [];
    const mustAnswer = transfers.filter((t) => !t.mayBeLost && !wasDown(t.node, t.invoke, t.invoke + REPLY_TIMEOUT));
    checks.push(
      unanswered(
        'Every transfer gets a reply',
        mustAnswer,
        'The client waits one second. A coordinator must decide and answer even when a participant stays silent.',
      ),
    );
    checks.push(wrongType(transfers, 'transfer_ok'));
    const vague = transfers.find((t) => t.res && typeof t.res.committed !== 'boolean');
    checks.push(check('Replies say committed: true or false', !vague, vague && `${vague.node} answered ${j(vague.res)} to ${vague.txn}.`));

    const broken = sim.nodeIds.filter((id) => {
      const s = statuses[id];
      return !s || s.type !== 'status_ok' || !s.balances || typeof s.balances !== 'object' || !Array.isArray(s.committed) || ACCOUNTS.some((a) => typeof s.balances[a] !== 'number');
    });
    checks.push(
      check(
        'Every node answers status',
        broken.length === 0,
        broken.length &&
          `${broken[0]} answered ${j(statuses[broken[0]] ?? null)} at the end of the run. Expected { type: "status_ok", balances: { a, b }, committed: [ids] }.`,
      ),
    );
    if (broken.length) return checks;

    const applied = Object.fromEntries(sim.nodeIds.map((id) => [id, new Set(statuses[id].committed)]));
    const byId = new Map(transfers.map((t) => [t.txn, t]));
    const participants = (t) => [...new Set(t.moves.map((m) => m.node))];
    const appliedOn = (t) => participants(t).filter((p) => applied[p].has(t.txn));

    // 1. Atomicity: every participant applied it, or none did.
    const torn = transfers.filter((t) => {
      const on = appliedOn(t).length;
      return on !== 0 && on !== participants(t).length;
    });
    checks.push(
      check(
        'All or nothing',
        torn.length === 0,
        torn.length &&
          `${plural(torn.length, 'transaction')} applied on some nodes only (${sample(torn.map((t) => t.txn))}). ${describe(torn[0])} is committed on ${appliedOn(torn[0]).join(', ')} but not on ${participants(torn[0])
            .filter((p) => !applied[p].has(torn[0].txn))
            .join(', ')}. No node may apply a transaction unless every participant voted yes, and once one node commits, every other must too.`,
      ),
    );

    // 2. The client was told the truth.
    const lied = transfers.find((t) => {
      if (!t.res || typeof t.res.committed !== 'boolean') return false;
      const on = appliedOn(t).length;
      return t.res.committed ? on !== participants(t).length : on !== 0;
    });
    checks.push(
      check(
        'Replies match what the nodes did',
        !lied,
        lied &&
          `${describe(lied)} was answered committed: ${lied.res.committed}, but at the end it is applied on ${appliedOn(lied).length ? appliedOn(lied).join(', ') : 'no node'} out of ${participants(lied).join(', ')}.`,
      ),
    );

    // 3. Each balance is the start plus exactly the committed moves.
    let drift = null;
    for (const id of sim.nodeIds) {
      const want = Object.fromEntries(ACCOUNTS.map((a) => [a, START]));
      const unknown = statuses[id].committed.filter((txn) => !byId.has(txn));
      for (const t of transfers) {
        if (!applied[id].has(t.txn)) continue;
        for (const m of t.moves) if (m.node === id) want[m.account] += m.delta;
      }
      for (const a of ACCOUNTS) {
        if (!drift && statuses[id].balances[a] !== want[a]) {
          drift = `${name(id, a)} is ${statuses[id].balances[a]}, but ${START} plus the moves of the ${plural(applied[id].size, 'transaction')} ${id} lists as committed gives ${want[a]}. A move was applied twice, applied without being listed, or lost${unknown.length ? ` (it also lists unknown ids: ${sample(unknown)})` : ''}.`;
        }
      }
    }
    checks.push(check('Balances match the committed transactions', !drift, drift));

    // 4. Conservation.
    const total = sim.nodeIds.reduce((sum, id) => sum + ACCOUNTS.reduce((s, a) => s + statuses[id].balances[a], 0), 0);
    const wantTotal = sim.nodeIds.length * ACCOUNTS.length * START;
    checks.push(
      check(
        'No money created or destroyed',
        total === wantTotal,
        `The cluster started with ${wantTotal} and ends with ${total}. Every transfer moves money, so a changed total means some transfer was applied on one side only.`,
      ),
    );

    // 5. Overdrafts, during the run or at the end.
    for (const id of sim.nodeIds) {
      for (const a of ACCOUNTS) if (statuses[id].balances[a] < 0) negatives.push({ node: id, account: a, value: statuses[id].balances[a], t: Math.round(sim.duration) });
    }
    const neg = negatives[0];
    checks.push(
      check(
        'No balance goes negative',
        !neg,
        neg &&
          `${name(neg.node, neg.account)} was ${neg.value} at ${neg.t}ms. Two transactions that each fit on their own were both let through. Money promised to a prepared transaction must not be promised again.`,
      ),
    );

    // 6. Decisions. Replay in send order, using what the nodes actually did.
    const model = Object.fromEntries(sim.nodeIds.map((id) => [id, Object.fromEntries(ACCOUNTS.map((a) => [a, START]))]));
    let wrongDecision = null;
    for (const t of transfers) {
      const short = t.moves.find((m) => model[m.node][m.account] + m.delta < 0);
      const said = t.res && typeof t.res.committed === 'boolean' ? t.res.committed : null;
      if (!wrongDecision && said !== null) {
        if (t.expect === 'auto' && !short && !said) {
          wrongDecision = `${describe(t)} was refused, although nothing else was in flight and the money was there (${t.moves
            .filter((m) => m.delta < 0)
            .map((m) => `${name(m.node, m.account)} held ${model[m.node][m.account]}`)
            .join(', ')}). Is an earlier transaction still holding a lock or a reservation, or did its outcome never reach one of these nodes?`;
        } else if (t.expect === 'auto' && short && said) {
          wrongDecision = `${describe(t)} was committed, although ${name(short.node, short.account)} held only ${model[short.node][short.account]}. A participant must vote no when a move would take an account below zero.`;
        } else if (t.expect === 'abort' && said) {
          wrongDecision = `${describe(t)} was answered committed: true, although ${t.why}.`;
        }
      }
      if (appliedOn(t).length) for (const m of t.moves) model[m.node][m.account] += m.delta;
    }
    checks.push(check('Transfers commit exactly when they can', !wrongDecision, wrongDecision));

    if (races.length) {
      const nobody = races.find((group) => group.every((t) => t.res && t.res.committed === false));
      checks.push(
        check(
          'One of several racing transfers wins',
          !nobody,
          nobody &&
            `${nobody.map((t) => t.txn).join(', ')} all took money from ${name(nobody[0].moves[0].node, nobody[0].moves[0].account)} at ${Math.round(nobody[0].invoke)}ms and all were refused, although any one of them fits. The first to prepare should win; only the later ones conflict.`,
        ),
      );
    }
    if (needSomeCommit) {
      const done = transfers.filter((t) => t.res && t.res.committed === true).length;
      checks.push(
        check(
          'Transfers still commit on a lossy network',
          done >= transfers.length / 4,
          `Only ${done} of ${transfers.length} transfers committed. Losing a message should cost one transaction at most, not block the ones after it.`,
        ),
      );
    }
    return checks;
  }

  return { transfer, move, drain, richest, crash, restart, hooks, races, est, finish };
}

// A node's richest account, as [node, account].
const rich = (b, node) => [node, b.richest(node)];
const roles = (sim, round) => {
  const n = sim.nodeIds.length;
  return [sim.nodeIds[round % n], sim.nodeIds[(round + 1) % n], sim.nodeIds[(round + 2) % n]];
};

// ------------------------------------------------- 1. prepare and commit

// One transfer at a time. `overdraw` is the share of transfers that ask for
// more than the account holds; `threeWay` the share with three moves.
export function tpcBasic(sim, { ops = 24, overdraw = 0, threeWay = 0, gap = 100 }) {
  const b = bank(sim);
  sim.at(100, async () => {
    for (let i = 0; i < ops && sim.now() < sim.duration - 700; i++) {
      const nodes = shuffled(sim, sim.nodeIds);
      const from = rich(b, nodes[0]);
      const have = b.est[from[0]][from[1]];
      const amount = sim.rand() < overdraw ? have + 1 + Math.floor(sim.rand() * 40) : 2 + Math.floor(sim.rand() * Math.min(have - 1, 60));
      const to = [nodes[1], sim.pick(ACCOUNTS)];
      let moves = b.move(from, to, amount);
      if (sim.rand() < threeWay) {
        // Split the credit: a second account on the same node, or a third node.
        const part = 1 + Math.floor(sim.rand() * (amount - 1));
        const other = nodes.length > 2 && sim.rand() < 0.6 ? [nodes[2], sim.pick(ACCOUNTS)] : [to[0], to[1] === 'a' ? 'b' : 'a'];
        moves = [moves[0], { node: to[0], account: to[1], delta: amount - part }, { node: other[0], account: other[1], delta: part }];
      }
      await b.transfer(sim.pick(sim.nodeIds), moves);
      await sim.sleep(gap);
    }
  });
  return b.finish;
}

// ------------------------------------------------------------ 2. isolation

export function tpcConcurrent(sim, { scenario = 'race' }) {
  const b = bank(sim);
  const ids = sim.nodeIds;

  if (scenario === 'race') {
    // Three transfers leave the same account at the same moment. Each fits on
    // its own; together they do not.
    sim.at(100, async () => {
      for (let round = 0; sim.now() < sim.duration - 1500; round++) {
        const source = ids
          .flatMap((id) => ACCOUNTS.map((a) => [id, a]))
          .reduce((best, x) => (b.est[x[0]][x[1]] > b.est[best[0]][best[1]] ? x : best));
        const amount = Math.ceil(b.est[source[0]][source[1]] * 0.6);
        const targets = shuffled(sim, ids.filter((id) => id !== source[0]).flatMap((id) => ACCOUNTS.map((a) => [id, a])));
        const group = await Promise.all(
          [0, 1, 2].map((i) => b.transfer(ids[(round + i) % ids.length], b.move(source, targets[i], amount), { expect: null, client: `c${i + 1}` })),
        );
        b.races.push(group);
        await sim.sleep(150);
        // What is left must be spendable, and nothing more than that.
        await b.drain(ids[round % ids.length], source, targets[3]);
        await sim.sleep(150);
        await b.transfer(ids[(round + 1) % ids.length], b.move(source, targets[0], 25));
        await sim.sleep(150);
      }
    });
  } else if (scenario === 'cross') {
    // Two transfers between the same pair of accounts, in opposite directions.
    sim.at(100, async () => {
      for (let round = 0; sim.now() < sim.duration - 1500; round++) {
        const [x, y, z] = roles(sim, round);
        const left = [x, 'a'];
        const right = [y, 'a'];
        await Promise.all([
          b.transfer(x, b.move(left, right, 20), { expect: null, client: 'c1' }),
          b.transfer(y, b.move(right, left, 30), { expect: null, client: 'c2' }),
        ]);
        await sim.sleep(150);
        // Whatever happened, both accounts must be usable again.
        await b.transfer(z, b.move(left, [z, 'b'], 10));
        await sim.sleep(150);
        await b.transfer(z, b.move([z, 'b'], right, 10));
        await sim.sleep(150);
      }
    });
  } else {
    // Many overlapping transfers with large amounts, then a quiet check.
    const stop = sim.duration - 3200;
    let i = 0;
    for (let t = 100; t < stop; t += 20) {
      sim.at(t, () => {
        const [x, y] = shuffled(sim, ids);
        const amount = 30 + Math.floor(sim.rand() * 60);
        b.transfer(sim.pick(ids), b.move([x, sim.pick(ACCOUNTS)], [y, sim.pick(ACCOUNTS)], amount), { expect: null, client: `c${(i++ % 4) + 1}` });
      });
    }
    sim.at(stop + 1300, async () => {
      for (let k = 0; k < ids.length; k++) {
        for (const a of ACCOUNTS) {
          await b.drain(ids[(k + 2) % ids.length], [ids[k], a], [ids[(k + 1) % ids.length], a]);
          await sim.sleep(120);
        }
      }
    });
  }
  return b.finish;
}

// ------------------------------------------------- 3. timeouts, partitions

const CUT = 1500; // how long a partition lasts
const SETTLE = 1000; // the briefs promise everything is finished this long after a heal or restart

export function tpcPartitions(sim, { scenario = 'unreachable', rounds = 3 }) {
  const b = bank(sim, { needSomeCommit: scenario === 'lossy' });
  const ids = sim.nodeIds;
  const others = (id) => ids.filter((x) => x !== id);

  if (scenario === 'lossy') {
    sim.at(100, async () => {
      while (sim.now() < sim.duration - 3600) {
        const [x, y] = shuffled(sim, ids);
        await b.transfer(sim.pick(ids), b.move(rich(b, x), [y, sim.pick(ACCOUNTS)], 1 + Math.floor(sim.rand() * 30)), { expect: null });
        await sim.sleep(300);
      }
    });
    return b.finish;
  }

  sim.at(100, async () => {
    for (let round = 0; round < rounds; round++) {
      const [c, p1, p2] = roles(sim, round);
      const from = rich(b, p1);
      const spare = [p1, from[1] === 'a' ? 'b' : 'a'];

      if (scenario === 'unreachable') {
        // p2 is cut off before the transaction starts.
        const start = sim.now();
        sim.partition([[p2], others(p2)]);
        await sim.sleep(100);
        await b.transfer(c, b.move(from, [p2, 'a'], 30), {
          expect: 'abort',
          why: `${p2} was cut off from ${c} for the whole transaction and cannot have voted yes`,
        });
        await sim.sleep(200);
        // The reachable participant must be free again, partition or not.
        await b.drain(c, from, [c, 'b']);
        await sim.sleep(150);
        await b.transfer(p2, b.move(spare, [c, 'a'], 20), {
          expect: 'abort',
          why: `the coordinator ${p2} was cut off from both participants`,
        });
        await sim.sleep(Math.max(0, start + CUT - sim.now()));
        sim.heal();
        await sim.sleep(SETTLE);
        await b.transfer(p1, b.move([p2, 'a'], spare, 40));
        await sim.sleep(150);
        await b.transfer(p2, b.move(spare, [c, 'a'], 20));
        await sim.sleep(150);
      } else if (scenario === 'voted') {
        // p2 is cut off the moment it has sent its first message (its vote)
        // to the coordinator: it is prepared and cannot hear the outcome.
        let cutAt = null;
        const cut = () => {
          b.hooks.send = null;
          cutAt = sim.now();
          sim.partition([[p2], others(p2)]);
        };
        b.hooks.send = (m) => m.src === p2 && m.dest === c && cut();
        await b.transfer(c, b.move(from, [p2, 'a'], 30), { expect: null });
        if (cutAt === null) cut();
        await sim.sleep(200);
        await b.transfer(c, b.move(spare, [c, 'a'], 10));
        await sim.sleep(Math.max(0, cutAt + CUT - sim.now()));
        sim.heal();
        await sim.sleep(SETTLE);
        // Only possible if p2 learned the outcome and released the account.
        await b.drain(p1, [p2, 'a'], [c, 'b']);
        await sim.sleep(150);
        await b.transfer(p2, b.move([c, 'b'], from, 20));
        await sim.sleep(150);
      } else {
        // The coordinator is cut off right after its first message to another
        // node: one participant is prepared, and no vote can come back.
        let cutAt = null;
        const cut = () => {
          b.hooks.send = null;
          cutAt = sim.now();
          sim.partition([[c], others(c)]);
        };
        b.hooks.send = (m) => m.src === c && m.dest !== c && cut();
        await b.transfer(c, b.move(from, [p2, 'a'], 30), {
          expect: 'abort',
          why: `${c} was cut off from ${p1} and ${p2} before any vote could reach it`,
        });
        if (cutAt === null) cut();
        await sim.sleep(Math.max(0, cutAt + CUT - sim.now()));
        sim.heal();
        await sim.sleep(SETTLE);
        // Both accounts must have been released by the abort.
        await b.drain(p2, from, [c, 'a']);
        await sim.sleep(150);
        await b.drain(p1, [p2, 'a'], [c, 'b']);
        await sim.sleep(150);
      }
    }
  });
  return b.finish;
}

// --------------------------------------------------------- 4. crashes

const DOWN = 600; // how long a crashed node stays down

export function tpcCrashes(sim, { scenario = 'participant', rounds = 3 }) {
  const b = bank(sim);
  const ids = sim.nodeIds;

  if (scenario === 'random') {
    const stop = sim.duration - 4200;
    let i = 0;
    for (let t = 100; t < stop; t += 110) {
      sim.at(t, () => {
        const up = ids.filter((id) => sim.isUp(id));
        const [x, y] = shuffled(sim, ids);
        const amount = 5 + Math.floor(sim.rand() * 40);
        b.transfer(sim.pick(up), b.move([x, sim.pick(ACCOUNTS)], [y, sim.pick(ACCOUNTS)], amount), { expect: null, client: `c${(i++ % 3) + 1}` });
      });
    }
    for (let t = 300; t < stop; t += 450) {
      const at = t + Math.floor(sim.rand() * 200);
      sim.at(at, () => {
        const victim = sim.pick(ids);
        b.crash(victim);
        sim.at(sim.now() + 250 + Math.floor(sim.rand() * 300), () => b.restart(victim));
      });
    }
    sim.at(stop + 2000, async () => {
      for (let k = 0; k < ids.length; k++) {
        for (const a of ACCOUNTS) {
          await b.drain(ids[(k + 2) % ids.length], [ids[k], a], [ids[(k + 1) % ids.length], a]);
          await sim.sleep(120);
        }
      }
    });
    return b.finish;
  }

  sim.at(100, async () => {
    for (let round = 0; round < rounds; round++) {
      const [c, p1, p2] = roles(sim, round);
      const from = rich(b, p1);
      const spare = [p1, from[1] === 'a' ? 'b' : 'a'];
      let downAt = null;
      const kill = (id) => {
        b.hooks.send = null;
        b.hooks.deliver = null;
        downAt = sim.now();
        b.crash(id);
        sim.at(downAt + DOWN, () => b.restart(id));
      };
      const settled = () => sim.sleep(Math.max(0, downAt + DOWN + SETTLE - sim.now()));

      if (scenario === 'participant') {
        // p2 dies the instant it sends its first message (its vote) to the
        // coordinator. Anything it writes to disk after that send is lost.
        b.hooks.send = (m) => m.src === p2 && m.dest === c && kill(p2);
        await b.transfer(c, b.move(from, [p2, 'a'], 30), { expect: null });
        if (downAt === null) kill(p2);
        await settled();
        await b.drain(p1, [p2, 'a'], [c, 'b']);
        await sim.sleep(150);
        // And once more with p2 already down when the transaction starts.
        kill(p2);
        await sim.sleep(50);
        await b.transfer(c, b.move(spare, [p2, 'b'], 20), {
          expect: 'abort',
          why: `${p2} was down for the whole transaction and cannot have voted yes`,
        });
        await settled();
        await b.transfer(p2, b.move([p2, 'b'], spare, 20));
        await sim.sleep(150);
        await b.drain(c, spare, [c, 'a']);
        await sim.sleep(150);
      } else if (scenario === 'early') {
        // The coordinator dies just before the first vote reaches it.
        b.hooks.deliver = (m) => m.dest === c && m.src !== c && kill(c);
        await b.transfer(c, b.move(from, [p2, 'a'], 30), { expect: null, mayBeLost: true });
        if (downAt === null) kill(c);
        await settled();
        await b.drain(p2, from, [c, 'a']);
        await sim.sleep(150);
        await b.drain(p1, [p2, 'a'], [c, 'b']);
        await sim.sleep(150);
      } else {
        // The coordinator dies while sending out its decision: once it has
        // heard from both participants, its next message to one of them is
        // the last thing it does.
        const heard = new Set();
        b.hooks.deliver = (m) => {
          if (m.dest !== c || (m.src !== p1 && m.src !== p2)) return;
          heard.add(m.src);
          if (heard.size === 2) {
            b.hooks.deliver = null;
            b.hooks.send = (s) => s.src === c && (s.dest === p1 || s.dest === p2) && kill(c);
          }
        };
        await b.transfer(c, b.move(from, [p2, 'a'], 30), { expect: null, mayBeLost: true });
        if (downAt === null) kill(c);
        await settled();
        await b.drain(p1, [p2, 'a'], [c, 'b']);
        await sim.sleep(150);
        await b.transfer(p2, b.move(from, [c, 'a'], 10));
        await sim.sleep(150);
      }
    }
  });
  return b.finish;
}
