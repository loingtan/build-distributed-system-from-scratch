// Workloads for the Raft log replication track.
//
// Nodes are observed only through client messages:
//
//   status                -> status_ok { role, term, leader }        (from the election task)
//   log                   -> log_ok { entries: [{ term, ... }], commit }
//   append { value }      -> append_ok | not_leader { leader }       (task 1)
//   put { key, value }    -> put_ok    | not_leader { leader }       (tasks 2-4)
//   get { key }           -> get_ok { value }                        (tasks 2-4)
//
// A background sampler probes every node every 100ms and folds what it sees
// into one "canonical" log: the entries some node has reported as settled
// (in task 1 the whole log, later only the committed prefix). Any node that
// ever reports something else at one of those indexes has broken safety.

import { check, j, plural } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;

// JSON with sorted keys, so two entries with the same fields always compare equal.
const stable = (v) =>
  JSON.stringify(v, (_, x) =>
    x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, x[k]])) : x,
  );

const PATIENCE = 2000; // how long a client keeps retrying one write
const SETTLE = 2500; // how long the cluster gets to recover from a fault

function raft(sim, { kv, persist = false, scenario = 'stable', clients = 2, gap = 90, keys = 3 }) {
  const ids = sim.nodeIds;
  const majority = Math.floor(ids.length / 2) + 1;
  const okType = kv ? 'put_ok' : 'append_ok';
  const word = kv ? 'put' : 'append';

  const canon = []; // { text, entry, node, t }
  const lives = new Map(ids.map((n) => [n, 0])); // bumped on every crash
  const lastLog = new Map(); // node -> { upto, t }
  const lastTerm = new Map(); // node -> { term, t }
  const statuses = [];
  const problem = { silent: null, shape: null, conflict: null, shrink: null, term: null, client: null, early: null };

  const quiet = []; // { from, until }: writes started in here are not required to succeed
  const frozen = []; // { from, to }: no majority exists, nothing may commit or be acknowledged
  const progress = []; // { from, to, what }: at least one write must be acknowledged in here
  const strict = (t) => t >= 2000 && quiet.every((q) => t + PATIENCE + 100 <= q.from || t >= q.until);
  const inFrozen = (t) => frozen.find((w) => t > w.from && t < w.to);

  // ------------------------------------------------------------ sampling

  const seeLog = (node, t, res, life) => {
    const sameLife = lives.get(node) === life;
    if (!res) {
      if (sameLife) problem.silent ??= `${node} did not answer { type: "log" } sent at ${ms(t)}.`;
      return;
    }
    const entries = res.entries;
    const shaped =
      res.type === 'log_ok' &&
      Array.isArray(entries) &&
      entries.every((e) => e && typeof e === 'object' && !Array.isArray(e) && Number.isInteger(e.term)) &&
      (!kv || (Number.isInteger(res.commit) && res.commit >= 0));
    if (!shaped) {
      problem.shape ??= `${node} replied ${j(res)} to { type: "log" }; expected { type: "log_ok", entries: [{ term, ... }]${kv ? ', commit' : ''} }.`;
      return;
    }
    if (kv && res.commit > entries.length) {
      problem.conflict ??= `At ${ms(t)} ${node} reported commit = ${res.commit} but holds only ${plural(entries.length, 'entry').replace('entrys', 'entries')}. A committed entry must never be removed from a log. Either it was never really committed (a leader may count copies only of entries from its own term), or the node dropped entries that matched the leader's (for example on a late, duplicate message).`;
      return;
    }
    const upto = kv ? res.commit : entries.length;
    for (let i = 0; i < upto; i++) {
      const text = stable(entries[i]);
      if (i < canon.length) {
        if (canon[i].text !== text && !problem.conflict) {
          const c = canon[i];
          problem.conflict = kv
            ? `Index ${i + 1}: ${c.node} reported ${j(c.entry)} as committed at ${ms(c.t)}, but at ${ms(t)} ${node} reports ${j(entries[i])} as committed there. Once an entry is committed, no node may ever hold anything else at that index.`
            : `Index ${i + 1}: ${c.node} held ${j(c.entry)} at ${ms(c.t)}, but at ${ms(t)} ${node} holds ${j(entries[i])}. Followers must store entries in the leader's order, even when messages overtake one another.`;
        }
      } else {
        const w = inFrozen(t);
        if (w) {
          problem.early ??= `At ${ms(t)} ${node} reported index ${i + 1} (${j(entries[i])}) as committed, but since ${ms(w.down)} only ${w.alive} of ${ids.length} nodes were running. An entry is committed only once ${majority} nodes store it.`;
        }
        canon.push({ text, entry: entries[i], node, t });
      }
    }
    if (!sameLife) return;
    const prev = lastLog.get(node);
    if (prev && upto < prev.upto) {
      problem.shrink ??= kv
        ? `${node} reported commit = ${prev.upto} at ${ms(prev.t)} and commit = ${upto} at ${ms(t)}.`
        : `${node} held ${prev.upto} entries at ${ms(prev.t)} and only ${upto} at ${ms(t)}.`;
    }
    lastLog.set(node, { upto, t });
  };

  const seeStatus = (node, t, res, life) => {
    const sameLife = lives.get(node) === life;
    if (!res) {
      if (sameLife) problem.silent ??= `${node} did not answer { type: "status" } sent at ${ms(t)}. Keep the status handler from the election task.`;
      return;
    }
    if (res.type !== 'status_ok' || !Number.isInteger(res.term) || typeof res.role !== 'string') {
      problem.shape ??= `${node} replied ${j(res)} to { type: "status" }; expected { type: "status_ok", role, term, leader }.`;
      return;
    }
    statuses.push({ t, node, role: res.role, term: res.term });
    if (!sameLife && !persist) return;
    const prev = lastTerm.get(node);
    if (prev && res.term < prev.term) {
      problem.term ??= `${node} was in term ${prev.term} at ${ms(prev.t)} and reports term ${res.term} at ${ms(t)}${sameLife ? '' : ', after its restart'}.${persist ? ' A node that forgets its term can vote again in a term it already voted in.' : ''}`;
    }
    lastTerm.set(node, { term: res.term, t });
  };

  const probe = (node) => {
    if (!sim.isUp(node)) return;
    const t = sim.now();
    const life = lives.get(node);
    sim.clientRpc(node, { type: 'log' }, { probe: true, timeout: 300 }).then((res) => seeLog(node, t, res, life));
    sim.clientRpc(node, { type: 'status' }, { probe: true, timeout: 300 }).then((res) => seeStatus(node, t, res, life));
  };
  for (let t = 100; t < sim.duration - 400; t += 100) sim.at(t, () => ids.forEach(probe));

  const currentLeader = () => {
    const recent = statuses.filter((o) => o.t >= sim.now() - 250 && o.role === 'leader' && sim.isUp(o.node));
    recent.sort((a, b) => b.term - a.term || b.t - a.t);
    return recent[0]?.node ?? ids[0];
  };
  const others = (leader) => {
    const rest = ids.filter((n) => n !== leader);
    for (let i = rest.length - 1; i > 0; i--) {
      const k = Math.floor(sim.rand() * (i + 1));
      [rest[i], rest[k]] = [rest[k], rest[i]];
    }
    return rest;
  };

  // -------------------------------------------------------------- faults

  const crash = (nodes) => {
    for (const n of nodes) {
      sim.crash(n);
      lives.set(n, lives.get(n) + 1);
      lastLog.delete(n);
      if (!persist) lastTerm.delete(n);
    }
  };
  const restart = (nodes) => {
    for (const n of nodes) {
      sim.restart(n);
      probe(n); // look at the node before anyone else has spoken to it
    }
  };
  const cut = (minority) => sim.partition([minority, ids.filter((n) => !minority.includes(n))]);

  // Writes sent straight to a leader that has just been cut off. They can
  // never commit there; they exist to leave uncommitted entries in its log.
  const strand = (node, count) => {
    for (let i = 0; i < count; i++) {
      sim.at(sim.now() + 150 + i * 120, async () => {
        const op = newOp('c9');
        const res = await sim.clientRpc(node, op.body, { client: 'c9', timeout: 6000 });
        if (res && res.type === okType) acked(op, node);
      });
    }
  };

  const downFor = (t0, t1, count, { leaderToo = false } = {}) => {
    let down = [];
    sim.at(t0, () => {
      const leader = currentLeader();
      down = leaderToo ? [leader, ...others(leader)].slice(0, count) : others(leader).slice(0, count);
      crash(down);
    });
    sim.at(t1, () => restart(down));
  };

  switch (scenario) {
    case 'follower-restart':
      downFor(2600, 3800, 1);
      break;
    case 'minority-down':
      downFor(3000, 4500, ids.length - majority);
      break;
    case 'majority-down': {
      const count = ids.length - majority + 1;
      downFor(3000, 5000, count);
      frozen.push({ from: 3250, to: 5000, down: 3000, alive: ids.length - count });
      quiet.push({ from: 3000, until: 6000 });
      break;
    }
    case 'cut-leader':
      sim.at(3000, () => {
        const leader = currentLeader();
        cut([leader, others(leader)[0]]);
        strand(leader, 3);
      });
      sim.at(6000, () => sim.heal());
      quiet.push({ from: 3000, until: 6000 + SETTLE });
      progress.push({ from: 3000 + SETTLE, to: 6000, what: 'the partition at 3000ms' });
      break;
    case 'stale-follower':
      sim.at(3000, () => cut([others(currentLeader())[0]]));
      sim.at(6500, () => sim.heal());
      quiet.push({ from: 3000, until: 6500 + SETTLE });
      progress.push({ from: 3000 + SETTLE, to: 6500, what: 'the partition at 3000ms' });
      break;
    case 'churn': {
      // The leader is cut off with uncommitted entries. It rejoins at the
      // moment its successor is cut off, so it is repaired by a third leader
      // elected after it came back. That leader starts nextIndex at the end
      // of its own, much longer log, past where the stale entries begin:
      // a follower that checks only log length keeps them.
      sim.at(2500, () => {
        const first = currentLeader();
        cut([first]);
        strand(first, 3);
      });
      sim.at(5500, () => {
        const second = currentLeader();
        cut([second]);
        strand(second, 3);
      });
      sim.at(8500, () => sim.heal());
      quiet.push({ from: 2500, until: 8500 + SETTLE });
      progress.push({ from: 2500 + SETTLE, to: 5500, what: 'the partition at 2500ms' });
      progress.push({ from: 5500 + SETTLE, to: 8500, what: 'the partition at 5500ms' });
      break;
    }
    case 'leader-restart':
      downFor(3000, 4500, 1, { leaderToo: true });
      quiet.push({ from: 3000, until: 3000 + SETTLE });
      break;
    case 'full-restart':
      downFor(3500, 4000, ids.length, { leaderToo: true });
      quiet.push({ from: 3500, until: 4000 + SETTLE });
      break;
    case 'rolling':
      downFor(2500, 3500, 1, { leaderToo: true });
      downFor(5000, 6000, 2, { leaderToo: true });
      downFor(7500, 8000, ids.length, { leaderToo: true });
      quiet.push({ from: 2500, until: 8000 + SETTLE });
      progress.push({ from: 2500 + SETTLE, to: 7500, what: 'the crashes at 2500ms and 5000ms (a majority was always running)' });
      break;
    case 'reorder': {
      // One message from the leader to follower F is held back 150ms, so
      // newer ones overtake it; the other follower G hears nothing from the
      // leader meanwhile, so entries acknowledged in that time are committed
      // with F's copy alone. The moment the old message reaches F, the leader
      // is cut off: F's log is what the next leader builds on.
      const T = 3000;
      let L = null;
      let F = null;
      let G = null;
      let held = null;
      let tagNext = false;
      let done = false;
      const isolate = () => {
        if (done || !L) return;
        done = true;
        cut([L]);
      };
      sim.at(T, () => {
        L = currentLeader();
        [F, G] = others(L);
      });
      sim.intercept = ({ src, dest, kind }) => {
        if (!L || done || kind !== 'node' || src !== L) return undefined;
        if (dest === G) return 'drop';
        if (dest === F && !held && !tagNext) {
          tagNext = true;
          return 150;
        }
        return undefined;
      };
      sim.onSend = (msg) => {
        if (!tagNext) return;
        held = msg;
        tagNext = false;
      };
      // onDeliver runs before F's handler: the cut lands before F reacts.
      sim.onDeliver = (msg) => msg === held && isolate();
      sim.at(T + 1000, isolate);
      sim.at(6000, () => sim.heal());
      quiet.push({ from: T, until: 6000 + SETTLE });
      progress.push({ from: T + SETTLE, to: 6000, what: 'the partition at 3000ms' });
      break;
    }
    case 'figure8': {
      // Figure 8 of the Raft paper, staged with partitions on five nodes.
      //  a. Leader S1 (term t) is cut off with S2 and takes writes U that only S2 gets.
      //  b. The other three elect X (term > t); X is cut off alone and takes one write V.
      //  c. S1, S2 and the remaining two (R) reconnect without X and elect W, which has U.
      //     W copies U to R: U is now on a majority, but it is from term t, not W's term.
      //  d. W is cut off; X reconnects with R. X's last term beats U's, so X can win and
      //     replace U with V. Only a leader that counted U as committed is caught out.
      // Client requests are dropped from b to d, so no entry of a newer term gets in the way.
      const T = 3000;
      let phase = 0;
      let at = 0;
      let S1;
      let S2;
      let X;
      let R = [];
      let tOld = 0;
      let tX = 0;
      let blocked = new Set();
      let healed = false;
      sim.intercept = ({ src, dest, kind }) =>
        kind === 'client' && /^c\d+$/.test(src) && src !== 'c9' && blocked.has(dest) ? 'drop' : undefined;
      const leaderIn = (group, above) =>
        statuses
          .filter((o) => o.t >= sim.now() - 150 && o.role === 'leader' && group.includes(o.node) && o.term > above)
          .sort((a, b) => b.term - a.term)[0] ?? null;
      const heal = () => {
        if (healed) return;
        healed = true;
        blocked = new Set();
        sim.heal();
        quiet.push({ from: T, until: sim.now() + SETTLE });
      };
      sim.at(T, () => {
        S1 = currentLeader();
        tOld = statuses.filter((o) => o.node === S1).at(-1)?.term ?? 0;
        let rest;
        [S2, ...rest] = others(S1);
        R = rest;
        sim.partition([[S1, S2], rest]);
        blocked = new Set(rest);
        strand(S1, 3);
        phase = 1;
      });
      const step = () => {
        if (healed) return;
        const now = sim.now();
        if (phase === 1) {
          const l = leaderIn(R, tOld);
          if (!l) return;
          X = l.node;
          tX = l.term;
          R = R.filter((n) => n !== X);
          sim.partition([[S1, S2], [X], R]);
          blocked = new Set(ids);
          strand(X, 1);
          phase = 2;
          at = now;
        } else if (phase === 2 && now >= at + 400) {
          sim.partition([[S1, S2, ...R], [X]]);
          phase = 3;
        } else if (phase === 3 && leaderIn([S1, S2, ...R], tX)) {
          phase = 4;
          at = now;
        } else if (phase === 4 && now >= at + 400) {
          sim.partition([[X, ...R], [S1], [S2]]);
          blocked = new Set();
          phase = 5;
          at = now;
        } else if (phase === 5 && now >= at + 2500) {
          heal();
        }
      };
      for (let t = T + 50; t < sim.duration; t += 50) sim.at(t, step);
      sim.at(T + 5500, heal); // whatever happened, the network is whole again by then
      break;
    }
    case 'double-vote': {
      // A vote is a promise. Leader L crashes; the first follower to stand (A)
      // asks the other (V) for its vote. The instant V has sent its answer, V
      // crashes. L restarts, still in the old term, times out and stands in
      // the same term as A. Its request to V is held back just long enough to
      // restart V first. A V that forgot its vote grants it again: two leaders
      // in one term. A's messages are dropped meanwhile, so nobody learns of A.
      const T = 3000;
      let L = null;
      let A = null;
      let V = null;
      let phase = 0;
      let clientsBlocked = false;
      sim.at(T - 300, () => (clientsBlocked = true)); // let the logs settle to equal
      sim.at(T, () => {
        L = currentLeader();
        crash([L]);
        phase = 1;
      });
      sim.intercept = ({ src, dest, kind }) => {
        if (clientsBlocked && kind === 'client' && /^c\d+$/.test(src)) return 'drop';
        if (kind !== 'node') return undefined;
        if (phase === 1 && src !== L && dest !== L) {
          A = src;
          V = dest;
          phase = 2;
        } else if (phase >= 3 && phase < 9 && src === A) {
          return 'drop';
        } else if (phase === 3 && src === L && dest === V) {
          phase = 4;
          sim.at(sim.now(), () => restart([V]));
          return 30;
        }
        return undefined;
      };
      sim.onSend = (msg) => {
        if (phase !== 2 || msg.kind !== 'node' || msg.src !== V || msg.dest !== A) return;
        crash([V]); // its answer is on the wire; whatever is not yet on disk is gone
        phase = 3;
        sim.at(sim.now() + 50, () => restart([L]));
      };
      sim.at(T + 2500, () => {
        phase = 9;
        clientsBlocked = false;
      });
      quiet.push({ from: T - 300, until: T + 2500 + SETTLE });
      break;
    }
    default:
  }

  // ------------------------------------------------------------- clients

  const ops = [];
  let counter = 0;
  let known = null; // the node that last acknowledged a write

  function newOp(client) {
    const value = ++counter;
    const key = kv ? `k${1 + Math.floor(sim.rand() * keys)}` : undefined;
    const body = kv ? { type: 'put', key, value } : { type: 'append', value };
    const op = { client, key, value, body, invoke: sim.now(), acked: null, node: null, read: null };
    ops.push(op);
    return op;
  }
  function acked(op, node) {
    op.acked = sim.now();
    op.node = node;
    const w = inFrozen(op.acked);
    if (w) {
      problem.early ??= `${node} acknowledged ${j(op.body)} at ${ms(op.acked)}, but since ${ms(w.down)} only ${w.alive} of ${ids.length} nodes were running. A write may be acknowledged only once ${majority} nodes store it.`;
    }
  }

  const nextNode = (from, avoid) => {
    const start = ids.indexOf(from);
    const ring = ids.map((_, i) => ids[(start + 1 + i) % ids.length]).filter((n) => sim.isUp(n));
    const free = ring.find((n) => !avoid.has(n));
    if (free) return free;
    avoid.clear();
    return ring[0] ?? from;
  };

  // One write, the way a real client library does it: follow not_leader
  // hints, move on after 500ms of silence, give up after PATIENCE.
  async function submit(client, body) {
    const started = sim.now();
    const avoid = new Set();
    let target = known && sim.isUp(known) ? known : nextNode(ids[ids.length - 1], avoid);
    while (sim.now() - started < PATIENCE) {
      const res = await sim.clientRpc(target, body, { client, timeout: 500 });
      if (res && res.type === okType) {
        known = target;
        return target;
      }
      avoid.add(target);
      if (res && res.type === 'not_leader') {
        const hint = res.leader;
        target = typeof hint === 'string' && ids.includes(hint) && sim.isUp(hint) && !avoid.has(hint) ? hint : nextNode(target, avoid);
        await sim.sleep(25);
      } else if (res) {
        problem.client ??= `${target} replied ${j(res)} to ${j(body)}; expected { type: "${okType}" } or { type: "not_leader", leader }.`;
        return null;
      } else {
        target = nextNode(target, avoid);
      }
    }
    return null;
  }

  const stopAt = sim.duration - PATIENCE - 400;
  for (let c = 1; c <= clients; c++) {
    const client = `c${c}`;
    sim.at(2000 + c * 31, async () => {
      while (sim.now() < stopAt) {
        const op = newOp(client);
        const node = await submit(client, op.body);
        if (node) {
          acked(op, node);
          if (kv) {
            const life = lives.get(node);
            const res = await sim.clientRpc(node, { type: 'get', key: op.key }, { client, timeout: 500 });
            if (res && lives.get(node) === life && sim.isUp(node)) op.read = res;
          }
        }
        await sim.sleep(gap * (0.5 + sim.rand()));
      }
    });
  }

  // ------------------------------------------------------ final snapshot

  const final = { logs: new Map(), gets: [] };
  const keyNames = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  sim.at(sim.duration - 250, () => {
    const t = sim.now();
    for (const node of ids) {
      const life = lives.get(node);
      sim.clientRpc(node, { type: 'log' }, { probe: true, timeout: 200 }).then((res) => {
        seeLog(node, t, res, life);
        final.logs.set(node, res);
      });
      if (!kv) continue;
      for (const key of keyNames) {
        sim.clientRpc(node, { type: 'get', key }, { probe: true, timeout: 200 }).then((res) => final.gets.push({ node, key, res }));
      }
    }
  });

  // -------------------------------------------------------------- checks

  return () => {
    const done = ops.filter((o) => o.acked !== null);
    const checks = [
      check('Every node answers log and status requests', !problem.silent, problem.silent),
      check('Replies are well formed', !problem.shape && !problem.client, problem.shape ?? problem.client),
    ];
    if (problem.shape || problem.client) return checks;

    // Where each written value sits in the canonical log.
    const first = new Map();
    const last = new Map();
    canon.forEach((c, i) => {
      const v = c.entry.value;
      if (kv && typeof c.entry.key !== 'string') return; // a no-op entry
      if (!first.has(v)) first.set(v, i + 1);
      last.set(v, i + 1);
    });

    checks.push(
      kv
        ? check('No two nodes ever report different committed entries at one index', !problem.conflict, problem.conflict)
        : check('No two nodes ever hold different entries at one index', !problem.conflict, problem.conflict),
      kv
        ? check('A node\'s commit index never goes backwards', !problem.shrink, problem.shrink)
        : check('Logs only grow', !problem.shrink, problem.shrink),
    );
    let twoLeaders = null;
    const leaderOf = new Map();
    for (const o of statuses) {
      if (o.role !== 'leader') continue;
      const who = leaderOf.get(o.term);
      if (who && who !== o.node) twoLeaders ??= `Both ${who} and ${o.node} reported role "leader" in term ${o.term}.${persist ? ' A node that forgets its vote in a crash can vote twice in one term.' : ' A node may vote only once per term.'}`;
      if (!who) leaderOf.set(o.term, o.node);
    }
    checks.push(check('At most one leader per term', !twoLeaders, twoLeaders));
    if (persist) checks.push(check('Terms survive a restart', !problem.term, problem.term));
    if (frozen.length) checks.push(check('Nothing is committed or acknowledged without a majority', !problem.early, problem.early));

    // Liveness.
    // Decided now, not when the write was sent: staged scenarios add their quiet window when they heal.
    const strictOps = ops.filter((o) => o.client !== 'c9' && strict(o.invoke));
    const stuck = strictOps.filter((o) => o.acked === null);
    checks.push(
      check(
        `Every ${word} is acknowledged within 2s while the cluster is healthy`,
        strictOps.length > 0 && stuck.length === 0,
        stuck.length
          ? `${plural(stuck.length, word)} of ${strictOps.length} got no ${okType}. First: ${j(stuck[0].body)}, sent at ${ms(stuck[0].invoke)}, retried on every node for 2s.${done.length ? '' : ` No ${word} was ever acknowledged.`}`
          : `No ${word} was sent.`,
      ),
    );
    for (const w of progress) {
      const got = done.some((o) => o.acked > w.from && o.acked < w.to);
      checks.push(
        check(
          `Writes are acknowledged again within 2.5s of ${w.what.split(' (')[0]}`,
          got,
          `No ${word} was acknowledged between ${ms(w.from)} and ${ms(w.to)}, although a majority of nodes could reach each other after ${w.what}. They must elect a leader among themselves and keep committing.`,
        ),
      );
    }

    // Acknowledged writes are durable.
    const lost = done.filter((o) => !first.has(o.value));
    checks.push(
      check(
        kv ? 'Every acknowledged put is in the committed log' : 'Every acknowledged value is in the log',
        lost.length === 0,
        lost.length &&
          `${plural(lost.length, 'acknowledged ' + word)} of ${done.length} ${lost.length === 1 ? 'is' : 'are'} missing. First: ${lost[0].node} acknowledged ${j(lost[0].body)} at ${ms(lost[0].acked)}, but ${kv ? 'no node reports it as committed' : 'it is in no log'} at the end.${kv ? ' Acknowledge only what is committed, and never let a committed entry be overwritten.' : ' Only the leader may accept an append.'}`,
      ),
    );

    // One client sends its writes one after another, so they must appear in that order.
    let swapped = null;
    const prevBy = new Map();
    for (const o of done) {
      if (!first.has(o.value)) continue;
      const p = prevBy.get(o.client);
      if (p && first.get(o.value) < first.get(p.value)) {
        swapped ??= `${o.client} sent ${j(p.body)} and, after it was acknowledged, ${j(o.body)}; in the log they are at index ${first.get(p.value)} and ${first.get(o.value)}.`;
      }
      prevBy.set(o.client, o);
    }
    checks.push(check('Writes from one client stay in the order they were acknowledged', !swapped, swapped));

    if (kv) {
      const stale = done.find((o) => {
        if (!o.read || !first.has(o.value)) return false;
        if (o.read.type !== 'get_ok') return true;
        const seen = last.get(o.read.value);
        return seen === undefined || seen < first.get(o.value);
      });
      checks.push(
        check(
          'A write is applied before it is acknowledged',
          !stale,
          stale &&
            `${stale.node} acknowledged ${j(stale.body)} at ${ms(stale.acked)}, then answered ${j(stale.read)} to a get of ${stale.key}. Apply the entry to the state machine first, then reply put_ok.`,
        ),
      );
    }

    // Convergence.
    let behind = null;
    for (const node of ids) {
      const res = final.logs.get(node);
      if (!res || !Array.isArray(res.entries)) {
        behind ??= `${node} did not answer the final log request at ${ms(sim.duration - 250)}.`;
        continue;
      }
      const have = kv ? res.commit : res.entries.length;
      if (have < canon.length) {
        behind ??= kv
          ? `${node} has committed ${have} of ${plural(canon.length, 'entry').replace('entrys', 'entries')} at the end (it holds ${res.entries.length}). Leaders must tell followers how far the log is committed, and bring every follower up to date.`
          : `${node} holds ${have} of ${plural(canon.length, 'entry').replace('entrys', 'entries')} at the end. The leader must keep sending a follower what it is missing until it has everything.`;
      }
    }
    checks.push(check(kv ? 'Every node ends with the whole log committed' : 'Every node ends with the same log', canon.length > 0 && !behind, behind ?? 'No entries were ever reported.'));

    if (kv) {
      const expected = {};
      for (const c of canon) if (typeof c.entry.key === 'string') expected[c.entry.key] = c.entry.value;
      const wrong = final.gets.find((g) => !g.res || g.res.type !== 'get_ok' || (g.res.value ?? null) !== (expected[g.key] ?? null));
      checks.push(
        check(
          'Every node\'s state machine matches the committed log',
          !wrong && final.gets.length > 0,
          wrong
            ? `${wrong.node} answered ${j(wrong.res)} to a get of ${wrong.key}; applying the ${canon.length} committed entries in order gives ${j(expected[wrong.key] ?? null)}. Apply each committed entry exactly once, in log order, on every node.`
            : 'No get was answered.',
        ),
      );
    }
    return checks;
  };
}

// Task 1: appends to a stable leader; whole logs must agree.
export function raftAppend(sim, params) {
  return raft(sim, { ...params, kv: false });
}

// Tasks 2-4: a key-value state machine on top of the log.
export function raftKv(sim, params) {
  return raft(sim, { ...params, kv: true });
}
