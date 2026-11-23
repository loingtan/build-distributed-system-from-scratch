// Workloads for the Paxos track.
//
// Single-decree tasks (1 and 2). Nodes are observed only through client messages:
//
//   propose { value }  -> propose_ok { value }   value is the CHOSEN value, which may differ
//   get                -> get_ok { value }       the chosen value this node knows, or null
//
// Multi-Paxos tasks (3 and 4):
//
//   append { value }   -> append_ok | not_leader { leader }
//   log                -> log_ok { entries }     values chosen for slots 1..k, no gaps; null is a no-op
//
// A background sampler probes every node every 100ms. Every value a node
// reports (in a probe or in a reply to a client) is checked against every
// other: two different chosen values, or two different values at one slot,
// is a broken safety guarantee.

import { check, j, plural } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;
// A node must answer a probe within this long. Real processes stall behind fsyncs.
const PROBE_TIMEOUT = (sim) => (sim.real ? 1000 : 300);

// Does a message body mention this client value anywhere? Used to stage
// interleavings without depending on the learner's message types: the value
// travels in the phase 2 (accept) message, whatever it is called.
function mentions(body, value) {
  if (body === value) return true;
  if (!body || typeof body !== 'object') return false;
  return Object.values(body).some((v) => mentions(v, value));
}

function distinctValues(sim, count) {
  const out = new Set();
  while (out.size < count) out.add(10 + Math.floor(sim.rand() * 990));
  return [...out];
}

// =================================================================
// Single-decree Paxos
// =================================================================

const BOUND = 2000; // a proposal must be answered within this long once the network is healthy

export function paxosSingle(sim, { scenario = 'one' }) {
  const ids = sim.nodeIds;
  const majority = Math.floor(ids.length / 2) + 1;
  const lives = new Map(ids.map((n) => [n, 0]));
  const seen = []; // { node, t, value, how }
  const lastBy = new Map(); // node -> { value, t, life }
  const problem = { silent: null, shape: null, changed: null };
  const ops = []; // { client, value, first, invoke, quiet, at, answer, by }
  const proposed = new Map(); // value -> invoke time

  // A node's answer was produced somewhere between t0 (request sent) and t
  // (reply received). Reports are compared by t; forgetting is flagged only
  // when the null answer was requested after a non-null one had arrived.
  const observe = (node, t0, t, value, how, life) => {
    seen.push({ node, t, value, how });
    if (lives.get(node) !== life) return;
    const prev = lastBy.get(node);
    if (prev && value !== null && prev.value !== value) {
      problem.changed ??= `${node} reported ${j(prev.value)} as chosen at ${ms(prev.t)} and ${j(value)} at ${ms(t)}. Once a value is chosen it is chosen for good.`;
    } else if (prev && value === null && t0 > prev.t) {
      problem.changed ??= `${node} reported ${j(prev.value)} as chosen at ${ms(prev.t)} and null at ${ms(t)}, without crashing in between. A learned value is never forgotten.`;
    }
    if (value !== null && !prev) lastBy.set(node, { value, t, life });
  };

  const probe = (node) => {
    if (!sim.isUp(node)) return;
    const t = sim.now();
    const life = lives.get(node);
    sim.clientRpc(node, { type: 'get' }, { probe: true, timeout: PROBE_TIMEOUT(sim) }).then((res) => {
      if (!res) {
        if (lives.get(node) === life && sim.isUp(node)) problem.silent ??= `${node} did not answer { type: "get" } sent at ${ms(t)}.`;
        return;
      }
      if (res.type !== 'get_ok' || !('value' in res)) {
        problem.shape ??= `${node} replied ${j(res)} to { type: "get" }; expected { type: "get_ok", value } with value null until this node knows the chosen value.`;
        return;
      }
      observe(node, t, sim.now(), res.value ?? null, 'get', life);
    });
  };
  for (let t = 100; t < sim.duration - 400; t += 100) sim.at(t, () => ids.forEach(probe));

  const crash = (nodes) => {
    for (const n of nodes) {
      sim.crash(n);
      lives.set(n, lives.get(n) + 1);
      lastBy.delete(n);
    }
  };
  const restart = (nodes) => {
    for (const n of nodes) {
      sim.restart(n);
      probe(n);
    }
  };

  // One proposal, the way a client library does it: wait 1s for an answer,
  // then try the next node with the same value, until the end of the run.
  // `quiet` is when the network is healthy again for this proposal.
  const propose = (at, client, node, value, quiet = at) => {
    sim.at(at, async () => {
      const op = { client, value, first: node, invoke: sim.now(), quiet: Math.max(quiet, sim.now()), at: null, answer: undefined, by: null };
      ops.push(op);
      if (!proposed.has(value)) proposed.set(value, op.invoke);
      let target = node;
      while (sim.now() < sim.duration - 1100) {
        const life = lives.get(target);
        const res = await sim.clientRpc(target, { type: 'propose', value }, { client, timeout: 1000 });
        if (res) {
          if (res.type !== 'propose_ok' || res.value === undefined || res.value === null) {
            problem.shape ??= `${target} replied ${j(res)} to ${j({ type: 'propose', value })}; expected { type: "propose_ok", value } carrying the chosen value. Reply only once this node knows which value was chosen.`;
            return;
          }
          op.at = sim.now();
          op.answer = res.value;
          op.by = target;
          observe(target, op.at, op.at, res.value, 'propose_ok', life);
          return;
        }
        const i = ids.indexOf(target);
        target = ids[(i + 1) % ids.length];
      }
    });
  };

  let quietAt = 0; // from here on, every node must learn the chosen value
  const values = distinctValues(sim, 12);
  let vi = 0;
  const next = () => values[vi++];

  switch (scenario) {
    case 'one': {
      propose(200, 'c1', ids[0], next());
      propose(1500, 'c2', ids[ids.length - 1], next());
      break;
    }
    case 'concurrent': {
      ids.forEach((n, i) => propose(300 + sim.rand() * 4, `c${i + 1}`, n, next()));
      ids.forEach((n, i) => propose(2200 + i * 20, `c${i + 1}`, ids[(i + 2) % ids.length], next()));
      break;
    }
    case 'hidden': {
      // A value is chosen but only its proposer knows. n1 proposes X: its
      // accept reaches n2, and from that moment nothing more from n1 reaches
      // n2. n1 and n3 cannot hear each other at all. n1 has a majority
      // (itself and n2) and reports X. Then n3 proposes Y: its phase 1 reaches
      // n2, which has accepted X. A proposer that ignores the accepted value
      // in the promises gets Y chosen too.
      const T = 300;
      const [A, B, C] = ids;
      const X = next();
      const Y = next();
      let staging = true;
      let armed = false;
      sim.intercept = ({ src, dest, kind }) => {
        if (!staging || kind !== 'node') return undefined;
        if ((src === A && dest === C) || (src === C && dest === A)) return 'drop';
        if (armed && src === A && dest === B) return 'drop';
        return undefined;
      };
      sim.onDeliver = (msg) => {
        if (staging && msg.kind === 'node' && msg.src === A && msg.dest === B && mentions(msg.body, X)) armed = true;
      };
      propose(T, 'c1', A, X);
      propose(T + 400, 'c2', C, Y, T + 1600);
      sim.at(T + 1600, () => (staging = false));
      quietAt = T + 1600;
      break;
    }
    case 'partition': {
      let minority = [];
      let majoritySide = [];
      sim.at(100, () => {
        const shuffled = [...ids];
        for (let i = shuffled.length - 1; i > 0; i--) {
          const k = Math.floor(sim.rand() * (i + 1));
          [shuffled[i], shuffled[k]] = [shuffled[k], shuffled[i]];
        }
        minority = shuffled.slice(0, ids.length - majority).sort();
        majoritySide = shuffled.slice(ids.length - majority).sort();
        sim.partition([minority, majoritySide]);
        // Proposals on the minority side cannot succeed until the heal; on the majority side they must.
        minority.forEach((n, i) => propose(300 + i * 7, `c${i + 1}`, n, next(), 2500));
        majoritySide.forEach((n, i) => propose(320 + i * 7, `c${minority.length + i + 1}`, n, next()));
      });
      sim.at(2500, () => sim.heal());
      ids.forEach((n, i) => propose(3000 + i * 15, `c${i + 1}`, n, next()));
      quietAt = 2500;
      break;
    }
    case 'crash-promise': {
      // An acceptor's promise must survive a crash. n1 proposes X and n3
      // proposes Y; they cannot hear each other. n2 promises n1's ballot,
      // then n3's (higher) one, and crashes the instant it has answered n3.
      // Both phase 2 messages are held back until n2 is up again: n1's
      // first, then n3's. Once n1's has reached n2, nothing more from n1
      // does, so n2 never hears that X was chosen. An n2 that forgot its
      // promise accepts n1's lower ballot, so X is chosen (n1, n2), then
      // accepts n3's: Y is chosen too (n2, n3).
      const T = 300;
      const [A, B, C] = ids;
      const X = next();
      const Y = next();
      let staging = true;
      let crashed = false;
      let armed = false;
      sim.intercept = ({ src, dest, body, kind }) => {
        if (!staging || kind !== 'node') return undefined;
        if ((src === A && dest === C) || (src === C && dest === A)) return 'drop';
        if (src === A && dest === B) {
          if (armed) return 'drop';
          if (!crashed && mentions(body, X)) return 80;
        }
        if (crashed && src === C && dest === B && mentions(body, Y)) return 160;
        return undefined;
      };
      sim.onSend = (msg) => {
        if (!staging || crashed || msg.kind !== 'node' || msg.src !== B || msg.dest !== C || sim.now() < T + 20) return;
        crashed = true;
        crash([B]); // its promise to n3 is on the wire; whatever is not on disk is gone
        sim.at(sim.now() + 20, () => restart([B]));
      };
      sim.onDeliver = (msg) => {
        if (staging && crashed && msg.kind === 'node' && msg.src === A && msg.dest === B && mentions(msg.body, X)) armed = true;
      };
      propose(T, 'c1', A, X, T + 1500);
      propose(T + 20, 'c2', C, Y, T + 1500);
      sim.at(T + 1500, () => (staging = false));
      quietAt = T + 1500;
      break;
    }
    case 'full-restart': {
      // Everything crashes the instant the first propose_ok leaves, before
      // most nodes have learned the value. After the restart, new proposals
      // must still come back with that value.
      const T = 300;
      let down = false;
      ids.slice(0, 3).forEach((n, i) => propose(T + i * 3, `c${i + 1}`, n, next(), 1500));
      sim.onSend = (msg) => {
        if (down || msg.kind !== 'client' || !msg.body || msg.body.type !== 'propose_ok') return;
        down = true;
        const at = sim.now();
        crash(ids);
        sim.at(at + 400, () => restart(ids));
      };
      ids.forEach((n, i) => propose(2000 + i * 11, `c${i + 4}`, n, next()));
      quietAt = 2000;
      break;
    }
    case 'duel': {
      // Every node proposes at the same moment, twice over, on slow links.
      ids.forEach((n, i) => propose(300, `c${i + 1}`, n, next()));
      ids.forEach((n, i) => propose(3000, `c${i + 1}`, ids[(i + 1) % ids.length], next()));
      break;
    }
    default:
  }

  // Final inspection: every node must know the chosen value.
  const final = new Map();
  // Real processes can stall for a while behind fsyncs; give them longer.
  const finalAt = sim.duration - (sim.real ? 600 : 300);
  sim.at(finalAt, () => {
    for (const node of ids) {
      const life = lives.get(node);
      sim.clientRpc(node, { type: 'get' }, { probe: true, timeout: sim.real ? 500 : 250 }).then((res) => {
        final.set(node, res);
        if (res && res.type === 'get_ok') observe(node, finalAt, sim.now(), res.value ?? null, 'get', life);
      });
    }
  });

  return () => {
    const checks = [
      check('Every node answers get requests', !problem.silent, problem.silent),
      check('Replies are well formed', !problem.shape, problem.shape),
    ];
    if (problem.shape) return checks;

    const reports = seen.filter((s) => s.value !== null);
    const first = reports[0];
    const other = first && reports.find((s) => s.value !== first.value);
    const how = (s) => (s.how === 'get' ? 'in reply to get' : 'in a propose_ok');
    checks.push(
      check(
        'Agreement: no two nodes ever report different chosen values',
        !other,
        other &&
          `${first.node} reported ${j(first.value)} as chosen at ${ms(first.t)} (${how(first)}), but ${other.node} reported ${j(other.value)} at ${ms(other.t)} (${how(other)}). ` +
            'A value is chosen once a majority of acceptors accept it in one ballot. A later proposer must find it in the promises it collects (some acceptor in any majority has it) and propose that value instead of its own' +
            (scenario === 'crash-promise' || scenario === 'full-restart' ? '; and an acceptor must keep its promise and its accepted value on node.disk, written before it answers.' : '.'),
      ),
      check('A node never changes or forgets its chosen value', !problem.changed, problem.changed),
    );
    const fake = reports.find((s) => !proposed.has(s.value) || proposed.get(s.value) > s.t);
    checks.push(
      check(
        'Validity: the chosen value was proposed by a client',
        !fake,
        fake &&
          `${fake.node} reported ${j(fake.value)} at ${ms(fake.t)}, but ${proposed.has(fake.value) ? `no client proposed it before ${ms(proposed.get(fake.value))}` : `no client ever proposed it (proposed: ${j([...proposed.keys()])})`}.`,
      ),
    );

    const late = ops.find((o) => o.at === null || o.at > o.quiet + BOUND);
    checks.push(
      check(
        'Termination: every proposal is answered within 2s once the network is healthy',
        ops.length > 0 && !late,
        late
          ? late.at === null
            ? `${late.client} proposed ${j(late.value)} to ${late.first} at ${ms(late.invoke)} and, retrying every node in turn, never got a propose_ok. Keep retrying with higher ballots until a value is chosen, and back off for a random time after a failed round, so two proposers stop pre-empting each other.`
            : `${late.client} proposed ${j(late.value)} to ${late.first} at ${ms(late.invoke)}; the answer came only at ${ms(late.at)}, more than 2s after ${ms(late.quiet)}.`
          : 'No proposal was made.',
      ),
    );

    let missing = null;
    const chosen = first?.value;
    for (const node of ids) {
      const res = final.get(node);
      if (!res) {
        missing ??= `${node} did not answer the final get at ${ms(finalAt)}.`;
      } else if (chosen === undefined || (res.value ?? null) !== chosen) {
        missing ??=
          chosen === undefined
            ? 'No node ever reported a chosen value.'
            : `At ${ms(finalAt)} ${node} answered ${j(res)}; the chosen value is ${j(chosen)}. Every node must learn it within 2s of ${ms(Math.max(quietAt, first.t))}: tell the others when a value is chosen, and keep telling a node until it confirms.`;
      }
    }
    checks.push(check('The final inspection completed', final.size === ids.length, `Only ${final.size} of ${ids.length} nodes answered the final get.`));
    checks.push(check('Every node learns the chosen value', !missing, missing));
    return checks;
  };
}

// =================================================================
// Multi-Paxos: a replicated log
// =================================================================

const PATIENCE = 2000; // how long a client keeps retrying one append
const SETTLE = 2500; // how long the cluster gets to recover from a fault

// measure: { idle: [from, to], busy: [from, to], perFollower } turns on the
// stable-leader cost check: node-to-node messages per acknowledged append in
// the busy window, minus the background rate measured while idle, divided by
// the number of followers, must stay within perFollower.
export function paxosLog(sim, { scenario = 'stable', clients = 2, gap = 80, startAt = 1000, measure = null }) {
  const ids = sim.nodeIds;
  const canon = []; // { text, value, node, t }
  const lives = new Map(ids.map((n) => [n, 0]));
  const lastLen = new Map(); // node -> { len, t }
  const problem = { silent: null, shape: null, conflict: null, shrink: null, client: null };
  const quiet = []; // { from, until }: appends sent in here need not succeed
  const progress = []; // { from, to, what }: at least one append must be acknowledged in here
  const strict = (t) => t >= 1000 && quiet.every((q) => t + PATIENCE + 100 <= q.from || t >= q.until);

  const seeLog = (node, t, res, life) => {
    const sameLife = lives.get(node) === life;
    if (!res) {
      if (sameLife && sim.isUp(node)) problem.silent ??= `${node} did not answer { type: "log" } sent at ${ms(t)}.`;
      return;
    }
    if (res.type !== 'log_ok' || !Array.isArray(res.entries)) {
      problem.shape ??= `${node} replied ${j(res)} to { type: "log" }; expected { type: "log_ok", entries: [...] } with the values chosen for slots 1, 2, ... in order (null for a no-op).`;
      return;
    }
    const entries = res.entries;
    for (let i = 0; i < entries.length; i++) {
      const text = JSON.stringify(entries[i] ?? null);
      if (i < canon.length) {
        if (canon[i].text !== text && !problem.conflict) {
          const c = canon[i];
          problem.conflict = `Slot ${i + 1}: ${c.node} reported ${c.text} as chosen at ${ms(c.t)}, but at ${ms(t)} ${node} reports ${text}. A new leader must run phase 1 and re-propose, for every slot, the accepted value with the highest ballot it hears of; only slots where it hears of none may get a new command or a no-op.`;
        }
      } else {
        canon.push({ text, value: entries[i] ?? null, node, t });
      }
    }
    if (!sameLife) return;
    // Replies can overtake each other: only a request sent after a longer
    // answer arrived may not be answered with a shorter log.
    const prev = lastLen.get(node);
    if (prev && entries.length < prev.len && t > prev.t) {
      problem.shrink ??= `${node} reported ${prev.len} chosen slots at ${ms(prev.t)} and only ${entries.length} at ${ms(t)}, without crashing in between.`;
    }
    if (!prev || entries.length >= prev.len) lastLen.set(node, { len: entries.length, t: sim.now() });
  };

  const probe = (node) => {
    if (!sim.isUp(node)) return;
    const t = sim.now();
    const life = lives.get(node);
    sim.clientRpc(node, { type: 'log' }, { probe: true, timeout: PROBE_TIMEOUT(sim) }).then((res) => seeLog(node, t, res, life));
  };
  for (let t = 100; t < sim.duration - 400; t += 100) sim.at(t, () => ids.forEach(probe));

  const crash = (nodes) => {
    for (const n of nodes) {
      sim.crash(n);
      lives.set(n, lives.get(n) + 1);
      lastLen.delete(n);
    }
  };
  const restart = (nodes) => {
    for (const n of nodes) {
      sim.restart(n);
      probe(n);
    }
  };
  const permanentlyDown = new Set();

  // ------------------------------------------------------------- clients

  const ops = [];
  let counter = 0;
  let known = null; // the node that last acknowledged an append

  function newOp(client) {
    const value = ++counter;
    const op = { client, value, body: { type: 'append', value }, invoke: sim.now(), acked: null, node: null };
    ops.push(op);
    return op;
  }

  const nextNode = (from, avoid) => {
    const start = ids.indexOf(from);
    const ring = ids.map((_, i) => ids[(start + 1 + i) % ids.length]).filter((n) => sim.isUp(n));
    const free = ring.find((n) => !avoid.has(n));
    if (free) return free;
    avoid.clear();
    return ring[0] ?? from;
  };

  // Follow not_leader hints, move on after 500ms of silence, give up after PATIENCE.
  async function submit(client, body) {
    const started = sim.now();
    const avoid = new Set();
    let target = known && sim.isUp(known) ? known : nextNode(ids[ids.length - 1], avoid);
    while (sim.now() - started < PATIENCE) {
      const res = await sim.clientRpc(target, body, { client, timeout: 500 });
      if (res && res.type === 'append_ok') {
        known = target;
        return target;
      }
      avoid.add(target);
      if (res && res.type === 'not_leader') {
        const hint = res.leader;
        target = typeof hint === 'string' && ids.includes(hint) && sim.isUp(hint) && !avoid.has(hint) ? hint : nextNode(target, avoid);
        await sim.sleep(25);
      } else if (res) {
        problem.client ??= `${target} replied ${j(res)} to ${j(body)}; expected { type: "append_ok" } or { type: "not_leader", leader }.`;
        return null;
      } else {
        target = nextNode(target, avoid);
      }
    }
    return null;
  }

  // Appends sent straight to a leader that has just been cut off.
  const strand = (node, count) => {
    for (let i = 0; i < count; i++) {
      sim.at(sim.now() + 150 + i * 120, async () => {
        const op = newOp('c9');
        const res = await sim.clientRpc(node, op.body, { client: 'c9', timeout: 5000 });
        if (res && res.type === 'append_ok') {
          op.acked = sim.now();
          op.node = node;
        }
      });
    }
  };

  const leaderNow = () => (known && sim.isUp(known) ? known : ids.find((n) => sim.isUp(n)));
  const followers = (leader) => {
    const rest = ids.filter((n) => n !== leader);
    for (let i = rest.length - 1; i > 0; i--) {
      const k = Math.floor(sim.rand() * (i + 1));
      [rest[i], rest[k]] = [rest[k], rest[i]];
    }
    return rest;
  };

  // -------------------------------------------------------------- faults

  switch (scenario) {
    case 'leader-crash': {
      // The leader crashes the instant it acknowledges an append, before it
      // can tell anyone that the slot is chosen. It does not come back.
      const T = 2500;
      let armed = false;
      let L = null;
      sim.at(T, () => {
        L = leaderNow();
        armed = true;
      });
      sim.onSend = (msg) => {
        if (!armed || msg.kind !== 'client' || msg.src !== L || msg.body?.type !== 'append_ok') return;
        armed = false;
        crash([L]);
        permanentlyDown.add(L);
        quiet.push({ from: sim.now(), until: sim.now() + SETTLE });
        progress.push({ from: sim.now() + SETTLE, to: sim.duration - PATIENCE - 400, what: 'the leader crash' });
      };
      sim.at(T + 1000, () => {
        if (!armed) return;
        armed = false;
        crash([L]);
        permanentlyDown.add(L);
        quiet.push({ from: sim.now(), until: sim.now() + SETTLE });
      });
      break;
    }
    case 'cut-leader': {
      sim.at(2500, () => {
        const L = leaderNow();
        sim.partition([[L, followers(L)[0]], ids.filter((n) => n !== L && n !== followers(L)[0])]);
        strand(L, 3);
      });
      sim.at(6000, () => sim.heal());
      quiet.push({ from: 2500, until: 6000 + SETTLE });
      progress.push({ from: 2500 + SETTLE, to: 6000, what: 'the partition at 2500ms' });
      break;
    }
    case 'follower-cut': {
      sim.at(2000, () => {
        const L = leaderNow();
        const F = followers(L)[0];
        sim.partition([[F], ids.filter((n) => n !== F)]);
      });
      sim.at(5000, () => sim.heal());
      break;
    }
    case 'follower-restart': {
      let F = null;
      sim.at(2500, () => {
        F = followers(leaderNow())[0];
        crash([F]);
      });
      sim.at(4000, () => restart([F]));
      break;
    }
    case 'leader-restart': {
      let L = null;
      sim.at(2500, () => {
        L = leaderNow();
        crash([L]);
      });
      sim.at(3500, () => restart([L]));
      quiet.push({ from: 2500, until: 2500 + SETTLE });
      break;
    }
    case 'full-restart': {
      sim.at(3000, () => crash(ids));
      sim.at(3500, () => restart(ids));
      quiet.push({ from: 3000, until: 3500 + SETTLE });
      break;
    }
    default:
  }

  // Stable-leader cost: count node-to-node messages in the idle and busy windows.
  const cost = measure && { idle: 0, busy: 0 };
  if (measure) {
    const inside = (w, t) => t >= w[0] && t < w[1];
    sim.onSend = (msg) => {
      if (msg.kind !== 'node') return;
      const t = sim.now();
      if (inside(measure.idle, t)) cost.idle++;
      else if (inside(measure.busy, t)) cost.busy++;
    };
  }

  const stopAt = measure ? measure.busy[1] : sim.duration - PATIENCE - 400;
  for (let c = 1; c <= clients; c++) {
    const client = `c${c}`;
    sim.at(startAt + c * 31, async () => {
      while (sim.now() < stopAt) {
        const op = newOp(client);
        const node = await submit(client, op.body);
        if (node) {
          op.acked = sim.now();
          op.node = node;
        }
        await sim.sleep(gap * (0.5 + sim.rand()));
      }
    });
  }

  // ------------------------------------------------------ final snapshot

  const final = new Map();
  const finalAt = sim.duration - (sim.real ? 600 : 250);
  sim.at(finalAt, () => {
    for (const node of ids) {
      if (!sim.isUp(node)) continue;
      const life = lives.get(node);
      sim.clientRpc(node, { type: 'log' }, { probe: true, timeout: sim.real ? 500 : 200 }).then((res) => {
        seeLog(node, finalAt, res, life);
        final.set(node, res);
      });
    }
  });

  return () => {
    const done = ops.filter((o) => o.acked !== null);
    const checks = [
      check('Every node answers log requests', !problem.silent, problem.silent),
      check('Replies are well formed', !problem.shape && !problem.client, problem.shape ?? problem.client),
    ];
    if (problem.shape || problem.client) return checks;

    const first = new Map();
    canon.forEach((c, i) => {
      if (c.value !== null && !first.has(c.value)) first.set(c.value, i + 1);
    });

    checks.push(
      check('No two nodes ever report different values for one slot', !problem.conflict, problem.conflict),
      check('A node\'s log only grows', !problem.shrink, problem.shrink),
    );

    const strictOps = ops.filter((o) => o.client !== 'c9' && strict(o.invoke));
    const stuck = strictOps.filter((o) => o.acked === null);
    checks.push(
      check(
        'Every append is acknowledged within 2s while the cluster is healthy',
        strictOps.length > 0 && stuck.length === 0,
        stuck.length
          ? `${plural(stuck.length, 'append')} of ${strictOps.length} got no append_ok. First: ${j(stuck[0].body)}, sent at ${ms(stuck[0].invoke)}, retried on every node for 2s.${done.length ? '' : ' No append was ever acknowledged.'}`
          : 'No append was sent.',
      ),
    );
    for (const w of progress) {
      const got = done.some((o) => o.acked > w.from && o.acked < w.to);
      checks.push(
        check(
          `Appends are acknowledged again within 2.5s of ${w.what}`,
          got,
          `No append was acknowledged between ${ms(w.from)} and ${ms(w.to)}, although a majority could talk after ${w.what}. A node that stops hearing from the leader must run phase 1 with a higher ballot and take over.`,
        ),
      );
    }

    const lost = done.filter((o) => !first.has(o.value));
    checks.push(
      check(
        'Every acknowledged append is in the log',
        lost.length === 0,
        lost.length &&
          `${plural(lost.length, 'acknowledged append')} of ${done.length} ${lost.length === 1 ? 'is' : 'are'} missing. First: ${lost[0].node} acknowledged ${j(lost[0].body)} at ${ms(lost[0].acked)}, but no node ever reported it in a slot. Acknowledge only once the slot is chosen (accepted by a majority), and have a new leader re-propose what acceptors have accepted.`,
      ),
    );

    let swapped = null;
    const prevBy = new Map();
    for (const o of done) {
      if (!first.has(o.value)) continue;
      const p = prevBy.get(o.client);
      if (p && first.get(o.value) < first.get(p.value)) {
        swapped ??= `${o.client} sent ${j(p.body)} and, after it was acknowledged, ${j(o.body)}; they are in slots ${first.get(p.value)} and ${first.get(o.value)}.`;
      }
      prevBy.set(o.client, o);
    }
    checks.push(check('Appends from one client stay in the order they were acknowledged', !swapped, swapped));

    let behind = null;
    const up = ids.filter((n) => !permanentlyDown.has(n));
    for (const node of up) {
      const res = final.get(node);
      if (!res || !Array.isArray(res.entries)) {
        behind ??= `${node} did not answer the final log request at ${ms(finalAt)}.`;
        continue;
      }
      if (res.entries.length < canon.length) {
        behind ??= `${node} knows ${res.entries.length} of ${canon.length} chosen slots at the end. Every node must learn every chosen slot, including ones whose announcement it missed: a node that sees it is behind must ask for what it lacks.`;
      }
    }
    checks.push(check('Every running node ends with the whole log', canon.length > 0 && !behind, behind ?? 'No slot was ever reported as chosen.'));
    if (measure) {
      const span = (w) => w[1] - w[0];
      const appends = done.filter((o) => o.acked >= measure.busy[0] && o.acked < measure.busy[1]).length;
      const background = (cost.idle / span(measure.idle)) * span(measure.busy);
      const followers = ids.length - 1;
      const perFollower = appends ? Math.max(0, cost.busy - background) / appends / followers : Infinity;
      checks.push(
        check(
          `A stable leader spends at most ${measure.perFollower} messages per follower on each append`,
          appends > 0 && perFollower <= measure.perFollower,
          appends === 0
            ? 'No append was acknowledged while the cost was being measured.'
            : `Each acknowledged append cost about ${perFollower.toFixed(1)} messages per follower (${cost.busy} node messages for ${appends} appends between ${ms(measure.busy[0])} and ${ms(measure.busy[1])}, after subtracting ${Math.round(background)} for the background traffic measured while idle). Phase 2 alone needs 2 (accept and accepted), 3 with a decide notice. Running phase 1 again for each command doubles that: a stable leader that has already won phase 1 for every slot should go straight to phase 2.`,
        ),
      );
    }
    return checks;
  };
}
