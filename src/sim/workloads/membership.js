// Workloads for the gossip-membership (SWIM) track.
//
// Every node keeps a view of the cluster. The harness never looks at the
// node-to-node protocol; it polls every node with a `members` request every
// 100ms (probe traffic: kept out of the timeline and never dropped) and judges
// the views it gets back:
//
//   → { type: 'members' }
//   ← { type: 'members_ok', members: { n1: 'alive', n2: 'suspect', n3: 'dead' } }
//
// Faults are staged by the harness: crashes (sometimes followed by a
// restart), one link that drops everything, a short partition isolating one
// node, and random loss set by the test's dropRate. Every node message sent
// is counted per sender for the message budget.

import { check, j, plural } from '../workloads.js';

const STATUSES = ['alive', 'suspect', 'dead'];
const POLL_EVERY = 100;
const POLL_TIMEOUT = 300;
const WARM = 600; // ms before false alarms count
const ms = (t) => `${Math.round(t)}ms`;
const s = (t) => `${(t / 1000).toFixed(1)}s`;

// params:
//   mode       'local' (tasks 1-2: a failure shows as anything but 'alive')
//              'dead'  (tasks 3-4: a failure must show as 'dead')
//   budget     max node messages per node per second, averaged over the run
//   crashes    [{ at, restart }]: a random distinct node crashes at `at` (and restarts at `restart`)
//   detect     ms after a crash by which every live node must report the crashed node
//   spread     ms after the first 'dead' report by which every live node reports it (mode 'dead')
//   rejoin     ms after a restart by which every node reports the restarted node 'alive'
//   brokenLink true: two random nodes cannot exchange any message, for the whole run
//   isolate    [start, end]: one random node is cut off from everyone in that window
//   refute     ms after the isolation ends by which every view is all 'alive' again
export function swim(sim, params) {
  const { mode = 'local', budget = 20, crashes = [], detect = 3000, spread = null, rejoin = 2500, brokenLink = false, isolate = null, refute = 2500 } = params;
  const ids = sim.nodeIds;
  const shuffled = [...ids];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const k = Math.floor(sim.rand() * (i + 1));
    [shuffled[i], shuffled[k]] = [shuffled[k], shuffled[i]];
  }
  let next = 0;
  const take = () => shuffled[next++];
  const victims = crashes.map((c) => ({ ...c, node: take() }));
  const link = brokenLink ? [take(), take()].sort() : null;
  const lonely = isolate ? take() : null;
  const troubled = new Set(victims.map((v) => v.node));
  const steady = ids.filter((n) => !troubled.has(n)); // never crash: must never be called dead
  const faultTimes = new Map(ids.map((n) => [n, []]));

  // ---- faults
  for (const v of victims) {
    sim.at(v.at, () => {
      sim.crash(v.node);
      faultTimes.get(v.node).push(sim.now());
    });
    if (v.restart != null) {
      sim.at(v.restart, async () => {
        faultTimes.get(v.node).push(sim.now());
        await sim.restart(v.node);
        v.booted = sim.now();
      });
    }
  }
  if (link) {
    sim.intercept = ({ src, dest, kind }) => (kind === 'node' && ((src === link[0] && dest === link[1]) || (src === link[1] && dest === link[0])) ? 'drop' : undefined);
  }
  if (isolate) {
    sim.at(isolate[0], () => sim.partition([[lonely], ids.filter((n) => n !== lonely)]));
    sim.at(isolate[1], () => sim.heal());
  }

  // ---- message budget
  const sent = new Map(ids.map((n) => [n, 0]));
  sim.onSend = (msg) => {
    if (msg.kind === 'node') sent.set(msg.src, sent.get(msg.src) + 1);
  };

  // ---- polling
  const polls = []; // { t, views: { node: members } }
  let malformed = null;
  let silent = null;
  const excused = (n, at) => faultTimes.get(n).some((t) => t > at - 1500 && t < at + POLL_TIMEOUT + 200);
  const poll = async () => {
    const at = sim.now();
    const up = ids.filter((n) => sim.isUp(n));
    const replies = await Promise.all(up.map((n) => sim.clientRpc(n, { type: 'members' }, { probe: true, timeout: POLL_TIMEOUT })));
    const views = {};
    replies.forEach((res, i) => {
      const n = up[i];
      if (!res) {
        if (sim.isUp(n) && !excused(n, at)) silent ??= { node: n, t: at };
        return;
      }
      const m = res.members;
      const bad = res.type !== 'members_ok' || !m || typeof m !== 'object' || Array.isArray(m) ? 'shape' : ids.find((id) => !STATUSES.includes(m[id]));
      if (bad) {
        malformed ??=
          bad === 'shape'
            ? `${n} replied ${j(res)}; expected { type: "members_ok", members: { n1: "alive", ... } }.`
            : `${n} reported ${j(m[bad])} for ${bad} in ${j(m)}; every node id, itself included, must map to "alive", "suspect" or "dead".`;
        return;
      }
      views[n] = m;
    });
    polls.push({ t: at, views });
  };
  for (let t = POLL_EVERY; t <= sim.duration - POLL_TIMEOUT - 100; t += POLL_EVERY) sim.at(t, poll);

  const failed = mode === 'dead' ? (st) => st === 'dead' : (st) => st !== 'alive';
  const failWord = mode === 'dead' ? '"dead"' : '"suspect" (or "dead")';

  return () => {
    const checks = [
      check('Every node answers members', !silent && polls.length > 0, silent ? `${silent.node} did not answer a members request sent at ${ms(silent.t)} within ${POLL_TIMEOUT}ms. Answer it right away from your current view; never wait on the network inside that handler.` : 'No members replies at all.'),
      check('members_ok maps every node id to alive, suspect or dead', !malformed, malformed),
    ];
    if (malformed || !polls.length) return checks;

    // Accuracy: a node that never crashed must never be reported as failed.
    // While one node is cut off, it may suspect everyone and everyone may
    // suspect it; only "dead" counts then.
    const isolatedAt = (t) => isolate && t >= isolate[0] && t < isolate[1] + refute;
    let falseAlarm = null;
    for (const p of polls) {
      if (p.t < WARM) continue;
      for (const [viewer, m] of Object.entries(p.views)) {
        for (const subject of steady) {
          if (subject === viewer) continue;
          const st = m[subject];
          const wrong = mode === 'dead' || isolatedAt(p.t) ? st === 'dead' : st !== 'alive';
          if (wrong) {
            falseAlarm = { t: p.t, viewer, subject, st };
            break;
          }
        }
        if (falseAlarm) break;
      }
      if (falseAlarm) break;
    }
    const why =
      mode === 'dead'
        ? isolate
          ? 'A node that was only unreachable for a moment must be able to refute the suspicion before anyone gives up on it: is your suspicion timeout long enough, and does the suspect hear about it?'
          : 'Only a failed direct probe followed by failed indirect probes should declare a node dead.'
        : link
          ? `The link ${link[0]} ↔ ${link[1]} drops every message, but both nodes are alive and every other node can reach them. Before suspecting, ask a few other members to ping the target for you.`
          : params.lossy
            ? 'On a lossy network one missing ack proves nothing: ask a few other members to ping the target before suspecting it.'
            : 'Every node was up and reachable. Is your ack timeout long enough, and do you clear a suspicion when a later probe succeeds?';
    checks.push(
      check(
        mode === 'dead' ? 'No live node is ever reported dead' : 'No false suspicions of live nodes',
        !falseAlarm,
        falseAlarm && `At ${ms(falseAlarm.t)} ${falseAlarm.viewer} reported ${falseAlarm.subject} as "${falseAlarm.st}", but ${falseAlarm.subject} never crashed. ${why}`,
      ),
    );

    // Completeness: each crashed node is reported failed by every live node
    // within `detect` ms, until it restarts.
    for (const v of victims) {
      const from = v.at + detect;
      const until = v.restart ?? Infinity;
      const window = polls.filter((p) => p.t >= from && p.t < until);
      let miss = null;
      for (const p of window) {
        for (const [viewer, m] of Object.entries(p.views)) {
          if (viewer === v.node) continue;
          if (!failed(m[v.node])) {
            miss = { t: p.t, viewer, st: m[v.node] };
            break;
          }
        }
        if (miss) break;
      }
      const hint =
        mode === 'dead'
          ? spread != null && !isolate
            ? 'The first node to notice should tell the others by piggybacking the news on the pings and acks it already sends.'
            : 'Suspect it when probes fail, then declare it dead when the suspicion timeout expires without a refutation.'
          : `Probe every member regularly: going round a shuffled list of peers guarantees each one is probed at least once every ${2 * (ids.length - 1) - 1} periods.`;
      checks.push(
        check(
          `The crashed node (${v.node}) is reported ${failWord} everywhere within ${s(detect)}`,
          window.length >= 2 && !miss,
          window.length < 2
            ? `Only ${plural(window.length, 'members poll')} ran between ${ms(from)} and ${ms(Math.min(until, sim.duration))}; the final inspection did not complete.`
            : !miss ? "" : `${v.node} crashed at ${ms(v.at)}; at ${ms(miss.t)} ${miss.viewer} still reported it as ${j(miss.st)}. ${hint}`,
        ),
      );

      // Dissemination: once any node says dead, all say dead soon after.
      if (spread != null) {
        const first = polls.find((p) => p.t >= v.at && p.t < until && Object.entries(p.views).some(([viewer, m]) => viewer !== v.node && m[v.node] === 'dead'));
        if (first) {
          const firstBy = Object.entries(first.views).find(([viewer, m]) => viewer !== v.node && m[v.node] === 'dead')[0];
          const late = polls
            .filter((p) => p.t >= first.t + spread && p.t < until)
            .flatMap((p) => Object.entries(p.views).filter(([viewer, m]) => viewer !== v.node && m[v.node] !== 'dead').map(([viewer, m]) => ({ t: p.t, viewer, st: m[v.node] })))[0];
          checks.push(
            check(
              `News of ${v.node}'s death reaches every node within ${s(spread)}`,
              !late,
              late &&
                `${firstBy} reported ${v.node} dead at ${ms(first.t)}, but at ${ms(late.t)} ${late.viewer} still said ${j(late.st)}. Each update should ride on about ${3 * Math.ceil(Math.log2(ids.length + 1))} outgoing messages before you drop it, so it reaches everyone in a few rounds.`,
            ),
          );
        }
      }

      // Rejoin: a restarted node is alive again everywhere.
      if (v.restart != null) {
        const after = polls.filter((p) => p.t >= v.restart + rejoin);
        let stale = null;
        for (const p of after) {
          for (const [viewer, m] of Object.entries(p.views)) {
            if (viewer !== v.node && m[v.node] !== 'alive') {
              stale = { t: p.t, viewer, st: m[v.node] };
              break;
            }
          }
          if (stale) break;
        }
        checks.push(
          check(
            `${v.node} is alive again everywhere within ${s(rejoin)} of restarting`,
            after.length >= 2 && !stale,
            after.length < 2
              ? `Only ${plural(after.length, 'members poll')} ran after ${ms(v.restart + rejoin)}; the final inspection did not complete.`
              : !stale ? "" : `${v.node} restarted at ${ms(v.restart)}; at ${ms(stale.t)} ${stale.viewer} still reported it as ${j(stale.st)}. Everyone else remembers it as dead at its old incarnation: it must come back with a higher one (keep it on node.disk) and announce itself as alive.`,
          ),
        );
      }
    }

    // Refutation: after a short isolation, everyone is alive everywhere again.
    if (isolate) {
      const from = isolate[1] + refute;
      const after = polls.filter((p) => p.t >= from);
      let stale = null;
      for (const p of after) {
        for (const [viewer, m] of Object.entries(p.views)) {
          const subject = steady.find((n) => n !== viewer && m[n] !== 'alive');
          if (subject) {
            stale = { t: p.t, viewer, subject, st: m[subject] };
            break;
          }
        }
        if (stale) break;
      }
      checks.push(
        check(
          `Every view is all "alive" again within ${s(refute)} of the network healing`,
          after.length >= 2 && !stale,
          after.length < 2
            ? `Only ${plural(after.length, 'members poll')} ran after ${ms(from)}; the final inspection did not complete.`
            : !stale ? "" : `${lonely} was cut off from ${ms(isolate[0])} to ${ms(isolate[1])}. At ${ms(stale.t)} ${stale.viewer} still reported ${stale.subject} as "${stale.st}". A suspected node that hears the rumour about itself must refute it with a higher incarnation, and that alive update must spread.`,
        ),
      );
    }

    // Message budget.
    const secs = sim.duration / 1000;
    const worst = [...sent.entries()].sort((a, b) => b[1] - a[1])[0];
    const rate = worst[1] / secs;
    checks.push(
      check(
        `Each node sends at most ${budget} messages per second`,
        rate <= budget,
        `${worst[0]} sent ${worst[1]} messages in ${s(sim.duration)}: ${rate.toFixed(1)} per second, over the budget of ${budget}. Heartbeating every peer costs (N-1) messages per beat and grows with the cluster; probe one member per period instead, so the cost per node stays the same however big the cluster gets.`,
      ),
    );
    return checks;
  };
}

export const swimProbe = (sim, params) => swim(sim, { ...params, mode: 'local' });
export const swimIndirect = (sim, params) => swim(sim, { ...params, mode: 'local' });
export const swimGossip = (sim, params) => swim(sim, { ...params, mode: 'dead' });
export const swimSuspicion = (sim, params) => swim(sim, { ...params, mode: 'dead' });
