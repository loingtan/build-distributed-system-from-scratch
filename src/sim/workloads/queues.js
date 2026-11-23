// Workload for the message-queue track. The first node is the broker; the
// harness plays producers (enqueue) and consumers (dequeue, work, ack).
//
// Everything is judged on the broker's own clock: the checker watches the
// replies the broker sends (sim.onSend), so network latency never blurs a
// timing rule.
//
// params:
//   jobs           how many jobs the producers enqueue
//   burst          jobs arrive in groups of this size (default 1: evenly spread)
//   consumers      how many consumers poll (default 3)
//   work           [min, max] ms a consumer spends on a job (default [20, 120])
//   ack            consumers acknowledge; unacknowledged jobs must be redelivered
//   failRate       chance that a job's consumer crashes (once, sometimes twice) before acking
//   retries        producers send some enqueues two or three times; some acks are sent twice
//   slowRate       chance that a consumer takes longer than the visibility timeout, then acks
//   lateAck        scripted opening: a job is acked after its timeout, while it waits in the queue
//   maxDeliveries  a job handed out this many times without an ack must move to the dead letters
//   poison         how many jobs are never acknowledged by anyone
//   crashes        times at which the broker crashes; it restarts 150ms later

import { check, j, plural, sample, unanswered } from '../workloads.js';

const VISIBILITY = 500; // ms a handed-out job stays invisible
const EARLY = 450; // handing it out again sooner than this is a failure
const LATE = 750; // refusing to hand it out later than this is a failure
const DOWN = 150; // ms the broker stays down after a crash

export function queue(sim, params = {}) {
  const {
    jobs: jobCount = 40,
    burst = 1,
    consumers = 3,
    work = [20, 120],
    ack: ackMode = false,
    failRate = 0,
    retries = false,
    slowRate = 0,
    lateAck = false,
    maxDeliveries = 0,
    poison = 0,
    crashes = [],
  } = params;

  const broker = sim.nodeIds[0];
  const crashMode = crashes.length > 0;
  const TIMEOUT = 300;
  const ms = (t) => `${Math.round(t)}ms`;
  const between = ([lo, hi]) => lo + sim.rand() * (hi - lo);

  // ---- what the checker knows

  const jobs = new Map(); // id -> { id, payload, copies, enq, deliveries, ack }
  const reqs = new Map(); // msg_id -> request body
  const handouts = []; // every dequeue_ok that carried a job, in broker order
  const nulls = []; // every dequeue_ok with job: null
  const results = []; // every client request { node, req, res, invoke }
  const downs = []; // [crashTime, restartTime]
  const got = new Map(); // id -> how many times consumers received it
  let deadReply;
  let malformed = null;
  let seq = 0;

  const register = (id, payload, fails) => {
    jobs.set(id, { id, payload, fails, copies: 0, enq: null, deliveries: [], ack: null });
    return jobs.get(id);
  };

  sim.onSend = (msg) => {
    const b = msg.body;
    if (msg.dest === broker && msg.kind === 'client') {
      reqs.set(b.msg_id, b);
      return;
    }
    if (msg.src !== broker) return;
    const req = reqs.get(b.in_reply_to);
    if (!req) return;
    const at = { t: sim.now(), s: seq++ };
    if (req.type === 'enqueue' && b.type === 'enqueue_ok') {
      const job = jobs.get(req.id);
      if (job && !job.enq) job.enq = at;
    } else if (req.type === 'ack' && b.type === 'ack_ok') {
      const job = jobs.get(req.id);
      if (job && !job.ack) job.ack = at;
    } else if (req.type === 'dequeue' && b.type === 'dequeue_ok') {
      if (b.job === null) nulls.push(at);
      else if (b.job && typeof b.job === 'object' && !Array.isArray(b.job)) handouts.push({ ...at, id: b.job.id, payload: b.job.payload, to: msg.dest });
      else malformed ??= `The broker replied ${j(b)} to a dequeue; job must be { id, payload } or null.`;
    }
  };

  // ---- clients

  // One request, repeated while the broker is silent (only producers and
  // acking consumers repeat, and only in the crash tests).
  const request = async (body, client, tries = 1) => {
    let res = null;
    for (let i = 0; i < tries && !res; i++) {
      const entry = { node: broker, req: body, res: null, invoke: sim.now() };
      results.push(entry);
      res = entry.res = await sim.clientRpc(broker, body, { timeout: TIMEOUT, client });
    }
    return res;
  };
  const persist = crashMode ? 6 : 1;

  const acknowledge = async (id, client) => {
    await request({ type: 'ack', id }, client, persist);
    if (retries && sim.rand() < 0.3) {
      await sim.sleep(between([10, 60]));
      await request({ type: 'ack', id }, client, persist);
    }
  };

  // ---- timeline

  const start = lateAck ? 950 : 100;
  const cycles = !ackMode ? 0 : poison > 0 ? Math.max(maxDeliveries, 2) : 2;
  const drain = !ackMode ? 800 : cycles * (LATE + 100) + 450 + (slowRate > 0 ? 400 : 0);
  const stopAt = sim.duration - 300;
  const prodEnd = stopAt - drain;
  const gap = (prodEnd - start) / jobCount;

  const poisonEvery = poison > 0 ? Math.floor(jobCount / poison) : 0;
  for (let i = 0; i < jobCount; i++) {
    const isPoison = poison > 0 && i % poisonEvery === Math.floor(poisonEvery / 3) && i / poisonEvery < poison;
    let fails = 0;
    if (isPoison) fails = Infinity;
    else if (ackMode && sim.rand() < failRate) fails = sim.rand() < 0.3 ? 2 : 1;
    const payload = isPoison ? { task: 'resize', photo: i + 1, corrupt: true } : { task: 'resize', photo: i + 1 };
    const job = register(`job-${i + 1}`, payload, fails);
    const when = start + Math.floor(i / burst) * burst * gap + (i % burst) * 2;
    const copies = !retries || sim.rand() < 0.5 ? 1 : sim.rand() < 0.6 ? 2 : 3;
    for (let c = 0; c < copies; c++) {
      // A retry follows closely, or arrives long after the job has been processed.
      const delay = c === 0 ? 0 : sim.rand() < 0.5 ? between([15, 75]) * c : between([400, 900]);
      sim.at(when + delay, async () => {
        job.copies++;
        await request({ type: 'enqueue', id: job.id, payload }, `c${7 + (i % 2)}`, persist);
      });
    }
  }

  if (lateAck) {
    // A consumer takes a job, stalls past the visibility timeout while nobody
    // else is polling, then acknowledges. The job is back in the queue by then.
    const slow = register('job-slow', { task: 'resize', photo: 0 }, 0);
    sim.at(40, async () => {
      slow.copies++;
      await request({ type: 'enqueue', id: slow.id, payload: slow.payload }, 'c7');
    });
    sim.at(100, async () => {
      const res = await request({ type: 'dequeue' }, 'c9');
      if (!res || !res.job || res.job.id !== slow.id) return;
      got.set(slow.id, 1);
      await sim.sleep(680);
      await request({ type: 'ack', id: slow.id }, 'c9');
    });
  }

  const consumer = async (client) => {
    while (sim.now() < stopAt) {
      const res = await request({ type: 'dequeue' }, client);
      const job = res && res.type === 'dequeue_ok' ? res.job : null;
      if (!job || typeof job !== 'object' || job.id === undefined) {
        await sim.sleep(between([25, 55]));
        continue;
      }
      const n = (got.get(job.id) ?? 0) + 1;
      got.set(job.id, n);
      if (!ackMode) {
        await sim.sleep(between(work));
        continue;
      }
      const fails = jobs.get(job.id)?.fails ?? 0;
      if (n <= fails) {
        // The consumer crashes in the middle of the job. It never acks, and
        // a replacement starts polling shortly after.
        await sim.sleep(between(work) + 60);
        continue;
      }
      if (sim.rand() < slowRate) await sim.sleep(between([VISIBILITY + 100, VISIBILITY + 350]));
      else await sim.sleep(between(work));
      await acknowledge(job.id, client);
    }
  };
  for (let c = 0; c < consumers; c++) sim.at(start + c * 7, () => consumer(`c${c + 1}`));

  for (const t of crashes) {
    // Just before the crash a consumer takes a job and then dies with the
    // broker, so there is always work in flight when the lights go out.
    sim.at(t - 25, async () => {
      const res = await request({ type: 'dequeue' }, 'c9');
      if (res && res.job && res.job.id !== undefined) got.set(res.job.id, (got.get(res.job.id) ?? 0) + 1);
    });
    sim.at(t, () => {
      sim.crash(broker);
      downs.push([t, t + DOWN]);
    });
    sim.at(t + DOWN, () => sim.restart(broker));
  }

  if (maxDeliveries) {
    sim.at(sim.duration - 200, async () => {
      deadReply = await request({ type: 'dead_letters' }, 'c6');
    });
  }

  // ---- checks

  return () => {
    // A request sent while the broker was down (or about to go down) cannot be answered.
    // Real processes need a few ms (and an fsync) to handle a request, so with the
    // local runner a request sent shortly before the kill may still be in progress.
    const margin = sim.real ? 100 : 20;
    const excused = (r) => downs.some(([a, b]) => r.invoke >= a - margin && r.invoke <= b);
    const crashBetween = (a, b) => downs.some(([c]) => c > a && c < b);
    const restartBefore = (t) => Math.max(-Infinity, ...downs.map(([, b]) => b).filter((b) => b <= t));
    const same = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

    const expected = { enqueue: 'enqueue_ok', dequeue: 'dequeue_ok', ack: 'ack_ok', dead_letters: 'dead_letters_ok' };
    const badType = results.find((r) => r.res && r.res.type !== expected[r.req.type]);
    const noJobField = results.find((r) => r.res && r.res.type === 'dequeue_ok' && r.res.job === undefined);
    if (noJobField) malformed ??= `The broker replied ${j(noJobField.res)} to a dequeue; say job: null when there is nothing to hand out.`;

    const checks = [
      unanswered(
        crashMode ? 'Every request gets a reply while the broker is up' : 'Every request gets a reply',
        results.filter((r) => !excused(r)),
        'Reply to every enqueue, dequeue and ack, including the ones that change nothing.',
      ),
      check('Replies are enqueue_ok / dequeue_ok / ack_ok', !badType, badType && `The broker replied ${j(badType.res)} to ${j(badType.req)}.`),
      check('dequeue_ok carries job: { id, payload } or job: null', !malformed, malformed),
    ];

    // Walk the hand-outs in the order the broker made them.
    let invented = null;
    let disorder = null;
    let twice = null;
    let early = null;
    let afterAck = null;
    let over = null;
    let newest = null; // the latest-enqueued job that has had its first hand-out
    for (const d of handouts) {
      const job = jobs.get(d.id);
      if (!job) {
        invented ??= `At ${ms(d.t)} the broker handed ${j({ id: d.id, payload: d.payload })} to ${d.to}, but no job with that id was ever enqueued.`;
        continue;
      }
      if (!same(job.payload, d.payload)) {
        invented ??= `${job.id} was enqueued with payload ${j(job.payload)} but handed out with ${j(d.payload)}.`;
      }
      const prev = job.deliveries[job.deliveries.length - 1];
      const copies = job.copies > 1 ? ` The producer sent the enqueue for ${job.id} ${job.copies} times (retries with the same id): did a retry create a second copy?` : '';
      if (!prev) {
        if (job.enq && newest && newest.enq && job.enq.s < newest.enq.s) {
          disorder ??= `${newest.id} (enqueued at ${ms(newest.enq.t)}) was handed out at ${ms(newest.deliveries[0].t)}, before ${job.id}, which was enqueued earlier (at ${ms(job.enq.t)}) and was waiting the whole time.`;
        }
        if (job.enq && (!newest || !newest.enq || job.enq.s > newest.enq.s)) newest = job;
      } else if (!ackMode) {
        twice ??= `${job.id} was handed to ${prev.to} at ${ms(prev.t)} and again to ${d.to} at ${ms(d.t)}. dequeue must remove the job from the queue.`;
      } else if (job.ack && job.ack.s < d.s) {
        afterAck ??= `${job.id} was acknowledged at ${ms(job.ack.t)}, yet the broker handed it to ${d.to} at ${ms(d.t)}. An ack must finish the job wherever it is: with a consumer, or back in the queue after a timeout.${copies}`;
      } else if (d.t - prev.t < EARLY && !crashBetween(prev.t, d.t)) {
        early ??= `${job.id} was handed to ${prev.to} at ${ms(prev.t)} and again to ${d.to} at ${ms(d.t)}, only ${ms(d.t - prev.t)} later. It must stay invisible for ${VISIBILITY}ms.${copies}`;
      }
      if (maxDeliveries && job.deliveries.length >= maxDeliveries) {
        over ??= `${job.id} was handed out a ${job.deliveries.length + 1}th time at ${ms(d.t)} (earlier: ${job.deliveries.map((x) => ms(x.t)).join(', ')}). After ${maxDeliveries} hand-outs without an ack it belongs in the dead letters.`;
      }
      job.deliveries.push(d);
    }

    // A dequeue may only come back empty when there is nothing to hand out.
    let idle = null;
    const enqueued = [...jobs.values()].filter((job) => job.enq);
    for (const n of nulls) {
      if (idle) break;
      for (const job of enqueued) {
        if (job.enq.s > n.s || (job.ack && job.ack.s < n.s)) continue;
        const before = job.deliveries.filter((d) => d.s < n.s);
        if (before.length === 0) {
          idle = `At ${ms(n.t)} a dequeue was answered with job: null, but ${job.id} (enqueued at ${ms(job.enq.t)}) had not been handed to anyone yet.`;
          if (crashMode && restartBefore(n.t) > job.enq.t) idle += ` The broker restarted at ${ms(restartBefore(n.t))}: did the job survive on node.disk?`;
          break;
        }
        if (!ackMode || (maxDeliveries && before.length >= maxDeliveries)) continue;
        const last = before[before.length - 1];
        const since = Math.max(last.t, restartBefore(n.t));
        if (n.t - since > LATE) {
          idle = `At ${ms(n.t)} a dequeue was answered with job: null, but ${job.id} was handed to ${last.to} at ${ms(last.t)} and never acknowledged. ${VISIBILITY}ms after a hand-out an unacknowledged job must be available again (the test allows until ${LATE}ms).`;
          if (crashMode && restartBefore(n.t) > last.t) idle += ` The broker restarted at ${ms(restartBefore(n.t))}: jobs that were with a consumer at the crash must come back too.`;
          break;
        }
      }
    }

    checks.push(
      check('Only enqueued jobs are handed out, payload intact', !invented, invented),
      check('Jobs get their first hand-out in the order they were enqueued', !disorder, disorder),
    );
    if (!ackMode) {
      checks.push(check('No job is handed out twice', !twice, twice));
    } else {
      checks.push(
        check(`A job is not handed out again within ${VISIBILITY}ms of the last hand-out`, !early, early),
        check('An acknowledged job is never handed out again', !afterAck, afterAck),
      );
    }
    checks.push(check('dequeue returns a job whenever one is available', !idle, idle));

    // Dead letters.
    let deadIds = new Set();
    if (maxDeliveries) {
      let deadProblem = null;
      const list = deadReply && deadReply.type === 'dead_letters_ok' ? deadReply.jobs : null;
      if (!Array.isArray(list)) {
        deadProblem = deadReply ? `The broker replied ${j(deadReply)} to dead_letters; expected { type: "dead_letters_ok", jobs: [...] }.` : 'The broker did not answer the dead_letters request at the end of the test.';
      } else {
        for (const entry of list) {
          const job = entry && typeof entry === 'object' ? jobs.get(entry.id) : null;
          if (!job) deadProblem ??= `dead_letters returned ${j(entry)}, which is not a job that was enqueued. Each entry must be { id, payload }.`;
          else if (!same(job.payload, entry.payload)) deadProblem ??= `dead_letters returned ${job.id} with payload ${j(entry.payload)}; it was enqueued with ${j(job.payload)}.`;
          else if (deadIds.has(job.id)) deadProblem ??= `dead_letters lists ${job.id} twice.`;
          else if (job.deliveries.length < maxDeliveries) deadProblem ??= `${job.id} is in the dead letters after only ${plural(job.deliveries.length, 'hand-out')}${job.ack ? ' (and it was acknowledged)' : ''}. A job gets ${maxDeliveries} attempts.`;
          if (job) deadIds.add(job.id);
        }
        const missing = enqueued.filter((job) => !job.ack && job.deliveries.length >= maxDeliveries && !deadIds.has(job.id));
        if (missing.length) {
          const m = missing[0];
          deadProblem ??= `${plural(missing.length, 'job')} failed ${maxDeliveries} times but ${missing.length === 1 ? 'is' : 'are'} not in dead_letters (${sample(missing.map((x) => x.id))}). ${m.id} was handed out at ${m.deliveries.map((x) => ms(x.t)).join(', ')} and never acknowledged.`;
        }
      }
      checks.push(
        check(`No job is handed out more than ${maxDeliveries} times`, !over, over),
        check(`dead_letters lists exactly the jobs that failed ${maxDeliveries} times`, !deadProblem, deadProblem),
      );
    }

    // Nothing may fall through the cracks.
    const lost = enqueued.filter((job) => (ackMode ? !job.ack && !deadIds.has(job.id) : job.deliveries.length === 0));
    let lostDetail = '';
    if (lost.length) {
      const l = lost[0];
      const last = l.deliveries[l.deliveries.length - 1];
      lostDetail = `${plural(lost.length, 'job')} of ${enqueued.length} ${lost.length === 1 ? 'was' : 'were'} accepted with enqueue_ok but never ${ackMode ? `acknowledged${maxDeliveries ? ' or dead-lettered' : ''}` : 'handed out'} (${sample(lost.map((x) => x.id))}), although consumers kept polling until ${ms(stopAt)}. `;
      if (!last) lostDetail += `${l.id} was enqueued at ${ms(l.enq.t)} and never handed out.`;
      else lostDetail += `${l.id} was last handed to ${last.to} at ${ms(last.t)}; that consumer ${ackMode ? 'never acknowledged it, so it had to be handed out again' : 'got it'}.`;
      const restart = restartBefore(sim.duration);
      if (crashMode && restart > l.enq.t) lostDetail += ` The broker crashed after accepting it (last restart at ${ms(restart)}); only node.disk survives a crash.`;
    }
    checks.push(check(ackMode ? 'No job is lost: every job is eventually acknowledged' + (maxDeliveries ? ' or dead-lettered' : '') : 'No job is lost: every job is handed out', enqueued.length > 0 && !lost.length, lostDetail || 'No enqueue was accepted with enqueue_ok.'));

    return checks;
  };
}
