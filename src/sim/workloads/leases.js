// Workloads for the "Leases and fencing" track.
//
// LS-1  one lock server (n1) and three test clients (c1..c3) that acquire,
//       renew and release; some go silent while holding the lock.
// LS-2  n1 is the lock server, n2 the storage, n3..n5 workers. Each job is a
//       read-modify-write of one list on n2 under the lock. Workers are paused
//       (sim.pause) or have their write held in the network past their lease.
// LS-3  the same cluster, with the lock server and the storage crashing.
// LS-4  the lock and a printer are services run by the harness; n1..n3 are
//       workers that may only print while they hold the lease. Clocks are
//       skewed, grants arrive late, and workers are paused right after a grant.

import { check, j, plural, unanswered, wrongType } from '../workloads.js';

export const LEASE = 300; // ms, every lease in this track
const ms = (t) => `${Math.round(t)}ms`;

// ===================================================================== LS-1

export function leaseLock(sim, { scenario = 'turns' }) {
  const SLACK = sim.real ? 40 : 5;
  const clients = ['c1', 'c2', 'c3'];
  const ops = []; // { client, kind, invoke, complete, res }
  const stopAt = sim.duration - 500;

  async function call(client, kind) {
    const op = { client, kind, invoke: sim.now(), complete: null, res: null };
    ops.push(op);
    op.res = await sim.clientRpc('n1', { type: kind }, { client, timeout: 500 });
    op.complete = op.res ? sim.now() : null;
    return op.res;
  }

  // Acquire, retrying every 40ms for up to 1.2s. Returns true if granted.
  async function acquire(client) {
    const deadline = sim.now() + 1200;
    while (sim.now() < deadline && sim.now() < stopAt) {
      const res = await call(client, 'acquire');
      if (res && res.granted === true) return true;
      await sim.sleep(40);
    }
    return false;
  }

  async function hold(client, holdMs) {
    const until = sim.now() + holdMs;
    while (sim.now() + 100 < until) {
      await sim.sleep(100);
      const res = await call(client, 'renew');
      if (!res || res.ok !== true) return false;
    }
    await sim.sleep(Math.max(0, until - sim.now()));
    return true;
  }

  async function life(client, start, abandonRate) {
    await sim.sleep(start);
    while (sim.now() < stopAt - 300) {
      if (await acquire(client)) {
        const kept = await hold(client, 120 + sim.rand() * 260);
        if (kept && sim.rand() < abandonRate) {
          // Go silent while holding: no renew, no release. Then come back
          // after the lease has certainly run out and try to carry on.
          await sim.sleep(LEASE + 150 + sim.rand() * 250);
          await call(client, 'renew'); // must fail: the lease has expired
          await call(client, 'release'); // must not free someone else's lock
        } else if (kept) {
          await call(client, 'release');
        }
      }
      await sim.sleep(40 + sim.rand() * 160);
    }
  }

  const abandonRate = scenario === 'abandon' ? 0.5 : 0;
  clients.forEach((c, i) => sim.at(0, () => life(c, 80 + i * 37, abandonRate)));

  return () => {
    const checks = [];
    const results = ops.map((o) => ({ node: 'n1', req: { type: o.kind }, res: o.res }));
    checks.push(unanswered('Every request gets a reply', results, 'n1 should answer acquire, renew and release straight away.'));
    const badType = ops.find((o) => o.res && o.res.type !== o.kind + '_ok');
    checks.push(check('Replies have the right type', !badType, badType && `n1 answered ${j(badType.res)} to ${badType.client}'s ${badType.kind}; expected type "${badType.kind}_ok".`));
    const vague = ops.find(
      (o) => o.res && ((o.kind === 'acquire' && typeof o.res.granted !== 'boolean') || (o.kind === 'renew' && typeof o.res.ok !== 'boolean')),
    );
    checks.push(
      check(
        'acquire says granted, renew says ok',
        !vague,
        vague && `n1 answered ${j(vague.res)} to ${vague.client}'s ${vague.kind}. acquire_ok carries granted: true/false, renew_ok carries ok: true/false.`,
      ),
    );

    const won = (o) => o.res && ((o.kind === 'acquire' && o.res.granted === true) || (o.kind === 'renew' && o.res.ok === true));
    const byClient = Object.fromEntries(clients.map((c) => [c, ops.filter((o) => o.client === c)]));

    // Spans in which a client certainly held the lock, whatever the exact
    // moment n1 handled each request: from the grant's reply until LEASE after
    // the request was sent, or until it sent a release.
    const spans = [];
    for (const c of clients) {
      const list = byClient[c];
      list.forEach((o, i) => {
        if (!won(o)) return;
        let end = o.invoke + LEASE;
        const rel = list.slice(i + 1).find((x) => x.kind === 'release');
        if (rel) end = Math.min(end, rel.invoke);
        if (o.complete < end) spans.push({ client: c, start: o.complete, end, op: o });
      });
    }
    let overlap = null;
    for (const a of spans) {
      for (const b of spans) {
        if (a.client < b.client && a.start < b.end - 0.5 && b.start < a.end - 0.5) {
          overlap ??= [a, b];
        }
      }
    }
    checks.push(
      check(
        'Never two holders at once',
        !overlap,
        overlap &&
          `${overlap[0].client} held the lock from ${ms(overlap[0].start)} to at least ${ms(overlap[0].end)} (its ${overlap[0].op.kind} sent at ${ms(overlap[0].op.invoke)} succeeded), ` +
            `yet ${overlap[1].client} was given it too (its ${overlap[1].op.kind} sent at ${ms(overlap[1].op.invoke)} succeeded at ${ms(overlap[1].start)}). ` +
            'A lease lasts 300ms from the grant or renewal; until it ends or the holder releases, nobody else may have the lock. Check renew and release: only the current holder of an unexpired lease may do either.',
      ),
    );

    // A denied acquire is wrong when every other client's lease had certainly
    // ended (expired or released) before the request was even sent.
    let wrongDenial = null;
    for (const d of ops) {
      if (d.kind !== 'acquire' || !d.res || d.res.granted !== false || wrongDenial) continue;
      const blockers = ops.filter((o) => o.client !== d.client && (o.kind === 'acquire' || o.kind === 'renew') && o.invoke < d.complete && (won(o) || !o.res));
      const live = blockers.find((o) => {
        if (!o.res) return true;
        if (o.complete + LEASE + SLACK <= d.invoke) return false;
        const rel = byClient[o.client].find((x) => x.kind === 'release' && x.invoke >= o.complete && x.complete !== null && x.complete + SLACK <= d.invoke);
        return !rel;
      });
      if (!live) {
        const last = blockers.filter((o) => o.res).sort((a, b) => b.complete - a.complete)[0];
        wrongDenial = { d, last };
      }
    }
    checks.push(
      check(
        'A free lock is granted',
        !wrongDenial,
        wrongDenial &&
          `${wrongDenial.d.client} asked for the lock at ${ms(wrongDenial.d.invoke)} and was refused, but nobody held it: ` +
            (wrongDenial.last
              ? `the last grant or renewal to anyone else was ${wrongDenial.last.client}'s at ${ms(wrongDenial.last.complete)}, so that lease ended by ${ms(wrongDenial.last.complete + LEASE)} at the latest. `
              : 'no one else had been granted it. ') +
            'A holder that stops renewing (it crashed, or is cut off) must lose the lock 300ms after its last grant or renewal; otherwise one dead client blocks everyone forever.',
      ),
    );

    let dropped = null;
    let zombie = null;
    for (const c of clients) {
      const list = byClient[c];
      list.forEach((r, i) => {
        if (r.kind !== 'renew' || !r.res) return;
        let prev = null;
        for (let k = i - 1; k >= 0; k--) {
          if (list[k].kind === 'release') break;
          if (won(list[k])) {
            prev = list[k];
            break;
          }
        }
        if (!prev) return;
        if (r.res.ok === false && r.complete < prev.invoke + LEASE - SLACK) dropped ??= { r, prev };
        if (r.res.ok === true && r.invoke > prev.complete + LEASE + SLACK) zombie ??= { r, prev };
      });
    }
    checks.push(
      check(
        'A holder that renews in time keeps the lock',
        !dropped,
        dropped &&
          `${dropped.r.client}'s renew at ${ms(dropped.r.invoke)} was refused, though its ${dropped.prev.kind} sent at ${ms(dropped.prev.invoke)} had succeeded and its lease could not have ended before ${ms(dropped.prev.invoke + LEASE)}. A renewal by the current holder extends the lease to 300ms from now.`,
      ),
    );
    checks.push(
      check(
        'An expired lease cannot be renewed',
        !zombie,
        zombie &&
          `${zombie.r.client}'s last successful ${zombie.prev.kind} was answered at ${ms(zombie.prev.complete)}, so its lease ended by ${ms(zombie.prev.complete + LEASE)}. It went silent, then renewed at ${ms(zombie.r.invoke)} and n1 said ok. After expiry the client must acquire again: someone else may have held the lock in between, and the client cannot know.`,
      ),
    );
    const grants = ops.filter((o) => o.kind === 'acquire' && won(o)).length;
    checks.push(check('The lock changes hands', grants >= 6, `Only ${plural(grants, 'acquire')} succeeded in the whole run.`));
    return checks;
  };
}

// ============================================================== LS-2, LS-3

export function leaseJobs(sim, { scenario = 'healthy', crashes = [] }) {
  const LOCK = 'n1';
  const STORE = 'n2';
  const workers = sim.nodeIds.slice(2);
  const JOB_TIMEOUT = 4500;
  const stopAt = sim.duration - 700 - (crashes.length ? 400 : 0);
  const jobs = []; // { job, worker, invoke, complete, res, faulted }
  const faults = []; // { kind, node, start, end, job }
  const grants = []; // acquire_ok granted, seen leaving n1
  let count = 0;
  let inspected = null;

  // Remember every grant n1 sends (to report tokens and who held the lock).
  sim.onSend = (m) => {
    if (m.src === LOCK && m.body && m.body.type === 'acquire_ok' && m.body.granted === true) {
      grants.push({ t: sim.now(), to: m.dest, token: m.body.token });
    }
    hooks.send && hooks.send(m);
  };
  const hooks = { send: null, deliver: null };
  sim.onDeliver = (m) => hooks.deliver && hooks.deliver(m);

  // One target worker gets a fault during one of its jobs. `armed` is set by
  // the client loop just before it sends that job.
  const plan = [];
  if (scenario === 'pause') plan.push({ at: 900, worker: workers[0], kind: 'pause' }, { at: 2500, worker: workers[1], kind: 'pause' });
  if (scenario === 'race') plan.push({ at: 900, worker: workers[0], kind: 'pause' }, { at: 2500, worker: workers[1], kind: 'pause' });
  if (scenario === 'slow-write') plan.push({ at: 900, worker: workers[0], kind: 'delay' }, { at: 2500, worker: workers[2], kind: 'delay' });
  let armed = null;
  let race = null; // { worker, start, end }: hold the next holder's write until the paused one has woken
  const sentToStore = {}; // worker -> messages sent to n2 during its current job
  const current = {}; // worker -> its job in progress

  hooks.deliver = (m) => {
    // The first thing n2 sends the target after its job starts is the reply
    // to its read. Freeze the worker right there: it has read under its lease
    // and is about to write.
    if (armed && armed.kind === 'pause' && m.src === STORE && m.dest === armed.worker) {
      const f = armed;
      armed = null;
      const len = scenario === 'race' ? 450 : 700;
      sim.pause(f.worker, len);
      faults.push({ kind: 'pause', node: f.worker, start: sim.now(), end: sim.now() + len, job: f.job });
      if (scenario === 'race') race = { worker: f.worker, start: sim.now(), end: sim.now() + len };
    }
  };
  sim.intercept = ({ src, dest }) => {
    if (dest === STORE && src in sentToStore) sentToStore[src] += 1;
    // While the paused worker sleeps (450ms), its lease runs out and another
    // worker takes the lock and reads. Hold that worker's write (its second
    // message to n2) until 60ms after the paused one wakes up and writes.
    // The new holder got its lease at most ~150ms before the wake-up, so its
    // lease is still running when its write lands.
    if (race && src !== race.worker && dest === STORE && sentToStore[src] === 2 && sim.now() < race.end) {
      const hold = race.end + 60 - sim.now();
      if (current[src]) current[src].faulted = true; // its own lease may run out while it waits
      faults.push({ kind: 'held', node: src, start: sim.now(), end: race.end + 60, sleeper: race.worker });
      race = null;
      return hold;
    }
    // The second message the target sends n2 in its job is its write. Hold
    // it in the network for 700ms, more than twice the lease.
    if (armed && armed.kind === 'delay' && src === armed.worker && dest === STORE) {
      armed.seen += 1;
      if (armed.seen === 2) {
        const f = armed;
        armed = null;
        faults.push({ kind: 'delay', node: f.worker, start: sim.now(), end: sim.now() + 700, job: f.job });
        return 700;
      }
    }
    return undefined;
  };

  async function loop(worker, k) {
    await sim.sleep(100 + k * 23);
    while (sim.now() < stopAt) {
      const job = 'j' + ++count;
      const rec = { job, worker, invoke: sim.now(), complete: null, res: null, faulted: false };
      const p = plan.find((x) => x.worker === worker && !x.used && sim.now() >= x.at);
      if (p && !armed) {
        p.used = true;
        armed = { ...p, job, seen: 0 };
        rec.faulted = true;
      }
      jobs.push(rec);
      sentToStore[worker] = 0;
      current[worker] = rec;
      rec.res = await sim.clientRpc(worker, { type: 'job', job }, { client: 'c' + (k + 1), timeout: JOB_TIMEOUT });
      rec.complete = rec.res ? sim.now() : null;
      if (armed && armed.job === job) armed = null; // the fault never found its moment
      await sim.sleep(30 + sim.rand() * 90);
    }
  }
  workers.forEach((w, k) => sim.at(0, () => loop(w, k)));

  for (const c of crashes) {
    sim.at(c.at, () => {
      sim.crash(c.node);
      faults.push({ kind: 'crash', node: c.node, start: sim.now(), end: sim.now() + c.down });
    });
    sim.at(c.at + c.down, () => sim.restart(c.node));
  }

  sim.at(sim.duration - 350, async () => {
    inspected = (await sim.clientRpc(STORE, { type: 'inspect' }, { timeout: 300 })) ?? false;
  });

  const describeFault = (f) =>
    f.kind === 'held'
      ? `${f.node}'s write was held in the network from ${ms(f.start)} to ${ms(f.end)}, so ${f.sleeper} woke up from its pause between ${f.node}'s read and ${f.node}'s write`
      : f.kind === 'pause'
      ? `${f.node} was paused from ${ms(f.start)} to ${ms(f.end)} during ${f.job}, right after its read came back`
      : f.kind === 'delay'
        ? `${f.node}'s write for ${f.job} was held in the network from ${ms(f.start)} to ${ms(f.end)}`
        : `${f.node} was down from ${ms(f.start)} to ${ms(f.end)}`;

  return () => {
    const checks = [];
    const results = jobs.map((r) => ({ node: r.worker, req: { type: 'job', job: r.job }, res: r.res }));
    // A job sent in the last 1.5s may still be waiting for its turn when the run ends.
    const settled = jobs.filter((r) => r.res || r.invoke < sim.duration - 1500).map((r) => ({ node: r.worker, req: { type: 'job', job: r.job }, res: r.res }));
    checks.push(unanswered('Every job gets a reply', settled, 'Answer job_ok with done: false if the lock or the storage stays out of reach; the client waits 4.5s.'));
    checks.push(wrongType(results, 'job_ok'));
    const vague = jobs.find((r) => r.res && typeof r.res.done !== 'boolean');
    checks.push(check('Replies say done: true or false', !vague, vague && `${vague.worker} answered ${j(vague.res)} to ${vague.job}.`));

    // Tokens.
    let badToken = null;
    let max = -Infinity;
    let last = null;
    for (const g of grants) {
      if (typeof g.token !== 'number') {
        badToken ??= `n1 granted the lock to ${g.to} at ${ms(g.t)} with token ${j(g.token)}. Every acquire_ok with granted: true must carry a number token.`;
        continue;
      }
      const sameHolder = last && last.to === g.to && last.t > g.t - LEASE;
      if (g.token < max || (g.token === max && !sameHolder)) {
        badToken ??=
          `n1 granted the lock to ${g.to} at ${ms(g.t)} with token ${g.token}, but it had already handed out token ${max}` +
          (last ? ` (to ${last.to} at ${ms(last.t)})` : '') +
          '. Every new grant needs a token higher than any before it, or the storage cannot tell the new holder from an old one.' +
          (crashes.some((c) => c.node === LOCK && c.at < g.t) ? ' n1 crashed before this grant: did its counter start again from zero?' : '');
      }
      max = Math.max(max, g.token);
      last = g;
    }
    checks.push(check('Each new grant has a higher token', !badToken && grants.length > 0, badToken || 'n1 never granted the lock.'));

    checks.push(
      check(
        'The final inspection completed',
        inspected && inspected.type === 'inspect_ok' && Array.isArray(inspected.entries),
        `n2 answered ${j(inspected ?? null)} to { type: "inspect" } near the end of the run. Expected { type: "inspect_ok", entries: [...] }.`,
      ),
    );
    if (!(inspected && Array.isArray(inspected.entries))) return checks;
    const entries = inspected.entries;

    const lost = jobs.filter((r) => r.res && r.res.done === true && !entries.includes(r.job));
    let lostDetail = '';
    if (lost.length) {
      const r = lost[0];
      const near = (f) => f.kind !== 'crash' && f.start <= r.complete + 1000 && f.end >= r.invoke - 1000;
      const held = faults.find((f) => f.kind === 'held' && near(f));
      const nearby = faults.find((f) => f.kind !== 'held' && near(f));
      const crash = faults.find((f) => f.kind === 'crash' && f.node === STORE && f.start >= r.complete);
      lostDetail =
        `${plural(lost.length, 'acknowledged job')} missing from n2's list. First: ${r.job}, done by ${r.worker} (acknowledged at ${ms(r.complete)}). ` +
        (held
          ? `${describeFault(held)}. n2 accepted ${held.sleeper}'s stale write, because no higher token had written yet; then ${held.node}'s write, built from the list it read before, wiped it out. ` +
            `Refuse lower tokens on reads as well: once ${held.node} has read with its token, ${held.sleeper}'s older token must not write.`
          : nearby
          ? `${describeFault(nearby)}. Its lease ran out meanwhile and ${r.worker} took the lock, wrote its list and finished; then ${nearby.node}'s write, built from the list it read earlier, replaced that list. ` +
            'This is what fencing tokens are for: send the token with every read and write, and have n2 refuse any token lower than the highest it has seen.'
          : crash
            ? `n2 crashed at ${ms(crash.start)}, after the job was acknowledged. Whatever n2 acknowledged must be on node.disk before it answers.`
            : 'Something overwrote it with an older version of the list.');
    }
    checks.push(check('No acknowledged job is lost', lost.length === 0, lostDetail));

    const seen = new Set();
    const dup = entries.find((e) => (seen.has(e) ? true : (seen.add(e), false)));
    const known = new Set(jobs.map((r) => r.job));
    const stray = entries.find((e) => !known.has(e));
    checks.push(check('Every job is in the list at most once', !dup && stray === undefined, dup ? `${j(dup)} appears twice in n2's list.` : `n2's list holds ${j(stray)}, which no client sent.`));

    // Jobs untouched by any fault must succeed.
    const excused = (r) => {
      if (r.faulted) return true;
      const end = r.complete ?? r.invoke + JOB_TIMEOUT;
      return faults.some((f) => f.kind === 'crash' && r.invoke < f.end + 300 && end > f.start);
    };
    const failed = jobs.filter((r) => r.res && r.res.done === false && !excused(r));
    let failDetail = '';
    if (failed.length) {
      const r = failed[0];
      const after = faults.filter((f) => f.kind === 'crash' && f.end < r.invoke).pop();
      failDetail =
        `${plural(failed.length, 'job')} answered done: false although no fault touched them. First: ${r.job} on ${r.worker}, sent at ${ms(r.invoke)}. ` +
        (after
          ? `${after.node} had restarted at ${ms(after.end)}. ` + (after.node === LOCK ? 'If n2 refuses every write after n1 restarts, n1 is handing out tokens it has used before: keep the counter on node.disk.' : 'Did n2 lose its highest token, or its list, in the crash?')
          : 'A worker waits for the lock (retry acquire for up to 1.5s), and a write with the current token must be accepted.');
    }
    checks.push(check('Jobs succeed when nothing goes wrong', failed.length === 0, failDetail));
    const done = jobs.filter((r) => r.res && r.res.done === true).length;
    checks.push(check('Most jobs get done', done >= jobs.length * 0.6 && done >= 6, `Only ${done} of ${jobs.length} jobs were done.`));
    return checks;
  };
}

// ===================================================================== LS-4

export function leaseClocks(sim, { scenario = 'steady' }) {
  const MARGIN_HINT = 50;
  const JOB_TIMEOUT = 5000;
  const workers = sim.nodeIds;
  const stopAt = sim.duration - 1000;
  const lock = { holder: null, expires: 0 };
  const grants = []; // { t, to, expires }
  const prints = []; // { t, src, job, page, valid, holder, expires, lastGrant }
  const jobs = [];
  const faults = [];
  let count = 0;
  let grantCount = 0;

  sim.service('lock', (msg, reply) => {
    const t = sim.now();
    const src = msg.src;
    const b = msg.body;
    if (b.type === 'acquire') {
      if (!lock.holder || t >= lock.expires || lock.holder === src) {
        lock.holder = src;
        lock.expires = t + LEASE;
        grants.push({ t, to: src, expires: lock.expires });
        reply({ type: 'acquire_ok', granted: true, lease_ms: LEASE, expires_at: Math.round(lock.expires) });
      } else {
        reply({ type: 'acquire_ok', granted: false, lease_ms: LEASE });
      }
    } else if (b.type === 'release') {
      if (lock.holder === src && t < lock.expires) lock.holder = null;
      reply({ type: 'release_ok' });
    } else {
      reply({ type: 'error', text: 'the lock understands acquire and release' });
    }
  });

  sim.service('printer', (msg, reply) => {
    const t = sim.now();
    const b = msg.body;
    if (b.type !== 'print') return reply({ type: 'error', text: 'the printer understands print' });
    const valid = lock.holder === msg.src && t < lock.expires;
    const lastGrant = grants.filter((g) => g.to === msg.src).pop() ?? null;
    prints.push({ t, src: msg.src, job: b.job, page: b.page, valid, holder: lock.holder, expires: lock.expires, lastGrant });
    sim.sleep(40).then(() => reply({ type: 'print_ok' }));
  });

  // Faults on the grants coming back from the lock.
  if (scenario === 'slow-grants') {
    sim.intercept = ({ src, body }) => {
      if (src === 'lock' && body && body.type === 'acquire_ok' && body.granted === true) {
        grantCount += 1;
        if (grantCount % 2 === 0) return 150;
      }
      return undefined;
    };
  }
  const pausePlan = scenario === 'pause' ? [{ at: 800, worker: workers[0] }, { at: 1900, worker: workers[1] }, { at: 3000, worker: workers[2] }] : [];
  if (pausePlan.length) {
    sim.onDeliver = (m) => {
      if (m.src !== 'lock' || !m.body || m.body.type !== 'acquire_ok' || m.body.granted !== true) return;
      const p = pausePlan.find((x) => !x.used && x.worker === m.dest && sim.now() >= x.at);
      if (!p) return;
      p.used = true;
      sim.pause(m.dest, 450);
      faults.push({ node: m.dest, start: sim.now(), end: sim.now() + 450 });
    };
  }

  async function loop(worker, k) {
    await sim.sleep(100 + k * 31);
    while (sim.now() < stopAt) {
      const job = 'j' + ++count;
      const pages = scenario === 'slow-grants' ? 5 + Math.floor(sim.rand() * 4) : 3 + Math.floor(sim.rand() * 6);
      const rec = { job, worker, pages, invoke: sim.now(), complete: null, res: null };
      jobs.push(rec);
      rec.res = await sim.clientRpc(worker, { type: 'job', job, pages }, { client: 'c' + (k + 1), timeout: JOB_TIMEOUT });
      rec.complete = rec.res ? sim.now() : null;
      await sim.sleep(40 + sim.rand() * 150);
    }
  }
  workers.forEach((w, k) => sim.at(0, () => loop(w, k)));

  return () => {
    const checks = [];
    const results = jobs.map((r) => ({ node: r.worker, req: { type: 'job', job: r.job, pages: r.pages }, res: r.res }));
    // A job sent in the last 3.5s may still be waiting for the lock when the run ends.
    const settled = jobs.filter((r) => r.res || r.invoke < sim.duration - 3500).map((r) => ({ node: r.worker, req: { type: 'job', job: r.job, pages: r.pages }, res: r.res }));
    checks.push(unanswered('Every job gets a reply', settled, 'The client waits 5s. Keep asking for the lock until you get it.'));
    checks.push(wrongType(results, 'job_ok'));
    const notDone = jobs.find((r) => r.res && r.res.done !== true);
    checks.push(check('Every job is done', !notDone, notDone && `${notDone.worker} answered ${j(notDone.res)} to ${notDone.job}. Nothing stops a job for good: wait for the lock and print every page.`));

    const bad = prints.filter((p) => !p.valid);
    let detail = '';
    if (bad.length) {
      const p = bad[0];
      const g = p.lastGrant;
      const pause = faults.find((f) => f.node === p.src && f.start <= p.t && f.start >= (g ? g.t - 1 : 0));
      detail =
        `${plural(bad.length, 'page')} reached the printer while the sender had no lease. First: ${p.src}'s ${p.job} page ${p.page} at ${ms(p.t)}. ` +
        (g
          ? `The lock last granted ${p.src} a lease at ${ms(g.t)}; it ended at ${ms(g.t + LEASE)}, ${ms(p.t - g.t - LEASE)} before this page arrived` +
            (p.holder && p.holder !== p.src && p.t < p.expires ? `, and ${p.holder} held the lock by then. ` : '. ')
          : `${p.src} never held the lock. `) +
        (scenario === 'skew'
          ? "Clocks differ by up to 400ms here, so expires_at (the lock's clock) means nothing on your node's clock. Measure the lease on your own clock: it starts no later than the moment you sent acquire."
          : scenario === 'slow-grants'
            ? 'Some grants spend 150ms in the network. The lease started when the lock granted it, not when the reply reached you: count from when you sent acquire.'
            : pause
              ? `${p.src} was paused from ${ms(pause.start)} to ${ms(pause.end)}, as the grant arrived. When it woke up the lease was already over: check the time left before every page, measured from when you sent acquire.`
              : `Stop printing at least ${MARGIN_HINT}ms before the lease ends by your own estimate; a page still has to travel to the printer.`);
    }
    checks.push(check('Pages print only under a valid lease', bad.length === 0, detail));

    const missing = jobs.find((r) => {
      if (!(r.res && r.res.done === true)) return false;
      const got = new Set(prints.filter((p) => p.src === r.worker && p.job === r.job).map((p) => p.page));
      for (let i = 1; i <= r.pages; i++) if (!got.has(i)) return true;
      return false;
    });
    checks.push(
      check(
        'Every page of a done job is printed',
        !missing,
        missing && `${missing.worker} said ${missing.job} was done, but the printer did not get all of pages 1 to ${missing.pages} from it.`,
      ),
    );
    return checks;
  };
}
