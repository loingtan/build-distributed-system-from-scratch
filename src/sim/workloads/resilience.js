// Workloads for the resilience track.
//
// In every task the learner's node answers `get` requests by calling a fake
// service named "upstream". The harness decides, call by call, whether that
// service answers, answers late, answers with an error or stays silent, and
// it records every call it receives. Those records are how the checkers
// measure the load a node puts on its dependency.

import { check, j, plural, unanswered } from '../workloads.js';

const ms = (t) => `${Math.round(t)}ms`;
const isOk = (r) => !!r.res && r.res.type === 'get_ok';

// Registers the upstream and watches the traffic around it.
//
// decide(call) -> { delay }                 answer fetch_ok after `delay` ms
//                 { error: 'busy', delay }  answer with an error after `delay` ms
//                 { silent: true }          never answer
//
// Each call is recorded as { key, src, n, sent, arrived, outcome, answered, heard }:
// when the node sent it, when it reached the upstream, what the upstream did,
// when the upstream answered and when that answer reached the node.
function makeUpstream(sim, decide) {
  const secret = 's' + Math.floor(sim.rand() * 1e6);
  const up = {
    calls: [],
    byKey: new Map(),
    arrivals: [], // client requests reaching a node: { t, node, key }
    replies: new Map(), // client msg_id -> number of replies a node sent
    peak: 0,
    peakAt: 0,
    value: (key) => key + '/' + secret,
    callsFor: (key) => up.byKey.get(key) ?? [],
  };
  const byId = new Map();
  const nodes = new Set(sim.nodeIds);
  let inFlight = 0;

  const record = (src, body, sent) => {
    const call = { key: body.key, src, n: up.calls.length + 1, sent, arrived: null, outcome: null, answered: null, heard: null };
    up.calls.push(call);
    if (!up.byKey.has(call.key)) up.byKey.set(call.key, []);
    up.byKey.get(call.key).push(call);
    if (body.msg_id != null) byId.set(body.msg_id, call);
    return call;
  };

  sim.onSend = (msg) => {
    if (msg.dest === 'upstream') record(msg.src, msg.body, msg.t0);
    else if (nodes.has(msg.src) && msg.kind === 'client' && msg.body.in_reply_to != null) {
      up.replies.set(msg.body.in_reply_to, (up.replies.get(msg.body.in_reply_to) ?? 0) + 1);
    }
  };
  sim.onDeliver = (msg) => {
    if (msg.src === 'upstream') {
      const call = byId.get(msg.body.in_reply_to);
      if (call) call.heard = sim.now();
    } else if (nodes.has(msg.dest) && msg.kind === 'client' && msg.body.type === 'get') {
      up.arrivals.push({ t: sim.now(), node: msg.dest, key: msg.body.key });
    }
  };

  sim.service('upstream', (msg, reply) => {
    const call = byId.get(msg.body.msg_id) ?? record(msg.src, msg.body, sim.now());
    call.arrived = sim.now();
    if (msg.body.type !== 'fetch') {
      call.outcome = 'bad_request';
      reply({ type: 'error', code: 'bad_request', text: 'the upstream only understands { type: "fetch", key }' });
      return;
    }
    const d = decide(call);
    if (d.silent) {
      call.outcome = 'silent';
      return;
    }
    call.outcome = d.error ?? 'ok';
    inFlight++;
    if (inFlight > up.peak) {
      up.peak = inFlight;
      up.peakAt = sim.now();
    }
    sim.at(sim.now() + Math.max(1, d.delay), () => {
      inFlight--;
      call.answered = sim.now();
      reply(d.error ? { type: 'error', code: d.error } : { type: 'fetch_ok', value: up.value(call.key) });
    });
  });
  return up;
}

// One client request. The record is pushed at once and filled in when (if) the reply arrives.
function ask(sim, results, node, key, { timeout = 1500, client = 'c1' } = {}) {
  const r = { node, key, req: { type: 'get', key }, res: null, invoked: sim.now(), done: null };
  results.push(r);
  sim.clientRpc(node, r.req, { timeout, client }).then((res) => {
    r.res = res;
    r.done = sim.now();
  });
  return r;
}

const fast = (sim) => ({ delay: 5 + sim.rand() * 35 });

function within(name, results, limit, advice) {
  const late = results.filter((r) => r.res && r.done - r.invoked > limit).sort((a, b) => b.done - b.invoked - (a.done - a.invoked));
  return check(
    name,
    late.length === 0,
    late.length &&
      `${plural(late.length, 'request')} of ${results.length} got a reply later than ${limit}ms. Slowest: ${late[0].node} answered ${j(late[0].req)} after ${ms(late[0].done - late[0].invoked)}. ${advice}`,
  );
}

function rightValues(results, up) {
  const bad = results.find((r) => isOk(r) && r.res.value !== up.value(r.key));
  return check(
    'Successful replies carry the upstream\'s value',
    !bad,
    bad && `${bad.node} answered ${j(bad.req)} with ${j(bad.res)}, but the upstream's value for "${bad.key}" is ${j(up.value(bad.key))}. Pass on res.value from the fetch_ok reply.`,
  );
}

// Every reply that is not get_ok must be { type: 'error', code: <string> }, optionally one of `codes`.
function errorShape(results, codes) {
  const bad = results.find((r) => r.res && !isOk(r) && (r.res.type !== 'error' || typeof r.res.code !== 'string' || (codes && !codes.includes(r.res.code))));
  return check(
    'Failures are reported as error replies',
    !bad,
    bad && `${bad.node} answered ${j(bad.req)} with ${j(bad.res)}. A request that cannot be served gets { type: "error", code: … }${codes ? ` with code ${codes.map((c) => `"${c}"`).join(' or ')}` : ''}.`,
  );
}

// ------------------------------------------------------------ timeouts

const DEADLINE = 300;

export function timeouts(sim, { ops = 60, slow = 0, silent = 0, dark = null }) {
  const plan = new Map();
  const up = makeUpstream(sim, (call) => {
    if (dark && sim.now() >= dark[0] && sim.now() < dark[1]) return { silent: true };
    const mode = plan.get(call.key) ?? 'fast';
    if (mode === 'silent') return { silent: true };
    if (mode === 'slow') return { delay: 400 + sim.rand() * 500 };
    if (mode === 'medium') return { delay: 100 + sim.rand() * 40 };
    return fast(sim);
  });
  const results = [];
  const gap = (sim.duration - 1900) / ops;
  for (let i = 0; i < ops; i++) {
    const key = 'k' + (i + 1);
    const x = sim.rand();
    plan.set(key, x < silent ? 'silent' : x < silent + slow ? 'slow' : x < silent + slow + 0.15 ? 'medium' : 'fast');
    sim.at(200 + i * gap, () => ask(sim, results, sim.pick(sim.nodeIds), key));
  }

  return () => {
    const inTime = [];
    const never = [];
    for (const r of results) {
      const call = up.callsFor(r.key)[0];
      if (call && call.outcome === 'ok' && call.answered - call.arrived < 200) inTime.push({ r, call });
      else never.push({ r, call });
    }
    const dropped = inTime.find(({ r }) => r.res && !isOk(r));
    const notAsked = never.find(({ call }) => !call);
    const invented = never.find(({ r, call }) => call && r.res && !(r.res.type === 'error' && r.res.code === 'timeout'));
    const twice = [...up.replies].find(([, n]) => n > 1);
    return [
      unanswered('Every request gets a reply', results, 'When the upstream stays silent the client must still hear back: reply with an error once you give up waiting.'),
      within(
        `Replies arrive within ${DEADLINE}ms`,
        results,
        DEADLINE,
        'node.rpc waits 1000ms unless you pass { timeout }. Choose one that leaves room for the reply to travel back to the client.',
      ),
      check(
        'Answers the upstream gave in time are passed on',
        !dropped && !notAsked,
        dropped
          ? `The upstream answered "${dropped.r.key}" ${ms(dropped.call.answered - dropped.call.arrived)} after the call arrived, but ${dropped.r.node} replied ${j(dropped.r.res)}. The upstream can take up to 150ms on a normal day, so the timeout must be longer than that.`
          : notAsked && `${notAsked.r.node} never called the upstream for ${j(notAsked.r.req)}.`,
      ),
      rightValues(results, up),
      check(
        'An upstream that does not answer in time becomes a timeout error',
        !invented,
        invented &&
          `The upstream ${invented.call.outcome === 'silent' ? 'never answered' : `took ${ms(invented.call.answered - invented.call.arrived)} to answer`} "${invented.r.key}", but ${invented.r.node} replied ${j(invented.r.res)}. Expected { type: "error", code: "timeout" }.`,
      ),
      check(
        'One reply per request',
        !twice,
        twice && 'A node sent two replies to the same client request. When the timeout has already answered, a late upstream reply must be ignored.',
      ),
    ];
  };
}

// ------------------------------------------------------------- retries

const MAX_ATTEMPTS = 4;
const RETRY_DEADLINE = 2000;

export function retries(sim, { mode = 'flaky', ops = 120, perNode = 12, bursts = [300, 2500] }) {
  const missing = new Set();
  const up = makeUpstream(sim, (call) => {
    if (mode === 'down') return { error: 'busy', delay: 1 + sim.rand() * 2 };
    if (missing.has(call.key)) return { error: 'not_found', delay: 5 + sim.rand() * 15 };
    const x = sim.rand();
    if (x < 0.15) return { silent: true };
    if (x < 0.3) return { error: 'busy', delay: 5 + sim.rand() * 15 };
    return fast(sim);
  });
  const results = [];
  const opts = { timeout: 3000 };
  if (mode === 'down') {
    let n = 0;
    for (const at of bursts) {
      for (const node of sim.nodeIds) {
        for (let i = 0; i < perNode; i++) {
          const key = 'k' + ++n;
          sim.at(at, () => ask(sim, results, node, key, { ...opts, client: 'c' + (1 + (i % 3)) }));
        }
      }
    }
  } else {
    const gap = (sim.duration - 3400) / ops;
    for (let i = 0; i < ops; i++) {
      const key = 'k' + (i + 1);
      if (i % 16 === 7) missing.add(key);
      sim.at(200 + i * gap, () => ask(sim, results, sim.pick(sim.nodeIds), key, opts));
    }
  }

  return () => {
    const checks = [
      unanswered('Every request gets a reply', results, 'After the last attempt fails, reply with { type: "error", code: "unavailable" }.'),
      within(
        `Replies arrive within ${RETRY_DEADLINE}ms, retries included`,
        results,
        RETRY_DEADLINE,
        'Add up the worst case: every attempt timing out plus every wait in between must fit in the deadline.',
      ),
    ];

    const greedy = results.map((r) => ({ r, n: up.callsFor(r.key).length })).sort((a, b) => b.n - a.n)[0];
    checks.push(
      check(
        `At most ${MAX_ATTEMPTS} upstream calls per request`,
        !greedy || greedy.n <= MAX_ATTEMPTS,
        greedy && `${greedy.r.node} called the upstream ${greedy.n} times for "${greedy.r.key}". Retries multiply load exactly when the upstream is struggling, so cap them: one first attempt and up to ${MAX_ATTEMPTS - 1} retries.`,
      ),
      rightValues(results, up),
      errorShape(results),
    );

    if (mode === 'flaky') {
      const findable = results.filter((r) => !missing.has(r.key));
      const ok = findable.filter(isOk).length;
      const need = Math.ceil(findable.length * 0.95);
      const firstFail = findable.find((r) => r.res && !isOk(r));
      checks.push(
        check(
          'Transient failures are retried',
          ok >= need,
          `Only ${ok} of ${findable.length} requests succeeded; with retries at least ${need} should. ${firstFail ? `${firstFail.node} answered "${firstFail.key}" with ${j(firstFail.res)} after ${plural(up.callsFor(firstFail.key).length, 'upstream call')} (outcomes: ${up.callsFor(firstFail.key).map((c) => c.outcome).join(', ')}).` : ''} Retry when the upstream says "busy" and when it does not answer.`,
        ),
      );
      const gone = results.filter((r) => missing.has(r.key));
      const again = gone.find((r) => up.callsFor(r.key).length > 1);
      const masked = gone.find((r) => r.res && !(r.res.type === 'error' && r.res.code === 'not_found'));
      checks.push(
        check(
          'A permanent error is passed on, not retried',
          !again && !masked,
          again
            ? `The upstream said "not_found" for "${again.key}" and ${again.node} asked ${up.callsFor(again.key).length} times. Asking again cannot change that answer; only "busy" and silence are worth a retry.`
            : masked && `The upstream said "not_found" for "${masked.key}" but ${masked.node} replied ${j(masked.res)}. Pass the upstream's error code on to the client.`,
        ),
      );
    } else {
      // Every call fails at once, so the gap between hearing a failure and
      // sending the next attempt is exactly the wait the node chose.
      const waits = [[], [], []];
      for (const r of results) {
        const calls = up.callsFor(r.key);
        for (let i = 1; i < calls.length && i <= 3; i++) {
          if (calls[i - 1].heard != null) waits[i - 1].push(calls[i].sent - calls[i - 1].heard);
        }
      }
      const [w1, w2] = waits;
      const mean = (a) => a.reduce((s, x) => s + x, 0) / a.length;
      const retried = results.filter((r) => up.callsFor(r.key).length >= 3).length;
      checks.push(
        check(
          'A busy upstream is retried',
          retried >= results.length * 0.9,
          `Only ${retried} of ${results.length} requests made a second retry after the upstream answered "busy" twice. Keep trying, up to ${MAX_ATTEMPTS} attempts in all.`,
        ),
      );
      if (w1.length >= 10 && w2.length >= 10) {
        const spread = Math.max(...w1) - Math.min(...w1);
        checks.push(
          check(
            'Retries are not synchronised',
            spread >= 25,
            `${w1.length} requests failed at the same moment and their first retries followed after ${ms(Math.min(...w1))} to ${ms(Math.max(...w1))}, a spread of only ${ms(spread)} (25ms or more is expected). Requests that fail together and wait the same time come back together, as a second spike. Multiply each wait by node.random().`,
          ),
          check(
            'Waits grow from one retry to the next',
            mean(w2) >= 1.5 * mean(w1),
            `The average wait was ${ms(mean(w1))} before the first retry and ${ms(mean(w2))} before the second; the second should be at least 1.5 times the first. Double the ceiling of the wait after every failed attempt.`,
          ),
        );
      }
    }
    return checks;
  };
}

// ------------------------------------------------------ circuit breaker

const OUTAGE_CALLS = 30;
const RECOVERY = 2000;

export function breaker(sim, { outages = [], kind = 'silent', failEvery = 0, rps = 20 }) {
  const down = (t) => outages.find(([a, b]) => t >= a && t < b);
  let n = 0;
  const up = makeUpstream(sim, () => {
    n++;
    if (down(sim.now())) return kind === 'silent' ? { silent: true } : { error: 'busy', delay: 1 + sim.rand() * 2 };
    if (failEvery && n % failEvery === 0) return (n / failEvery) % 2 ? { error: 'busy', delay: 3 } : { silent: true };
    return fast(sim);
  });
  const results = [];
  const step = 1000 / rps;
  let i = 0;
  for (let t = 200; t < sim.duration - 500; t += step) {
    const key = 'k' + ++i;
    const client = 'c' + (1 + (i % 2));
    sim.at(t, () => ask(sim, results, sim.pick(sim.nodeIds), key, { timeout: 450, client }));
  }

  return () => {
    const checks = [
      unanswered('Every request gets a reply', results, 'Whatever state the breaker is in, the client gets an answer.'),
      within(`Replies arrive within ${DEADLINE}ms`, results, DEADLINE, 'Keep the 200ms timeout on every upstream call, the probe included.'),
      rightValues(results, up),
      errorShape(results),
    ];

    // Windows in which the upstream is healthy and the breaker has had time to notice.
    const windows = [];
    let from = 0;
    for (const [a, b] of outages) {
      windows.push([from, a - 250, from === 0 ? null : from - RECOVERY]);
      from = b + RECOVERY;
    }
    windows.push([from, Infinity, from === 0 ? null : from - RECOVERY]);
    let refused = null;
    let refusedCount = 0;
    for (const r of results) {
      const w = windows.find(([a, b]) => r.invoked >= a && r.invoked < b);
      if (!w || !r.res || isOk(r)) continue;
      const call = up.callsFor(r.key)[0];
      if (call && call.outcome !== 'ok') continue; // the upstream itself failed this one
      refusedCount++;
      refused = refused ?? { r, call, recoveredAt: w[2] };
    }
    checks.push(
      check(
        'Requests succeed while the upstream is healthy',
        !refused,
        refused &&
          (refused.recoveredAt !== null
            ? `The upstream recovered at ${ms(refused.recoveredAt)}, yet at ${ms(refused.r.invoked)} ${refused.r.node} still answered ${j(refused.r.res)}${refused.call ? '' : ' without calling it'} (${plural(refusedCount, 'such request')}). After the cool-down, let one request through as a probe and close the breaker when it succeeds; service must be back within ${RECOVERY}ms.`
            : `At ${ms(refused.r.invoked)} the upstream was healthy, yet ${refused.r.node} answered ${j(refused.r.res)}${refused.call ? '' : ' without calling it'} (${plural(refusedCount, 'such request')}). Open only after 5 failures in a row: a success must reset the count.`),
      ),
    );

    for (const [a, b] of outages) {
      const calls = up.calls.filter((c) => c.arrived !== null && c.arrived >= a && c.arrived < b);
      const during = results.filter((r) => r.invoked >= a && r.invoked < b);
      checks.push(
        check(
          `The upstream is left alone during the outage at ${ms(a)}`,
          calls.length <= OUTAGE_CALLS,
          `The upstream was down from ${ms(a)} to ${ms(b)} and received ${plural(calls.length, 'call')} out of ${during.length} client requests; at most ${OUTAGE_CALLS} are allowed. Once open, the breaker answers without calling, and after each cool-down it lets exactly one probe through.`,
        ),
      );
      const settled = results.filter((r) => r.res && r.invoked >= a + 1000 && r.invoked < b);
      const quick = settled.filter((r) => !isOk(r) && r.done - r.invoked <= 50);
      const slowOne = settled.find((r) => r.done - r.invoked > 50);
      checks.push(
        check(
          `Requests fail fast during the outage at ${ms(a)}`,
          quick.length >= settled.length * 0.8,
          `Between ${ms(a + 1000)} and ${ms(b)} only ${quick.length} of ${settled.length} requests were refused within 50ms; at least 80% should be.${slowOne ? ` ${slowOne.node} took ${ms(slowOne.done - slowOne.invoked)} to answer "${slowOne.key}".` : ''} An open breaker replies at once instead of waiting for a timeout it already expects.`,
        ),
      );
    }
    return checks;
  };
}

// --------------------------------------------------------- token bucket

const RATE = 20;
const BURST = 10;

export function rateLimit(sim, { load = [], bursts = [] }) {
  const up = makeUpstream(sim, () => fast(sim));
  const results = [];
  let n = 0;
  for (const { from, to, rps } of load) {
    for (let t = from; t < to; t += 1000 / rps) {
      const key = 'k' + ++n;
      const client = 'c' + (1 + (n % 3));
      sim.at(t, () => ask(sim, results, sim.pick(sim.nodeIds), key, { timeout: 600, client }));
    }
  }
  for (const { at, count } of bursts) {
    for (let i = 0; i < count; i++) {
      const key = 'k' + ++n;
      const client = 'c' + (1 + (i % 3));
      sim.at(at, () => ask(sim, results, sim.pick(sim.nodeIds), key, { timeout: 600, client }));
    }
  }

  return () => {
    // Most calls ever seen in a window, compared with what a bucket can release in it.
    const sent = up.calls.map((c) => c.sent).sort((a, b) => a - b);
    let over = null;
    for (let a = 0; a < sent.length && !over; a++) {
      for (let b = a + 1; b < sent.length; b++) {
        const allowed = BURST + (RATE * (sent[b] - sent[a])) / 1000 + 1;
        if (b - a + 1 > allowed) {
          over = { count: b - a + 1, from: sent[a], to: sent[b], allowed: Math.floor(allowed - 1) };
          break;
        }
      }
    }

    // What a perfect bucket would have admitted, given when requests reached the node.
    let tokens = BURST;
    let last = 0;
    let ideal = 0;
    for (const a of up.arrivals) {
      tokens = Math.min(BURST, tokens + ((a.t - last) * RATE) / 1000);
      last = a.t;
      if (tokens >= 1) {
        tokens -= 1;
        ideal++;
      }
    }
    const ok = results.filter(isOk).length;
    const need = Math.floor(ideal * 0.85);
    const refused = results.filter((r) => r.res && !isOk(r));
    const wrongCode = refused.find((r) => r.res.type !== 'error' || r.res.code !== 'rate_limited');
    const slowNo = refused.find((r) => r.done - r.invoked > 50);
    const wasted = refused.find((r) => up.callsFor(r.key).length > 0);

    return [
      unanswered('Every request gets a reply', results, 'A request that finds the bucket empty is answered with an error, not dropped.'),
      check(
        `Never more than ${BURST} + ${RATE}/s upstream calls`,
        !over,
        over &&
          `${over.count} upstream calls were sent between ${ms(over.from)} and ${ms(over.to)}. A bucket of ${BURST} refilled at ${RATE} per second can release at most ${over.allowed} in ${ms(over.to - over.from)}. Check that the bucket never holds more than ${BURST} tokens and that every admitted request takes one.`,
      ),
      check(
        'Requests are admitted when the bucket has tokens',
        ok >= need,
        `${ok} of ${results.length} requests succeeded. A bucket of ${BURST} refilled at ${RATE}/s would have admitted ${ideal} of them, and at least ${need} are expected. Start with a full bucket and add tokens continuously (elapsed ms × ${RATE} / 1000), not in one lump per second.`,
      ),
      rightValues(results, up),
      check(
        'Refused requests get a "rate_limited" error at once',
        !wrongCode && !slowNo && !wasted,
        wrongCode
          ? `${wrongCode.node} answered ${j(wrongCode.req)} with ${j(wrongCode.res)}. Expected { type: "error", code: "rate_limited" }.`
          : wasted
            ? `${wasted.node} refused "${wasted.key}" but still called the upstream for it. Take the token before calling.`
            : slowNo && `${slowNo.node} took ${ms(slowNo.done - slowNo.invoked)} to refuse "${slowNo.key}"; a refusal needs no upstream call and should arrive within 50ms.`,
      ),
    ];
  };
}

// ------------------------------------------------------------- bulkhead

const SLOTS = 4;

export function bulkhead(sim, { work = null, silent = 0, rps = 0, from = 200, bursts = null, minOk = 0, allOk = false }) {
  const up = makeUpstream(sim, () => {
    if (sim.rand() < silent) return { silent: true };
    return work ? { delay: work } : fast(sim);
  });
  const results = [];
  let n = 0;
  const end = sim.duration - 600;
  if (rps) {
    for (let t = from; t < end; t += 1000 / rps) {
      const key = 'k' + ++n;
      const client = 'c' + (1 + (n % 3));
      sim.at(t, () => ask(sim, results, sim.pick(sim.nodeIds), key, { timeout: 450, client }));
    }
  }
  if (bursts) {
    for (let t = from; t < end; t += bursts.every) {
      for (let i = 0; i < bursts.count; i++) {
        const key = 'k' + ++n;
        sim.at(t, () => ask(sim, results, sim.pick(sim.nodeIds), key, { timeout: 450, client: 'c' + (1 + i) }));
      }
    }
  }

  return () => {
    const ok = results.filter(isOk).length;
    const refused = results.filter((r) => r.res && r.res.type === 'error' && r.res.code === 'overloaded');
    const slowNo = refused.find((r) => r.done - r.invoked > 50);
    const wasted = refused.find((r) => up.callsFor(r.key).length > 0);
    const shedIdle = allOk && results.find((r) => r.res && !isOk(r));
    const lastOk = results.filter(isOk).pop();
    return [
      unanswered('Every request gets a reply', results, 'A request that finds no free slot is answered with an error, not parked.'),
      within(`Replies arrive within ${DEADLINE}ms`, results, DEADLINE, 'Do not queue requests behind the limit: refuse them. Keep the 200ms timeout on upstream calls.'),
      check(
        `At most ${SLOTS} upstream calls in flight`,
        up.peak <= SLOTS,
        `At ${ms(up.peakAt)} the upstream was working on ${up.peak} calls from this node at once; the limit is ${SLOTS}. Count a call when you send it and refuse new requests while the count is at the limit.`,
      ),
      check(
        allOk ? 'Nothing is refused while slots are free' : 'The slots stay in use under overload',
        allOk ? !shedIdle : ok >= minOk,
        allOk
          ? shedIdle && `${shedIdle.node} answered ${j(shedIdle.req)} with ${j(shedIdle.res)} although the upstream never had more than ${up.peak} calls in flight. Refuse only when all ${SLOTS} slots are taken.`
          : `Only ${ok} of ${results.length} requests succeeded; with ${SLOTS} slots at least ${minOk} should.${lastOk ? ` The last success was at ${ms(lastOk.done)}.` : ''} A slot must be given back when its call ends in any way: a reply, an error, or a timeout.`,
      ),
      rightValues(results, up),
      errorShape(results, ['overloaded', 'timeout']),
      check(
        'Refused requests get an "overloaded" error at once',
        !slowNo && !wasted,
        wasted
          ? `${wasted.node} refused "${wasted.key}" but still called the upstream for it.`
          : slowNo && `${slowNo.node} took ${ms(slowNo.done - slowNo.invoked)} to refuse "${slowNo.key}"; a refusal should arrive within 50ms.`,
      ),
    ];
  };
}
