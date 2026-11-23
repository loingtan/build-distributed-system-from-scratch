// Workloads: each one drives simulated clients against the cluster and
// returns a function that produces the list of checks once the run is over.
//
//   workload(sim, params) -> () => [{ name, ok, detail }]

export const check = (name, ok, detail = '') => ({ name, ok: !!ok, detail: ok ? '' : detail });
export const j = (v) => {
  const s = JSON.stringify(v);
  return s && s.length > 120 ? `${s.slice(0, 117)}…` : String(s);
};
export const sample = (arr, n = 4) => arr.slice(0, n).join(', ') + (arr.length > n ? ', …' : '');
export const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

// Split the cluster into a majority and a minority side.
export function split(sim) {
  const ids = [...sim.nodeIds];
  for (let i = ids.length - 1; i > 0; i--) {
    const k = Math.floor(sim.rand() * (i + 1));
    [ids[i], ids[k]] = [ids[k], ids[i]];
  }
  const cut = Math.floor(ids.length / 2);
  return [ids.slice(0, cut).sort(), ids.slice(cut).sort()];
}

export function schedulePartition(sim, partition) {
  if (!partition) return;
  sim.at(partition[0], () => sim.partition(split(sim)));
  sim.at(partition[1], () => sim.heal());
}

export function unanswered(name, results, what) {
  const lost = results.filter((r) => !r.res);
  return check(
    name,
    lost.length === 0,
    lost.length &&
      `${plural(lost.length, 'request')} of ${results.length} got no reply within the timeout. First: ${lost[0].node} never answered ${j(lost[0].req)}.${what ? ` ${what}` : ''}`,
  );
}

export function wrongType(results, expected) {
  const bad = results.find((r) => r.res && r.res.type !== expected);
  return check(
    `Replies have type "${expected}"`,
    !bad,
    bad && `${bad.node} replied ${j(bad.res)} to ${j(bad.req)}.`,
  );
}

// ---------------------------------------------------------------- echo

export function echo(sim, { ops = 20 }) {
  const results = [];
  const gap = (sim.duration - 400) / ops;
  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      const req = { type: 'echo', echo: `hello ${i + 1}` };
      const res = await sim.clientRpc(node, req);
      results.push({ node, req, res });
    });
  }
  return () => {
    const mismatch = results.find((r) => r.res && r.res.echo !== r.req.echo);
    return [
      unanswered('Every request gets a reply', results, 'Use node.reply(msg, body) so the reply is matched to its request.'),
      wrongType(results, 'echo_ok'),
      check(
        'The reply carries the same payload',
        !mismatch,
        mismatch && `Sent ${j(mismatch.req.echo)} but got back ${j(mismatch.res.echo)}.`,
      ),
    ];
  };
}

// ---------------------------------------------------------- unique ids

export function uniqueIds(sim, { ops = 200, partition, sortable = false }) {
  const results = [];
  const gap = (sim.duration - 400) / ops;
  schedulePartition(sim, partition);
  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      const req = { type: 'generate' };
      const invoked = sim.now();
      const res = await sim.clientRpc(node, req);
      results.push({ node, req, res, invoked, done: sim.now() });
    });
  }
  return () => {
    const seen = new Map();
    let dup = null;
    let missing = null;
    for (const r of results) {
      if (!r.res || r.res.type !== 'generate_ok') continue;
      if (r.res.id === undefined || r.res.id === null) {
        missing = missing ?? r;
        continue;
      }
      const key = JSON.stringify(r.res.id);
      if (seen.has(key) && !dup) dup = { id: r.res.id, a: seen.get(key), b: r.node };
      seen.set(key, r.node);
    }
    return [
      unanswered(
        'Every request gets a reply, even during a partition',
        results,
        'A node must be able to mint an ID without asking anyone else.',
      ),
      wrongType(results, 'generate_ok'),
      check('Every reply includes an id', !missing, missing && `${missing.node} replied ${j(missing.res)}.`),
      check(
        'All ids are unique across the cluster',
        !dup,
        dup &&
          (dup.a === dup.b
            ? `${dup.a} handed out ${j(dup.id)} twice.`
            : `${dup.a} and ${dup.b} both handed out ${j(dup.id)}.`),
      ),
      ...(sortable ? sortableChecks(results) : []),
    ];
  };
}

// Ids must be integers that sort roughly by creation time: an id issued
// well after another one finished must be larger, whichever node issued it.
function sortableChecks(results) {
  const ok = results.filter((r) => r.res && r.res.type === 'generate_ok');
  const notInt = ok.find((r) => !Number.isSafeInteger(r.res.id));
  const checks = [
    check('Ids are integers that fit in 53 bits', !notInt, notInt && `${notInt.node} handed out ${j(notInt.res.id)}. Use a whole number below 2^53 so it survives JSON.`),
  ];
  if (notInt) return checks;
  const SLACK = 50;
  const byDone = [...ok].sort((a, b) => a.done - b.done);
  let i = 0;
  let best = null;
  let wrong = null;
  for (const b of [...ok].sort((x, y) => x.invoked - y.invoked)) {
    while (i < byDone.length && byDone[i].done < b.invoked - SLACK) {
      if (!best || byDone[i].res.id > best.res.id) best = byDone[i];
      i++;
    }
    if (best && b.res.id <= best.res.id) {
      wrong ??= `${best.node} issued ${best.res.id} at ${Math.round(best.done)}ms; ${b.node} issued ${b.res.id} at ${Math.round(b.invoked)}ms, ${Math.round(b.invoked - best.done)}ms later, yet it sorts earlier.`;
    }
  }
  checks.push(check('Later ids sort after earlier ones (50ms of slack)', !wrong, wrong));
  return checks;
}

// ----------------------------------------------------------- broadcast

export function broadcast(sim, { ops = 40, partition, quiet = 1500, budget }) {
  const sends = [];
  const finals = [];
  const firstSeenEverywhere = new Map();
  const opsEnd = sim.duration - quiet - 300;
  const gap = (opsEnd - 100) / ops;
  schedulePartition(sim, partition);

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      const req = { type: 'broadcast', message: 1000 + i * 7 };
      const res = await sim.clientRpc(node, req);
      sends.push({ node, req, res, acked: sim.now() });
    });
  }

  if (budget) {
    // Sample every node regularly to see when each value has reached all of them.
    for (let t = 150; t < sim.duration - 300; t += 50) {
      sim.at(t, async () => {
        const at = sim.now();
        const reads = await Promise.all(
          sim.nodeIds.map((n) => sim.clientRpc(n, { type: 'read' }, { probe: true, timeout: 400 })),
        );
        if (reads.some((r) => !r || !Array.isArray(r.messages))) return;
        const sets = reads.map((r) => new Set(r.messages));
        for (const v of sets[0]) {
          if (!firstSeenEverywhere.has(v) && sets.every((s) => s.has(v))) firstSeenEverywhere.set(v, at);
        }
      });
    }
  }

  sim.at(sim.duration - 300, async () => {
    for (const node of sim.nodeIds) {
      const req = { type: 'read' };
      sim.clientRpc(node, req, { client: 'c2' }).then((res) => finals.push({ node, req, res }));
    }
  });

  return () => {
    const acked = sends.filter((s) => s.res && s.res.type === 'broadcast_ok').map((s) => s.req.message);
    const sent = new Set(sends.map((s) => s.req.message));
    const badRead = finals.find((f) => !f.res || f.res.type !== 'read_ok' || !Array.isArray(f.res.messages));
    const checks = [
      unanswered('Every broadcast is acknowledged', sends, 'Acknowledge straight away; do not wait for other nodes.'),
      wrongType(sends, 'broadcast_ok'),
      check(
        'Every node answers read with { type: "read_ok", messages: [...] }',
        finals.length === sim.nodeIds.length && !badRead,
        badRead ? `${badRead.node} replied ${j(badRead.res)}.` : 'Some nodes did not answer the final read.',
      ),
    ];
    if (badRead || finals.length !== sim.nodeIds.length) return checks;

    let lost = null;
    let extra = null;
    for (const f of finals) {
      const got = new Set(f.res.messages);
      const missing = acked.filter((v) => !got.has(v));
      if (missing.length && (!lost || missing.length > lost.missing.length)) lost = { node: f.node, missing };
      const unknown = f.res.messages.filter((v) => !sent.has(v));
      const dupes = f.res.messages.length - got.size;
      if (!extra && unknown.length) extra = `${f.node} returned ${j(unknown[0])}, which nobody broadcast.`;
      if (!extra && dupes) extra = `${f.node} returned ${plural(dupes, 'duplicate value')}.`;
    }
    checks.push(
      check(
        'Every acknowledged value reaches every node',
        !lost,
        lost &&
          `${lost.node} is missing ${lost.missing.length} of ${acked.length} values (e.g. ${sample(lost.missing)}) ${Math.round(quiet)}ms after the last broadcast.`,
      ),
      check('No invented or duplicate values', !extra, extra),
    );

    if (budget) {
      const perOp = sim.stats.nodeMsgs / Math.max(1, sends.length);
      checks.push(
        check(
          `At most ${budget.msgsPerOp} node-to-node messages per broadcast`,
          perOp <= budget.msgsPerOp,
          `Your nodes exchanged ${perOp.toFixed(1)} messages per broadcast (${sim.stats.nodeMsgs.toLocaleString()} in total). Batch values together and think about who really needs to talk to whom.`,
        ),
      );
      const lags = sends
        .filter((s) => s.res)
        .map((s) => (firstSeenEverywhere.has(s.req.message) ? firstSeenEverywhere.get(s.req.message) - s.acked : Infinity))
        .sort((a, b) => a - b);
      const worst = lags[lags.length - 1] ?? 0;
      checks.push(
        check(
          `Every value is everywhere within ${budget.maxLag}ms`,
          worst <= budget.maxLag,
          Number.isFinite(worst)
            ? `The slowest value took about ${Math.round(worst)}ms to reach all nodes.`
            : 'Some values never reached every node while the test was sampling.',
        ),
      );
    }
    return checks;
  };
}

// ------------------------------------------------------ lamport clocks

export function lamport(sim, { ops = 60, skew = false }) {
  const results = [];
  const lastReply = new Map(sim.nodeIds.map((n) => [n, -Infinity]));
  const floor = new Map(sim.nodeIds.map((n) => [n, -Infinity]));
  const notes = new Set();
  let noteCount = 0;
  const violations = { number: null, forward: null, receive: null };
  const isNode = (id) => sim.nodeIds.includes(id);

  sim.onSend = (msg) => {
    const { src, dest, body } = msg;
    if (!isNode(src)) return;
    if (isNode(dest) && body.type === 'note') {
      noteCount++;
      if (typeof body.ts !== 'number') {
        violations.number ??= `${src} sent a note to ${dest} with ts = ${j(body.ts)}.`;
        return;
      }
      notes.add(`${src}>${dest}:${body.ts}`);
      if (body.ts <= floor.get(src)) {
        violations.receive ??= `${src} received a note stamped ${floor.get(src)}, then sent one stamped ${body.ts}.`;
      }
      if (body.ts < lastReply.get(src)) {
        violations.forward ??= `${src} reported ts ${lastReply.get(src)}, then sent a note stamped ${body.ts}.`;
      }
    } else if (!isNode(dest) && (body.type === 'event_ok' || body.type === 'notify_ok')) {
      if (typeof body.ts !== 'number') {
        violations.number ??= `${src} replied ${j(body)}; ts must be a number.`;
        return;
      }
      if (body.ts <= lastReply.get(src)) {
        violations.forward ??= `${src} reported ts ${lastReply.get(src)} for one event and then ts ${body.ts} for a later one.`;
      }
      if (body.ts <= floor.get(src)) {
        violations.receive ??= `${src} received a note stamped ${floor.get(src)}, but stamped a later event ${body.ts}. An effect must be stamped later than its cause.`;
      }
      lastReply.set(src, Math.max(lastReply.get(src), body.ts));
    }
  };
  sim.onDeliver = (msg) => {
    if (isNode(msg.src) && isNode(msg.dest) && msg.body.type === 'note' && typeof msg.body.ts === 'number') {
      floor.set(msg.dest, Math.max(floor.get(msg.dest), msg.body.ts));
    }
  };

  const gap = (sim.duration - 500) / ops;
  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const busy = sim.nodeIds[0];
      const node = skew && sim.rand() < 0.85 ? busy : sim.pick(sim.nodeIds);
      const others = sim.nodeIds.filter((n) => n !== node);
      const req = sim.rand() < 0.4 ? { type: 'notify', to: sim.pick(others) } : { type: 'event' };
      const res = await sim.clientRpc(node, req);
      results.push({ node, req, res });
    });
  }

  return () => {
    const notifies = results.filter((r) => r.req.type === 'notify');
    const badType = results.find((r) => r.res && r.res.type !== `${r.req.type}_ok`);
    const unsent = notifies.find((r) => r.res && typeof r.res.ts === 'number' && !notes.has(`${r.node}>${r.req.to}:${r.res.ts}`));
    return [
      unanswered('Every request gets a reply', results),
      check('Replies are event_ok / notify_ok', !badType, badType && `${badType.node} replied ${j(badType.res)} to ${j(badType.req)}.`),
      check('Timestamps are numbers', !violations.number, violations.number),
      check(
        'A notify sends { type: "note", ts } to the target with the same ts it reports',
        !unsent && noteCount >= notifies.filter((r) => r.res).length,
        unsent
          ? `${unsent.node} reported ts ${unsent.res.ts} for a notify to ${unsent.req.to}, but no note with that ts was sent there.`
          : `Expected ${notifies.length} notes between nodes, saw ${noteCount}.`,
      ),
      check('Each node’s clock moves forward on every event', !violations.forward, violations.forward),
      check('Receiving a note pushes the clock past the note’s timestamp', !violations.receive, violations.receive),
    ];
  };
}

// ------------------------------------------------------------ counters

export function counter(sim, { ops = 80, partition, negative = false, quiet = 1500 }) {
  const adds = [];
  const reads = [];
  const finals = [];
  const opsEnd = sim.duration - quiet - 300;
  const gap = (opsEnd - 100) / ops;
  schedulePartition(sim, partition);

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      if (sim.rand() < 0.2) {
        const req = { type: 'read' };
        const invoked = adds.reduce((s, a) => s + Math.max(0, a.req.delta), 0);
        const res = await sim.clientRpc(node, req);
        reads.push({ node, req, res, invoked, at: sim.now() });
      } else {
        let delta = 1 + Math.floor(sim.rand() * 5);
        if (negative && sim.rand() < 0.45) delta = -delta;
        const req = { type: 'add', delta };
        const entry = { node, req, res: null };
        adds.push(entry);
        entry.res = await sim.clientRpc(node, req);
      }
    });
  }

  sim.at(sim.duration - 300, () => {
    for (const node of sim.nodeIds) {
      const req = { type: 'read' };
      sim.clientRpc(node, req, { client: 'c2' }).then((res) => finals.push({ node, req, res }));
    }
  });

  return () => {
    const total = adds.filter((a) => a.res && a.res.type === 'add_ok').reduce((s, a) => s + a.req.delta, 0);
    const badRead = [...reads, ...finals].find((r) => r.res && (r.res.type !== 'read_ok' || typeof r.res.value !== 'number'));
    const wrong = finals.find((f) => f.res && f.res.value !== total);
    const checks = [
      unanswered('Every add is acknowledged, even during a partition', adds, 'Record the add locally and reply; sync with peers in the background.'),
      wrongType(adds, 'add_ok'),
      unanswered('Every read gets a reply', [...reads, ...finals]),
      check('Reads return { type: "read_ok", value: <number> }', !badRead, badRead && `${badRead.node} replied ${j(badRead.res)}.`),
      check(
        `All nodes converge on the true total (${total})`,
        finals.length === sim.nodeIds.length && !wrong,
        wrong
          ? `${wrong.node} reports ${j(wrong.res.value)}; every node should report ${total}. Others: ${finals.map((f) => `${f.node}=${f.res?.value}`).join(', ')}.`
          : 'Some nodes did not answer the final read.',
      ),
    ];
    if (!negative) {
      let backwards = null;
      let over = null;
      const last = new Map();
      for (const r of [...reads].sort((a, b) => a.at - b.at)) {
        if (!r.res || typeof r.res.value !== 'number') continue;
        if (last.has(r.node) && r.res.value < last.get(r.node)) {
          backwards ??= `${r.node} reported ${last.get(r.node)} and later ${r.res.value}.`;
        }
        last.set(r.node, r.res.value);
      }
      const allInvoked = adds.reduce((s, a) => s + a.req.delta, 0);
      for (const r of [...reads, ...finals]) {
        if (r.res && typeof r.res.value === 'number' && r.res.value > allInvoked) {
          over ??= `${r.node} reported ${r.res.value}, but only ${allInvoked} was ever added. Something is being counted twice.`;
        }
      }
      checks.push(
        check('A node’s reads never go backwards', !backwards, backwards),
        check('Nothing is counted twice', !over, over),
      );
    }
    return checks;
  };
}

// ----------------------------------------------------------------- log

export function log(sim, { ops = 90, keys = 3, partition, quiet = 1200 }) {
  const keyNames = Array.from({ length: keys }, (_, i) => `k${i + 1}`);
  const sends = [];
  const polls = [];
  const commits = [];
  const finals = [];
  const listed = [];
  const known = new Map(keyNames.map((k) => [k, -1])); // highest acked offset per key, as seen by clients
  const opsEnd = sim.duration - quiet - 400;
  const gap = (opsEnd - 100) / ops;
  const timeout = 3000;
  schedulePartition(sim, partition);

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      const key = sim.pick(keyNames);
      const roll = sim.rand();
      if (roll < 0.6) {
        const req = { type: 'send', key, msg: 5000 + i };
        const entry = { node, req, res: null, invoke: sim.now(), complete: null };
        sends.push(entry);
        entry.res = await sim.clientRpc(node, req, { timeout });
        entry.complete = sim.now();
        if (entry.res && Number.isInteger(entry.res.offset)) known.set(key, Math.max(known.get(key), entry.res.offset));
      } else if (roll < 0.85) {
        const from = Math.max(0, known.get(key) - Math.floor(sim.rand() * 4));
        const req = { type: 'poll', offsets: { [key]: from } };
        const res = await sim.clientRpc(node, req, { timeout });
        polls.push({ node, req, res });
      } else if (known.get(key) >= 0) {
        const req = { type: 'commit_offsets', offsets: { [key]: known.get(key) } };
        const res = await sim.clientRpc(node, req, { timeout });
        commits.push({ node, req, res });
      }
    });
  }

  sim.at(sim.duration - Math.max(400, quiet * 0.7), () => {
    for (const node of sim.nodeIds) {
      const req = { type: 'poll', offsets: Object.fromEntries(keyNames.map((k) => [k, 0])) };
      sim.clientRpc(node, req, { client: 'c2', timeout: 4000 }).then((res) => finals.push({ node, req, res }));
      const req2 = { type: 'list_committed_offsets', keys: keyNames };
      sim.clientRpc(node, req2, { client: 'c2', timeout: 4000 }).then((res) => listed.push({ node, req: req2, res }));
    }
  });

  return () => {
    const acked = sends.filter((s) => s.res && s.res.type === 'send_ok');
    const badOffset = acked.find((s) => !Number.isInteger(s.res.offset));
    const byMsg = new Map(sends.map((s) => [s.req.msg, s]));

    let dup = null;
    const taken = new Map();
    for (const s of acked) {
      const id = `${s.req.key}@${s.res.offset}`;
      if (taken.has(id)) dup ??= `Offset ${s.res.offset} of ${s.req.key} was given to both message ${taken.get(id).req.msg} (by ${taken.get(id).node}) and ${s.req.msg} (by ${s.node}).`;
      taken.set(id, s);
    }

    let reorder = null;
    for (const k of keyNames) {
      const list = acked.filter((s) => s.req.key === k).sort((a, b) => a.complete - b.complete);
      let maxDone = null;
      for (const s of [...list].sort((a, b) => a.invoke - b.invoke)) {
        // Any send that completed before this one started must have a smaller offset.
        for (const prev of list) {
          if (prev.complete >= s.invoke) break;
          if (!maxDone || prev.res.offset > maxDone.res.offset) maxDone = prev;
        }
        if (maxDone && maxDone.complete < s.invoke && s.res.offset <= maxDone.res.offset) {
          reorder ??= `On ${k}, message ${maxDone.req.msg} got offset ${maxDone.res.offset} (via ${maxDone.node}); message ${s.req.msg} was sent after that finished but got offset ${s.res.offset} (via ${s.node}).`;
        }
      }
    }

    let badPoll = null;
    const inspect = (p) => {
      if (!p.res) return;
      if (p.res.type !== 'poll_ok' || typeof p.res.msgs !== 'object' || p.res.msgs === null) {
        badPoll ??= `${p.node} replied ${j(p.res)} to a poll.`;
        return;
      }
      for (const [k, from] of Object.entries(p.req.offsets)) {
        const list = p.res.msgs[k] ?? [];
        if (!Array.isArray(list)) {
          badPoll ??= `${p.node} returned ${j(list)} for ${k}; expected a list of [offset, msg] pairs.`;
          continue;
        }
        let prev = -Infinity;
        for (const pair of list) {
          if (!Array.isArray(pair) || pair.length !== 2) {
            badPoll ??= `${p.node} returned ${j(pair)} in a poll; each entry must be [offset, msg].`;
            break;
          }
          const [off, msg] = pair;
          const origin = byMsg.get(msg);
          if (off < from) badPoll ??= `Poll of ${k} from offset ${from} returned offset ${off}.`;
          if (off <= prev) badPoll ??= `Poll of ${k} returned offset ${off} after ${prev}; offsets must increase.`;
          if (!origin || origin.req.key !== k) badPoll ??= `Poll of ${k} returned message ${j(msg)}, which was never sent to ${k}.`;
          else if (origin.res && origin.res.offset !== off) badPoll ??= `Message ${msg} was acknowledged at offset ${origin.res.offset} but polled at offset ${off}.`;
          prev = off;
        }
      }
    };
    [...polls, ...finals].forEach(inspect);

    let lost = null;
    for (const f of finals) {
      if (!f.res || !f.res.msgs) continue;
      for (const k of keyNames) {
        const got = new Set((Array.isArray(f.res.msgs[k]) ? f.res.msgs[k] : []).map((p) => p && p[1]));
        const missing = acked.filter((s) => s.req.key === k && !got.has(s.req.msg));
        if (missing.length) lost ??= `${f.node}: polling ${k} from 0 is missing ${missing.length} acknowledged ${missing.length === 1 ? 'message' : 'messages'} (e.g. msg ${missing[0].req.msg} at offset ${missing[0].res.offset}).`;
      }
    }

    const want = {};
    for (const c of commits) {
      if (!c.res) continue;
      for (const [k, o] of Object.entries(c.req.offsets)) want[k] = Math.max(want[k] ?? -1, o);
    }
    let badCommit = null;
    for (const l of listed) {
      if (!l.res) continue;
      if (l.res.type !== 'list_committed_offsets_ok' || typeof l.res.offsets !== 'object' || l.res.offsets === null) {
        badCommit ??= `${l.node} replied ${j(l.res)}.`;
        continue;
      }
      for (const [k, o] of Object.entries(want)) {
        if (l.res.offsets[k] !== o) badCommit ??= `${l.node} lists ${k} committed at ${j(l.res.offsets[k])}; the highest committed offset was ${o}.`;
      }
    }
    const badCommitAck = commits.find((c) => c.res && c.res.type !== 'commit_offsets_ok');

    return [
      unanswered('Every request gets a reply', [...sends, ...polls, ...commits, ...finals, ...listed]),
      wrongType(sends, 'send_ok'),
      check('send_ok carries an integer offset', !badOffset, badOffset && `${badOffset.node} replied ${j(badOffset.res)}.`),
      check('No two messages share an offset', !dup, dup),
      check('Offsets follow the order sends were acknowledged in', !reorder, reorder),
      check('Polls return [offset, msg] pairs in order, from the requested offset', !badPoll, badPoll),
      check('No acknowledged message is lost', finals.length === sim.nodeIds.length && !lost, lost ?? 'Some nodes did not answer the final poll.'),
      check(
        'Committed offsets are remembered and never move backwards',
        !badCommit && !badCommitAck && listed.length === sim.nodeIds.length,
        badCommit ?? (badCommitAck ? `${badCommitAck.node} replied ${j(badCommitAck.res)} to commit_offsets.` : 'Some nodes did not answer list_committed_offsets.'),
      ),
    ];
  };
}

// -------------------------------------------------------- transactions

export function txn(sim, { ops = 60, keys = 5, partition, quiet = 1500 }) {
  const multi = sim.nodeIds.length > 1;
  const results = [];
  const finals = [];
  const opsEnd = sim.duration - (multi ? quiet : 0) - 400;
  const gap = (opsEnd - 100) / ops;
  let nextValue = 1;
  schedulePartition(sim, partition);

  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(sim.nodeIds);
      const size = 1 + Math.floor(sim.rand() * 4);
      const body = [];
      for (let n = 0; n < size; n++) {
        const key = Math.floor(sim.rand() * keys);
        body.push(sim.rand() < 0.5 ? ['r', key, null] : ['w', key, nextValue++]);
      }
      const req = { type: 'txn', txn: body };
      const entry = { node, req, res: null, op: null };
      results.push(entry);
      entry.res = await sim.clientRpc(node, req, { client: `c${1 + (i % 3)}` });
      entry.op = sim.ops.find((o) => o.req === req);
    });
  }

  sim.at(sim.duration - 400, () => {
    for (const node of sim.nodeIds) {
      const req = { type: 'txn', txn: Array.from({ length: keys }, (_, k) => ['r', k, null]) };
      sim.clientRpc(node, req, { client: 'c4' }).then((res) => finals.push({ node, req, res }));
    }
  });

  return () => {
    const all = [...results, ...finals];
    let shape = null;
    for (const r of all) {
      if (!r.res || r.res.type !== 'txn_ok') continue;
      const out = r.res.txn;
      if (!Array.isArray(out) || out.length !== r.req.txn.length) {
        shape ??= `${r.node} replied ${j(r.res)} to ${j(r.req.txn)}; txn_ok must list the same operations, in order.`;
        continue;
      }
      out.forEach((o, i) => {
        const [f, k, v] = r.req.txn[i];
        if (!Array.isArray(o) || o[0] !== f || o[1] !== k || (f === 'w' && o[2] !== v)) {
          shape ??= `${r.node} answered operation ${j(r.req.txn[i])} with ${j(o)}.`;
        }
      });
    }
    const checks = [
      unanswered(multi ? 'Every transaction is answered, even during a partition' : 'Every transaction is answered', all, multi ? 'Total availability means a node never waits for its peers before replying.' : ''),
      wrongType(all, 'txn_ok'),
      check('txn_ok echoes the operations with reads filled in', !shape, shape),
    ];
    if (shape) return checks;

    const good = (r) => r.res && r.res.type === 'txn_ok';

    if (!multi) {
      // One node: transactions must behave as if run one at a time, in arrival order.
      const model = new Map();
      let wrong = null;
      const ordered = all.filter(good).sort((a, b) => txnOrder(a, sim) - txnOrder(b, sim));
      for (const r of ordered) {
        r.res.txn.forEach(([f, k, v]) => {
          if (f === 'w') model.set(k, v);
          else if ((model.get(k) ?? null) !== v) {
            wrong ??= `Reading key ${k} returned ${j(v)}, but the last value written to it was ${j(model.get(k) ?? null)}.`;
          }
        });
      }
      checks.push(check('Reads return the latest written value', !wrong, wrong));
      return checks;
    }

    const written = new Map();
    for (const r of results) for (const [f, k, v] of r.req.txn) if (f === 'w') (written.get(k) ?? written.set(k, new Set()).get(k)).add(v);

    let internal = null;
    let garbage = null;
    for (const r of all.filter(good)) {
      const local = new Map();
      r.res.txn.forEach(([f, k, v]) => {
        if (f === 'w') local.set(k, v);
        else {
          if (local.has(k) && local.get(k) !== v) internal ??= `A transaction on ${r.node} wrote ${local.get(k)} to key ${k}, then read ${j(v)} back from it.`;
          if (v !== null && !(written.get(k)?.has(v))) garbage ??= `${r.node} read ${j(v)} from key ${k}, but nobody ever wrote that value there.`;
          local.set(k, v);
        }
      });
    }

    let diverged = null;
    let vanished = null;
    const views = finals.filter(good).map((f) => ({ node: f.node, values: f.res.txn.map((o) => o[2]) }));
    for (let k = 0; k < keys && views.length; k++) {
      const first = views[0].values[k];
      const other = views.find((v) => v.values[k] !== first);
      if (other) diverged ??= `After the network settled, key ${k} is ${j(first)} on ${views[0].node} but ${j(other.values[k])} on ${other.node}.`;
      const ackedWrite = results.some((r) => good(r) && r.req.txn.some(([f, key]) => f === 'w' && key === k));
      if (ackedWrite && first === null) vanished ??= `Key ${k} was written, but ${views[0].node} reads null at the end.`;
    }
    checks.push(
      check('A transaction sees its own writes', !internal, internal),
      check('Reads only return values that were really written', !garbage, garbage),
      check('All nodes converge on the same value for every key', views.length === sim.nodeIds.length && !diverged, diverged ?? 'Some nodes did not answer the final read.'),
      check('Acknowledged writes do not vanish', !vanished, vanished),
    );
    return checks;
  };
}

function txnOrder(r, sim) {
  const op = sim.ops.find((o) => o.req === r.req);
  return op?.msg?.s1 ?? Infinity;
}

// ------------------------------------------------------ leader election

export function election(sim, { scenario = 'stable' }) {
  const obs = [];
  const events = { partitions: [] };
  const roles = new Set(['leader', 'follower', 'candidate']);
  let malformed = null;

  const poll = async () => {
    const at = sim.now();
    const replies = await Promise.all(
      sim.nodeIds.map((n) => sim.clientRpc(n, { type: 'status' }, { probe: true, timeout: 300 })),
    );
    replies.forEach((res, i) => {
      const node = sim.nodeIds[i];
      if (!res) return obs.push({ t: at, node, silent: true });
      if (res.type !== 'status_ok' || !roles.has(res.role) || !Number.isInteger(res.term)) {
        malformed ??= `${node} replied ${j(res)}; expected { type: "status_ok", role, term, leader }.`;
        return;
      }
      obs.push({ t: at, node, role: res.role, term: res.term, leader: res.leader ?? null });
    });
  };
  for (let t = 100; t < sim.duration; t += 100) sim.at(t, poll);

  const currentLeader = () => {
    const recent = obs.filter((o) => o.t >= sim.now() - 250 && o.role === 'leader');
    recent.sort((a, b) => b.term - a.term || b.t - a.t);
    return recent[0]?.node ?? null;
  };
  const cut = (minority) => {
    const rest = sim.nodeIds.filter((n) => !minority.includes(n));
    sim.partition([minority, rest]);
    events.partitions.push({ start: sim.now(), end: null, majority: rest });
  };
  const heal = () => {
    sim.heal();
    events.partitions[events.partitions.length - 1].end = sim.now();
  };

  if (scenario === 'isolate-leader') {
    sim.at(2500, () => cut([currentLeader() ?? sim.nodeIds[0]]));
    sim.at(6000, heal);
  } else if (scenario === 'churn') {
    sim.at(2500, () => {
      const leader = currentLeader() ?? sim.nodeIds[0];
      const other = sim.pick(sim.nodeIds.filter((n) => n !== leader));
      cut([leader, other]);
    });
    sim.at(5500, heal);
    sim.at(7500, () => cut([currentLeader() ?? sim.nodeIds[1]]));
    sim.at(10500, heal);
  }

  return () => {
    const seen = obs.filter((o) => !o.silent);
    const silent = obs.find((o) => o.silent);
    const checks = [
      check('Every node answers status', !silent && seen.length > 0, silent ? `${silent.node} did not answer a status request at ${Math.round(silent.t)}ms.` : 'No status replies were received.'),
      check('status_ok has a valid role and an integer term', !malformed, malformed),
    ];
    if (malformed || !seen.length) return checks;

    const byTerm = new Map();
    let twoLeaders = null;
    for (const o of seen) {
      if (o.role !== 'leader') continue;
      const who = byTerm.get(o.term);
      if (who && who !== o.node) twoLeaders ??= `Both ${who} and ${o.node} claimed to be leader in term ${o.term}. A node may vote only once per term.`;
      byTerm.set(o.term, who ?? o.node);
    }

    let backwards = null;
    const lastTerm = new Map();
    for (const o of seen) {
      if (lastTerm.has(o.node) && o.term < lastTerm.get(o.node)) backwards ??= `${o.node} went from term ${lastTerm.get(o.node)} back to term ${o.term}.`;
      lastTerm.set(o.node, o.term);
    }

    const first = seen.find((o) => o.role === 'leader');
    checks.push(
      check('At most one leader per term', !twoLeaders, twoLeaders),
      check('Terms never go backwards', !backwards, backwards),
      check('A leader is elected within 2s of startup', first && first.t <= 2000, first ? `The first leader appeared at ${Math.round(first.t)}ms.` : 'No node ever reported role "leader".'),
    );

    for (const p of events.partitions) {
      const before = Math.max(0, ...seen.filter((o) => o.t < p.start && o.role === 'leader').map((o) => o.term));
      const fresh = seen.find((o) => o.t > p.start && o.t <= p.start + 2500 && p.majority.includes(o.node) && o.role === 'leader' && (o.term > before || p.majority.includes(byTerm.get(before))));
      checks.push(
        check(
          `The majority side has a leader within 2.5s of the partition at ${Math.round(p.start)}ms`,
          !!fresh,
          `Nodes ${p.majority.join(', ')} could still talk to each other, but none of them led within 2.5s. Followers need to notice missing heartbeats and start an election.`,
        ),
      );
    }

    const tail = seen.filter((o) => o.t >= sim.duration - 1000);
    const leaders = new Set(tail.filter((o) => o.role === 'leader').map((o) => o.node));
    const terms = new Set(tail.map((o) => o.term));
    const [leader] = leaders;
    const disagree = tail.find((o) => o.leader !== leader);
    let settle = '';
    if (leaders.size !== 1) settle = leaders.size ? `Nodes ${[...leaders].join(' and ')} both claimed leadership in the last second.` : 'Nobody was leader in the last second of the test.';
    else if (terms.size !== 1) settle = `Terms were still changing in the last second (${[...terms].sort((a, b) => a - b).join(', ')}).`;
    else if (disagree) settle = `${disagree.node} reports leader = ${j(disagree.leader)}, but the leader is ${leader}.`;
    checks.push(check('The cluster settles on one leader that everyone agrees on', !settle, settle));
    return checks;
  };
}

// ------------------------------------------------- idempotent requests

// Clients retry: the same request (same request_id) may arrive up to three times.
export function dedup(sim, { ops = 60 }) {
  const sends = [];
  const finals = [];
  let total = 0;
  const gap = (sim.duration - 900) / ops;
  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, () => {
      const node = sim.pick(sim.nodeIds);
      const delta = 1 + Math.floor(sim.rand() * 9);
      const copies = sim.rand() < 0.5 ? 1 : sim.rand() < 0.6 ? 2 : 3;
      const req = { type: 'add', delta, request_id: `req-${i + 1}` };
      total += delta;
      for (let c = 0; c < copies; c++) {
        sim.at(sim.now() + c * (15 + sim.rand() * 60), async () => {
          const entry = { node, req, res: null };
          sends.push(entry);
          entry.res = await sim.clientRpc(node, req);
        });
      }
    });
  }
  sim.at(sim.duration - 300, () => {
    for (const node of sim.nodeIds) {
      const req = { type: 'read' };
      sim.clientRpc(node, req, { client: 'c2' }).then((res) => finals.push({ node, req, res }));
    }
  });
  return () => {
    const bad = finals.find((f) => f.res && (f.res.type !== 'read_ok' || typeof f.res.value !== 'number'));
    const wrong = finals.find((f) => f.res && f.res.value !== total);
    const retried = sends.length - ops;
    return [
      unanswered('Every copy of a request is answered', sends, 'A retry must get add_ok too, or the client will keep retrying.'),
      wrongType(sends, 'add_ok'),
      unanswered('The read gets a reply', finals),
      check('Reads return { type: "read_ok", value: <number> }', !bad, bad && `${bad.node} replied ${j(bad.res)}.`),
      check(
        `Each request counts once (total ${total})`,
        finals.length === sim.nodeIds.length && !wrong,
        wrong
          ? `${wrong.node} reports ${j(wrong.res.value)}, expected ${total}. ${retried} of the ${sends.length} messages were retries carrying a request_id the node had already seen.`
          : 'The node did not answer the final read.',
      ),
    ];
  };
}

// -------------------------------------------------------- vector clocks

export function vector(sim, { ops = 60 }) {
  const results = [];
  const ids = sim.nodeIds;
  const isNode = (id) => ids.includes(id);
  const zero = () => Object.fromEntries(ids.map((n) => [n, 0]));
  const truth = new Map(ids.map((n) => [n, zero()]));
  const tickedIn = new Map();
  const noteTruth = new WeakMap();
  let malformed = null;
  let wrongReply = null;
  let wrongNote = null;
  let noteCount = 0;

  const tick = (node) => {
    if (tickedIn.get(node) === sim.eventId()) return;
    tickedIn.set(node, sim.eventId());
    truth.get(node)[node] += 1;
  };
  const matches = (clock, want) => clock && typeof clock === 'object' && !Array.isArray(clock) && ids.every((n) => (clock[n] ?? 0) === want[n]);

  sim.onSend = (msg) => {
    const { src, dest, body } = msg;
    if (!isNode(src)) return;
    if (isNode(dest) && body.type === 'note') {
      noteCount++;
      tick(src);
      const want = { ...truth.get(src) };
      noteTruth.set(msg, want);
      if (!matches(body.clock, want)) wrongNote ??= `${src} sent a note to ${dest} with clock ${j(body.clock)}; expected ${j(want)}.`;
    } else if (!isNode(dest) && (body.type === 'event_ok' || body.type === 'notify_ok')) {
      tick(src);
      const want = truth.get(src);
      if (!body.clock || typeof body.clock !== 'object' || Array.isArray(body.clock)) malformed ??= `${src} replied ${j(body)}; clock must be an object such as {"n1": 2, "n2": 0}.`;
      else if (!matches(body.clock, want)) wrongReply ??= `${src} reported ${j(body.clock)}; with the events and notes it has seen so far the clock should be ${j(want)}.`;
    }
  };
  sim.onDeliver = (msg) => {
    const sent = noteTruth.get(msg);
    if (!sent) return;
    const mine = truth.get(msg.dest);
    for (const n of ids) mine[n] = Math.max(mine[n], sent[n]);
    mine[msg.dest] += 1;
  };

  const gap = (sim.duration - 500) / ops;
  for (let i = 0; i < ops; i++) {
    sim.at(100 + i * gap, async () => {
      const node = sim.pick(ids);
      const req = sim.rand() < 0.45 ? { type: 'notify', to: sim.pick(ids.filter((n) => n !== node)) } : { type: 'event' };
      const res = await sim.clientRpc(node, req);
      results.push({ node, req, res });
    });
  }

  return () => {
    const notifies = results.filter((r) => r.req.type === 'notify' && r.res).length;
    const badType = results.find((r) => r.res && r.res.type !== `${r.req.type}_ok`);
    return [
      unanswered('Every request gets a reply', results),
      check('Replies are event_ok / notify_ok', !badType, badType && `${badType.node} replied ${j(badType.res)} to ${j(badType.req)}.`),
      check('Replies carry a clock object', !malformed, malformed),
      check('Every notify sends a note', noteCount >= notifies, `Expected ${notifies} notes between nodes, saw ${noteCount}.`),
      check('Notes carry the sender’s clock', !wrongNote, wrongNote),
      check('Clocks count exactly the events each node knows about', !wrongReply, wrongReply),
    ];
  };
}

// ----------------------------------------------------- failure detector

export function detector(sim, { isolate }) {
  const obs = [];
  let malformed = null;
  let victim = null;
  const poll = async () => {
    const at = sim.now();
    const replies = await Promise.all(sim.nodeIds.map((n) => sim.clientRpc(n, { type: 'status' }, { probe: true, timeout: 300 })));
    // A reply that comes back after the network changed (partition or heal)
    // could describe either side of the change, so it is not judged.
    const done = sim.now();
    if (isolate && isolate.some((edge) => at < edge && done > edge)) return;
    replies.forEach((res, i) => {
      const node = sim.nodeIds[i];
      if (!res) return obs.push({ t: at, node, silent: true });
      if (res.type !== 'status_ok' || !Array.isArray(res.suspected)) {
        malformed ??= `${node} replied ${j(res)}; expected { type: "status_ok", suspected: [...] }.`;
        return;
      }
      obs.push({ t: at, node, suspected: [...res.suspected].sort() });
    });
  };
  for (let t = 100; t < sim.duration; t += 100) sim.at(t, poll);
  if (isolate) {
    sim.at(isolate[0], () => {
      victim = sim.pick(sim.nodeIds);
      sim.partition([[victim], sim.nodeIds.filter((n) => n !== victim)]);
    });
    sim.at(isolate[1], () => sim.heal());
  }

  return () => {
    const seen = obs.filter((o) => !o.silent);
    const silent = obs.find((o) => o.silent);
    const checks = [
      check('Every node answers status', !silent && seen.length > 0, silent ? `${silent.node} did not answer at ${Math.round(silent.t)}ms.` : 'No status replies.'),
      check('status_ok lists suspected node ids', !malformed, malformed),
    ];
    if (malformed || !seen.length) return checks;

    const WARM = 1000;
    const REACT = 1500;
    const calm = (o) => o.t >= WARM && (!isolate || o.t < isolate[0] || o.t >= isolate[1] + REACT);
    const falsePos = seen.find((o) => calm(o) && o.suspected.length > 0);
    checks.push(
      check(
        'No false suspicions while every node is reachable',
        !falsePos,
        falsePos && `At ${Math.round(falsePos.t)}ms ${falsePos.node} suspected ${falsePos.suspected.join(', ')}, although all nodes were up and connected. One late or lost heartbeat should not be enough.`,
      ),
    );
    if (isolate) {
      const others = sim.nodeIds.filter((n) => n !== victim);
      const during = seen.filter((o) => o.t >= isolate[0] + REACT && o.t < isolate[1]);
      const missed = during.find((o) => o.node !== victim && !o.suspected.includes(victim));
      const blamed = during.find((o) => o.node !== victim && o.suspected.some((n) => n !== victim));
      const lonely = during.find((o) => o.node === victim && o.suspected.length !== others.length);
      checks.push(
        check(`The unreachable node (${victim}) is suspected within 1.5s`, !missed, missed && `${Math.round(missed.t - isolate[0])}ms after ${victim} was cut off, ${missed.node} still did not suspect it.`),
        check('Only the unreachable node is suspected', !blamed, blamed && `${blamed.node} suspected ${blamed.suspected.join(', ')}; only ${victim} was unreachable.`),
        check('The isolated node suspects everyone else', !lonely, lonely && `${victim} could reach nobody, but suspected only [${lonely.suspected.join(', ')}].`),
      );
      const after = seen.find((o) => o.t >= isolate[1] + REACT && o.suspected.length > 0);
      checks.push(check('Suspicion is withdrawn within 1.5s of the network healing', !after, after && `${after.node} still suspected ${after.suspected.join(', ')} at ${Math.round(after.t)}ms.`));
    }
    return checks;
  };
}

export const workloads = { echo, uniqueIds, broadcast, lamport, counter, log, txn, election, dedup, vector, detector };
