// Workloads for the "Byzantine faults" track.
//
// The learner's code runs on every node, including the Byzantine one. The
// harness plays the traitor by rewriting what that node says: sim.intercept
// sees every message as it is sent, and for messages from the traitor it
// replaces the body (or drops the message) according to a script. The
// traitor can therefore lie, send different things to different peers and
// stay silent, but it cannot fake `msg.src` (links are authenticated) and it
// cannot sign with anyone else's key.
//
// Every brief fixes the message formats, so the scripts may key on message
// types. Honest nodes are only ever observed through `read`.

import { check, j, plural } from '../workloads.js';

const F = 1;
const QUORUM = 2 * F + 1;

// The toy signature scheme of the starters (FNV-1a over key|text). The
// harness knows every key, but a traitor only ever signs with its own.
const KEYS = {
  n1: 'key-n1-7f3a',
  n2: 'key-n2-19be',
  n3: 'key-n3-c05d',
  n4: 'key-n4-62f8',
  client: 'key-client-d4e1',
};
function digest(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return ('0000000' + h.toString(16)).slice(-8);
}
const signAs = (who, text) => digest(KEYS[who] + '|' + text);
const validSig = (who, text, sig) => typeof sig === 'string' && KEYS[who] !== undefined && signAs(who, text) === sig;

const READ_TIMEOUT = 500;

// Replace a message body in place: the engine delivers the very object the
// interceptor was handed.
function replace(body, next) {
  for (const k of Object.keys(body)) delete body[k];
  Object.assign(body, JSON.parse(JSON.stringify(next)));
}

// Install the traitor. `script(m)` is called for every node-to-node message
// sent by the traitor and may return 'drop', a new body, { body, delay }, or
// undefined (pass unchanged). `inbound(m)` may rewrite what honest nodes tell the traitor
// (only the traitor's own view changes) and `delay(m)` may hold back honest
// messages to stage an order. Every lie is recorded for the check details.
function traitor(sim, id, { script, inbound, delay } = {}) {
  const lies = [];
  sim.intercept = (m) => {
    if (m.kind !== 'node') return undefined;
    if (m.src === id && m.dest !== id) {
      let out = script ? script(m) : undefined;
      if (out === 'drop') return 'drop';
      let hold;
      if (out && typeof out === 'object' && out.body && !out.type) {
        hold = out.delay;
        out = out.body;
      }
      if (out && typeof out === 'object') {
        if (lies.length < 40) lies.push({ t: m.t, dest: m.dest, was: j(m.body), now: j(out) });
        replace(m.body, out);
      }
      return hold;
    }
    if (m.dest === id && inbound) {
      const out = inbound(m);
      if (out && typeof out === 'object') replace(m.body, out);
      return undefined;
    }
    if (delay) return delay(m);
    return undefined;
  };
  return lies;
}

// Read every honest node in parallel at `at`; returns the result records.
function finalReads(sim, honest, at) {
  const reads = honest.map((node) => ({ node, req: { type: 'read' }, res: null, done: false }));
  sim.at(at, () =>
    Promise.all(
      reads.map(async (r) => {
        r.res = await sim.clientRpc(r.node, r.req, { timeout: READ_TIMEOUT });
        r.done = true;
      }),
    ),
  );
  return reads;
}

function readsAnswered(reads, at, expected) {
  const lost = reads.filter((r) => !r.res || r.res.type !== expected);
  return check(
    'Every honest node answered the final read',
    lost.length === 0,
    lost.length &&
      `${lost[0].node} ${lost[0].res ? 'replied ' + j(lost[0].res) : 'did not answer'} to ${j(lost[0].req)} sent at ${at}ms (expected "${expected}"). The other checks need this answer.`,
  );
}

const story = (lies, who) =>
  lies.length ? ` The traitor ${who} said, for example: ` + lies.slice(0, 3).map((l) => `to ${l.dest} at ${Math.round(l.t)}ms ${l.now}`).join('; ') + '.' : '';

// ------------------------------------------------- reliable broadcast
//
// Clients ask one node to broadcast a value; honest nodes must deliver it
// (validity), all deliver the same value per sender (agreement), and all or
// none deliver it (totality), even when the sender itself is the traitor.

export function bftBroadcast(sim, { scenario = 'silent' }) {
  const ids = sim.nodeIds;
  const byz = scenario === 'totality' || scenario === 'split' ? 'n1' : 'n4';
  const honest = ids.filter((id) => id !== byz);
  const sends = []; // { sender, value, t, honest }
  const send = (sender, value, t) =>
    sim.at(t, async () => {
      sends.push({ sender, value, t, honest: sender !== byz });
      await sim.clientRpc(sender, { type: 'broadcast', value }, { timeout: 500 });
    });

  let lies = [];
  if (scenario === 'silent') {
    lies = traitor(sim, byz, { script: () => 'drop' });
    send('n1', 'apple', 100);
    send('n2', 'pear', 300);
    send('n3', 'plum', 500);
  } else if (scenario === 'liar') {
    // n4 echoes and readies a forged value for every sender, and proposes
    // its own broadcast to n1 only.
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (b.type === 'echo' || b.type === 'ready') return { type: b.type, sender: b.sender, value: 'forged-' + b.value };
        if (b.type === 'propose') return m.dest === 'n1' ? { type: 'propose', value: 'fig' } : 'drop';
        return undefined;
      },
    });
    send('n1', 'apple', 100);
    send('n2', 'pear', 300);
    send('n4', 'fig', 500);
  } else if (scenario === 'totality') {
    // n1 proposes apple to n2 and n3 and cherry to n4, echoes the same way,
    // and sends its ready to n2 alone. n2 can deliver; n3 and n4 must then
    // be pulled along by the ready messages of the others.
    const v = { n2: 'apple', n3: 'apple', n4: 'cherry' };
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (b.type === 'propose') return { type: 'propose', value: v[m.dest] };
        if (b.type === 'echo' && b.sender === byz) return { type: 'echo', sender: byz, value: v[m.dest] };
        if (b.type === 'ready' && b.sender === byz) return m.dest === 'n2' ? { type: 'ready', sender: byz, value: 'apple' } : 'drop';
        return 'drop';
      },
    });
    send('n1', 'apple', 100);
    send('n2', 'pear', 700);
  } else if (scenario === 'split') {
    // n1 tells n2 apple and n3, n4 cherry in everything it sends.
    const v = { n2: 'apple', n3: 'cherry', n4: 'cherry' };
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (b.type === 'propose') return { type: 'propose', value: v[m.dest] };
        if ((b.type === 'echo' || b.type === 'ready') && b.sender === byz) return { type: b.type, sender: byz, value: v[m.dest] };
        return 'drop';
      },
    });
    send('n1', 'apple', 100);
    send('n3', 'plum', 700);
  }

  const READ_AT = sim.duration - 600;
  const reads = finalReads(sim, honest, READ_AT);

  return () => {
    const checks = [readsAnswered(reads, READ_AT, 'read_ok')];
    const got = reads.filter((r) => r.res && r.res.type === 'read_ok').map((r) => ({ node: r.node, d: (r.res.delivered && typeof r.res.delivered === 'object') ? r.res.delivered : {} }));
    const who = `${byz} (Byzantine)`;

    // Validity: every honest sender's value reaches every honest node.
    let bad = null;
    for (const s of sends.filter((x) => x.honest)) {
      for (const g of got) {
        if (g.d[s.sender] !== s.value && !bad) bad = { s, g };
      }
    }
    checks.push(
      check(
        'Honest senders: every honest node delivers their value',
        !bad,
        bad &&
          `${bad.s.sender} is honest and was asked at ${bad.s.t}ms to broadcast ${j(bad.s.value)}, but ${bad.g.node} delivered ${j(bad.g.d[bad.s.sender] ?? null)} for it by ${READ_AT}ms. ` +
            (bad.g.d[bad.s.sender] === undefined
              ? 'Count echo and ready messages per sender and per value, from distinct nodes: 3 (2f+1) echoes send a ready, 3 readies deliver.'
              : 'Count only messages that carry the same value; one node lying must not be enough to change what you deliver.') +
            story(lies, who),
      ),
    );

    // Nothing is delivered for a sender that never broadcast.
    const used = new Set(sends.map((s) => s.sender));
    let phantom = null;
    for (const g of got) for (const k of Object.keys(g.d)) if (!used.has(k) && !phantom) phantom = { g, k };
    checks.push(
      check(
        'Nothing is delivered for a node that never broadcast',
        !phantom,
        phantom && `${phantom.g.node} delivered ${j(phantom.g.d[phantom.k])} for ${phantom.k}, which never received a broadcast request. The sender of a propose is msg.src, never a field in the body.` + story(lies, who),
      ),
    );

    // Agreement and totality, per sender (the interesting one is the traitor).
    for (const s of used) {
      const vals = got.filter((g) => g.d[s] !== undefined).map((g) => `${g.node}=${j(g.d[s])}`);
      const distinct = new Set(got.filter((g) => g.d[s] !== undefined).map((g) => JSON.stringify(g.d[s])));
      if (distinct.size > 1) {
        checks.push(
          check(
            `Agreement: honest nodes deliver the same value for ${s}`,
            false,
            `Honest nodes delivered different values for ${s}: ${vals.join(', ')}. A traitor sender can tell each node something different; deliver only after 3 (2f+1) ready messages for one value, and send ready only after 3 matching echoes (or 2 matching readies).` + story(lies, who),
          ),
        );
      }
      const have = got.filter((g) => g.d[s] !== undefined).map((g) => g.node);
      const missing = got.filter((g) => g.d[s] === undefined).map((g) => g.node);
      if (have.length && missing.length) {
        checks.push(
          check(
            `Totality: if one honest node delivers for ${s}, all do`,
            false,
            `${have.join(', ')} delivered for ${s} but ${missing.join(', ')} had nothing by ${READ_AT}ms. A node that sees f+1 = 2 ready messages for a value knows at least one honest node is ready, so it should send its own ready too.` + story(lies, who),
          ),
        );
      }
    }
    if (checks.length === 3) checks.push(check('Agreement and totality for every sender', true));
    return checks;
  };
}

// ------------------------------------------------- signed votes
//
// Each node gets an input and broadcasts a signed vote. A node decides a
// value once it holds 2f+1 valid votes for it from distinct nodes, and
// passes the decision on as a certificate others can check.

const voteText = (value) => 'vote:' + value;

function certProblem(value, cert, ids) {
  if (typeof value !== 'string') return `the decided value ${j(value)} is not a string`;
  if (!Array.isArray(cert)) return `cert is ${j(cert)}, not a list of votes`;
  const voters = new Set();
  for (const e of cert) {
    if (!e || typeof e !== 'object') return `cert entry ${j(e)} is not a vote`;
    if (!ids.includes(e.voter)) return `cert entry ${j(e)} names an unknown voter`;
    if (e.value !== value) return `cert entry ${j(e)} is a vote for another value than ${j(value)}`;
    if (!validSig(e.voter, voteText(value), e.sig)) return `the signature in ${j(e)} does not verify`;
    voters.add(e.voter);
  }
  if (voters.size < QUORUM) return `it holds valid votes from only ${plural(voters.size, 'distinct node')} (${[...voters].join(', ') || 'none'}); 2f+1 = ${QUORUM} are needed`;
  return null;
}

export function bftVotes(sim, { scenario = 'silent' }) {
  const ids = sim.nodeIds;
  const byz = ids[ids.length - 1];
  const honest = ids.filter((id) => id !== byz);
  const inputs = {};
  const give = (node, value, t) =>
    sim.at(t, async () => {
      inputs[node] = value;
      await sim.clientRpc(node, { type: 'input', value }, { timeout: 500 });
    });
  const vote = (who, value) => ({ voter: who, value, sig: signAs(who, voteText(value)) });
  let expectAll = null; // value every honest node must decide

  // Votes honest nodes sent to the traitor: what it can copy into a certificate.
  const seen = {};
  sim.onSend = (m) => {
    if (m.kind === 'node' && m.dest === byz && m.src !== byz && m.body.type === 'vote' && !seen[m.src]) seen[m.src] = { voter: m.src, value: m.body.value, sig: m.body.sig };
  };

  let lies = [];
  if (scenario === 'silent') {
    lies = traitor(sim, byz, { script: () => 'drop' });
    for (const id of honest) give(id, 'blue', 100);
    expectAll = 'blue';
  } else if (scenario === 'both-ways') {
    // Inputs blue, blue, red. The traitor votes blue to n1, red to n3, and
    // sends n2 a vote for blue whose signature does not verify.
    const v = { n1: 'blue', n3: 'red' };
    lies = traitor(sim, byz, {
      script: (m) => {
        if (m.body.type !== 'vote') return 'drop';
        if (m.dest === 'n2') return { type: 'vote', value: 'blue', sig: '0badf00d' };
        return { type: 'vote', value: v[m.dest], sig: signAs(byz, voteText(v[m.dest])) };
      },
    });
    give('n1', 'blue', 100);
    give('n2', 'blue', 100);
    give('n3', 'red', 100);
    give(byz, 'blue', 100);
    expectAll = 'blue';
  } else if (scenario === 'forged') {
    // Inputs blue, red, red: red has two honest votes. The traitor waits,
    // then sends forged certificates to n1 and n2 and a genuine one (built
    // from the red votes it received) to n3.
    lies = traitor(sim, byz, {
      script: (m) => {
        if (m.body.type !== 'vote') return 'drop';
        if (m.dest === 'n1') return { type: 'decided', value: 'evil', cert: [vote(byz, 'evil'), vote(byz, 'evil'), vote(byz, 'evil')] };
        if (m.dest === 'n2')
          return {
            type: 'decided',
            value: 'evil',
            cert: [
              { voter: 'n1', value: 'evil', sig: signAs(byz, voteText('evil')) },
              { voter: 'n3', value: 'evil', sig: seen.n3 ? seen.n3.sig : '00000000' },
              vote(byz, 'evil'),
            ],
          };
        // Held back a little, so the forgeries reach n1 and n2 before n3 passes this one on.
        if (m.dest === 'n3') return { body: { type: 'decided', value: 'red', cert: [seen.n2, seen.n3, vote(byz, 'red')].filter(Boolean) }, delay: 40 };
        return 'drop';
      },
    });
    give('n1', 'blue', 100);
    give('n2', 'red', 100);
    give('n3', 'red', 100);
    give(byz, 'red', 300);
    expectAll = 'red';
  } else if (scenario === 'three') {
    // n = 3 but the system is built for f = 1. Inputs blue, red; the traitor
    // votes blue to n1 and red to n2.
    const v = { n1: 'blue', n2: 'red' };
    lies = traitor(sim, byz, {
      script: (m) => (m.body.type === 'vote' ? { type: 'vote', value: v[m.dest], sig: signAs(byz, voteText(v[m.dest])) } : 'drop'),
    });
    give('n1', 'blue', 100);
    give('n2', 'red', 100);
    give(byz, 'blue', 100);
  }

  const READ_AT = sim.duration - 600;
  const reads = finalReads(sim, honest, READ_AT);

  return () => {
    const who = `${byz} (Byzantine)`;
    const checks = [readsAnswered(reads, READ_AT, 'read_ok')];
    const got = reads.filter((r) => r.res && r.res.type === 'read_ok').map((r) => ({ node: r.node, value: r.res.value ?? null, cert: r.res.cert ?? null }));
    const decided = got.filter((g) => g.value !== null);

    const badCert = decided.map((g) => ({ g, why: certProblem(g.value, g.cert, ids) })).find((x) => x.why);
    checks.push(
      check(
        'Every decision carries a valid certificate of 2f+1 signed votes',
        !badCert,
        badCert && `${badCert.g.node} decided ${j(badCert.g.value)} but ${badCert.why}. Count a vote only if verify(voter, "vote:" + value, sig) holds, at most one per voter, and check every entry of a certificate you receive the same way.` + story(lies, who),
      ),
    );

    const distinct = new Set(decided.map((g) => g.value));
    checks.push(
      check(
        'Agreement: honest nodes decide the same value',
        distinct.size <= 1,
        `Honest nodes decided different values: ${decided.map((g) => `${g.node}=${j(g.value)}`).join(', ')}.` +
          (ids.length < 3 * F + 1
            ? ` With n = ${ids.length} and f = 1, any two groups of n - f = ${ids.length - F} nodes overlap in a single node, and it may be the traitor. Wait for 2f+1 = ${QUORUM} matching votes, even if that means never deciding.`
            : ` Two groups of 2f+1 = ${QUORUM} out of ${ids.length} nodes share at least ${2 * QUORUM - ids.length} nodes, so at least one honest one; deciding on fewer matching votes loses that overlap.`) +
          story(lies, who),
      ),
    );

    const honestInputs = new Set(honest.map((id) => inputs[id]).filter((v) => v !== undefined));
    const invented = decided.find((g) => !honestInputs.has(g.value));
    checks.push(
      check(
        'Validity: a decided value was the input of an honest node',
        !invented,
        invented && `${invented.node} decided ${j(invented.value)}, but the honest inputs were ${j([...honestInputs])}.` + story(lies, who),
      ),
    );

    if (expectAll !== null) {
      const slow = got.filter((g) => g.value !== expectAll);
      checks.push(
        check(
          `Every honest node decides ${j(expectAll)}`,
          slow.length === 0,
          slow.length &&
            `${slow.map((g) => `${g.node}=${j(g.value)}`).join(', ')} at ${READ_AT}ms. ${j(expectAll)} had 2f+1 valid votes available. A node that cannot collect them itself should accept a "decided" message whose certificate checks out.` +
              story(lies, who),
        ),
      );
    } else {
      checks.push(check('No honest node can decide (n < 3f+1): none did', decided.length === 0, `${decided.map((g) => `${g.node}=${j(g.value)}`).join(', ')} decided.`));
    }
    return checks;
  };
}

// ------------------------------------------------- PBFT round
//
// A client sends a signed request to the primary n1. Replicas run
// pre-prepare / prepare / commit with 2f+1 quorums and decide one value.

const requestText = (value) => 'request:' + value;

export function bftPbft(sim, { scenario = 'silent' }) {
  const ids = sim.nodeIds;
  const primary = ids[0];
  const byz = scenario === 'equivocate' || scenario === 'forge' ? primary : ids[ids.length - 1];
  const honest = ids.filter((id) => id !== byz);
  const signed = []; // values the client signed, in order
  const toPrimary = []; // values sent to the primary, in order
  const request = (node, value, t) =>
    sim.at(t, async () => {
      signed.push(value);
      if (node === primary) toPrimary.push(value);
      await sim.clientRpc(node, { type: 'request', value, sig: signAs('client', requestText(value)) }, { timeout: 500 });
    });
  const pp = (value, sigOf = value) => ({ type: 'pre_prepare', view: 0, seq: 1, value, client_sig: signAs('client', requestText(sigOf)) });

  // What the traitor primary proposed to itself: used to keep its own view consistent.
  let ownValue = null;
  const keepOwnView = (m) => {
    const b = m.body;
    if (m.src === byz && b.type === 'pre_prepare' && ownValue === null) ownValue = b.value;
    if ((b.type === 'prepare' || b.type === 'commit') && ownValue !== null) return { ...b, value: ownValue };
    return undefined;
  };

  let lies = [];
  if (scenario === 'silent') {
    lies = traitor(sim, byz, { script: () => 'drop' });
    request(primary, 'pay-alice-10', 100);
  } else if (scenario === 'liar') {
    // The client mistakenly also sends a request to n4. n4 turns its first
    // message to n2 into a pre_prepare for that request, and lies about the
    // value in every prepare and commit. The primary's pre_prepare to n2 is
    // held back so the fake one arrives first.
    const sent = {};
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (m.dest === 'n2' && !sent.n2) {
          sent.n2 = true;
          return pp('pay-mallory-99');
        }
        if (b.type === 'prepare' || b.type === 'commit') return { type: b.type, view: 0, seq: 1, value: 'pay-mallory-99' };
        return undefined;
      },
      delay: (m) => (m.src === primary && m.dest === 'n2' && m.body.type === 'pre_prepare' ? 150 : undefined),
    });
    request(byz, 'pay-mallory-99', 50);
    request(primary, 'pay-alice-10', 100);
  } else if (scenario === 'equivocate') {
    // Two signed requests reach the primary; it pre-prepares one to n2 and
    // the other to n3 and n4, and backs each side in its prepares and commits.
    const v = { n2: 'pay-alice-10', n3: 'pay-bob-20', n4: 'pay-bob-20' };
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (b.type === 'pre_prepare') return pp(v[m.dest]);
        if (b.type === 'prepare' || b.type === 'commit') return { type: b.type, view: 0, seq: 1, value: v[m.dest] };
        return 'drop';
      },
      inbound: keepOwnView,
      // n2 hears from n3 and n4 late, so the primary's support reaches it first.
      delay: (m) => (m.dest === 'n2' && (m.src === 'n3' || m.src === 'n4') ? 80 : undefined),
    });
    request(primary, 'pay-alice-10', 100);
    request(primary, 'pay-bob-20', 130);
  } else if (scenario === 'forge') {
    // One signed request. The primary pre-prepares an invented value to n2
    // and n3 (with the real request's signature) and the real one to n4.
    const v = { n2: 'pay-mallory-99', n3: 'pay-mallory-99', n4: 'pay-alice-10' };
    lies = traitor(sim, byz, {
      script: (m) => {
        const b = m.body;
        if (b.type === 'pre_prepare') return pp(v[m.dest], 'pay-alice-10');
        if (b.type === 'prepare' || b.type === 'commit') return { type: b.type, view: 0, seq: 1, value: v[m.dest] };
        return 'drop';
      },
      inbound: keepOwnView,
    });
    request(primary, 'pay-alice-10', 100);
  }

  const READ_AT = sim.duration - 600;
  const reads = finalReads(sim, honest, READ_AT);

  return () => {
    const who = `${byz} (Byzantine${byz === primary ? ' primary' : ''})`;
    const checks = [readsAnswered(reads, READ_AT, 'read_ok')];
    const got = reads.filter((r) => r.res && r.res.type === 'read_ok').map((r) => ({ node: r.node, value: r.res.value ?? null }));
    const decided = got.filter((g) => g.value !== null);

    const distinct = new Set(decided.map((g) => JSON.stringify(g.value)));
    checks.push(
      check(
        'Agreement: honest replicas decide the same value',
        distinct.size <= 1,
        `Honest replicas decided different values: ${decided.map((g) => `${g.node}=${j(g.value)}`).join(', ')}. Send commit only after 2f+1 = ${QUORUM} prepares that match the value you accepted, and decide only on 2f+1 commits for one value.` +
          story(lies, who),
      ),
    );

    const invented = decided.find((g) => !signed.includes(g.value));
    checks.push(
      check(
        'Validity: only a value the client signed is decided',
        !invented,
        invented &&
          `${invented.node} decided ${j(invented.value)}, but the client only signed ${j(signed)}. Accept a pre_prepare only if verify("client", "request:" + value, client_sig) holds.` +
          story(lies, who),
      ),
    );

    if (byz !== primary) {
      const want = toPrimary[0];
      const off = got.filter((g) => g.value !== want);
      checks.push(
        check(
          `Honest primary: every honest replica decides the request (${j(want)}) in time`,
          off.length === 0,
          off.length &&
            `${off.map((g) => `${g.node}=${j(g.value)}`).join(', ')} at ${READ_AT}ms; the honest primary ${primary} received ${j(want)} at 100ms. Accept pre_prepare only from the primary, and only the first one; with ${primary}'s pre_prepare every honest replica can gather 2f+1 prepares and commits.` +
              story(lies, who),
        ),
      );
    } else {
      checks.push(check('Termination is not required with a Byzantine primary', true));
    }
    return checks;
  };
}
