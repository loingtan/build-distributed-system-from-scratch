// Checks the curriculum against itself: every reference solution must pass
// every test, and every starter must fail at least one.
//
//   npm run verify            all challenges
//   npm run verify -- echo             one challenge, with details
//   npm run verify -- --track quorum   one track, with details

import { readFileSync } from 'node:fs';
import { challenges } from '../src/curriculum/catalog.js';
import { runTest } from '../src/sim/runner.js';

const trackOnly = process.argv[2] === '--track' ? process.argv[3] : null;
const only = trackOnly ? null : process.argv[2];
const read = (slug, kind) => readFileSync(new URL(`../src/curriculum/code/${slug}.${kind}.js`, import.meta.url), 'utf8');
let bad = 0;

for (const c of challenges) {
  if (only && c.slug !== only) continue;
  if (trackOnly && c.trackId !== trackOnly) continue;
  for (const kind of ['solution', 'starter']) {
    const code = read(c.slug, kind);
    const outcomes = [];
    for (let i = 0; i < c.tests.length; i++) {
      const started = Date.now();
      const r = await runTest(c, i, code);
      outcomes.push(r.ok);
      const s = r.stats;
      const line = `  ${r.ok ? 'pass' : 'FAIL'}  ${c.tests[i].name.padEnd(30)} ${String(Date.now() - started).padStart(5)}ms  ops=${s.ops} nodeMsgs=${s.nodeMsgs} svcMsgs=${s.kvMsgs} perOp=${s.msgsPerOp.toFixed(1)} dropped=${s.dropped} p50=${Math.round(s.p50)}ms`;
      if (kind === 'solution' || only || trackOnly) {
        if (i === 0) console.log(`${c.number} ${c.slug} [${kind}]`);
        console.log(line);
        if (!r.ok || only || trackOnly) for (const k of r.checks) if (!k.ok) console.log(`        x ${k.name}: ${k.detail}`);
      }
    }
    if (kind === 'solution' && outcomes.some((ok) => !ok)) bad++;
    if (kind === 'starter') {
      const fails = outcomes.some((ok) => !ok);
      console.log(`  starter: ${outcomes.map((ok) => (ok ? 'pass' : 'fail')).join(' ')}${fails ? '' : '   <-- STARTER PASSES EVERYTHING'}`);
      if (!fails) bad++;
    }
  }
}
console.log(bad ? `\n${bad} problem(s)` : '\nAll reference solutions pass and all starters fail.');
process.exit(bad ? 1 : 0);
