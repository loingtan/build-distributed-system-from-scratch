import { simulate } from './engine.js';
import { workloads } from './workloads.js';

// The fault schedule of each test comes from a seed. By default it is fixed, so a
// rerun repeats the same scenario; pass another seed to try a different one.
export const defaultSeed = (challenge, testIndex) => 7919 * (testIndex + 1) + challenge.slug.length;

// Run one test of one challenge against the learner's code.
export async function runTest(challenge, testIndex, code, seed = defaultSeed(challenge, testIndex)) {
  const test = challenge.tests[testIndex];
  const result = await simulate({
    code,
    nodeCount: test.nodeCount,
    duration: test.duration,
    latency: test.latency,
    dropRate: test.dropRate,
    kv: test.kv,
    maxEvents: test.maxEvents,
    clockSkew: test.clockSkew,
    seed,
    // A task names a built-in workload, or carries its own workload function.
    workload: typeof challenge.workload === 'function' ? challenge.workload : workloads[challenge.workload],
    params: test.params,
  });
  return { ...result, seed };
}
