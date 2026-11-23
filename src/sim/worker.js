// Runs a challenge's tests off the main thread, so a slow or looping
// solution never freezes the page. The page terminates this worker if it
// stops reporting.
//   data: { slug, code, only?: test index, seed?: seed for the test(s) run }
import { bySlug } from '../curriculum/catalog.js';
import { runTest, defaultSeed } from './runner.js';

self.onmessage = async ({ data }) => {
  const challenge = bySlug[data.slug];
  const indexes = Number.isInteger(data.only) ? [data.only] : challenge.tests.map((_, i) => i);
  for (const i of indexes) {
    self.postMessage({ type: 'start', index: i });
    const seed = Number.isInteger(data.seed) ? data.seed + i : defaultSeed(challenge, i);
    const result = await runTest(challenge, i, data.code, seed);
    self.postMessage({ type: 'result', index: i, result });
  }
  self.postMessage({ type: 'done' });
};
