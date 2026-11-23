// Track: Load balancing. See docs/authoring.md for the shape of this file.
import { roundRobin, leastOutstanding, stickyRouting, healthChecks, slowStart } from '../../sim/workloads/load-balancing.js';

const four = ['b1', 'b2', 'b3', 'b4'];
const five = ['b1', 'b2', 'b3', 'b4', 'b5'];

export default {
  id: 'load-balancing',
  code: 'LB',
  category: 'scalability',
  title: 'Load balancing',
  level: 'Intermediate',
  blurb: 'Spread requests over a pool of backends: round robin, least outstanding requests, health checks and sticky routing.',
  subtracks: [
    {
      title: 'Choosing a backend',
      tasks: [
        {
          slug: 'lb-round-robin',
          title: 'Round robin',
          level: 'Easy',
          minutes: 15,
          summary: 'Forward each request to the next backend in turn and relay its answer.',
          concepts: ['reverse proxy', 'round robin', 'request relay'],
          workload: roundRobin,
          tests: [
            { name: 'Three backends', blurb: '50 requests per second for 3 seconds over b1, b2 and b3.', nodeCount: 1, duration: 4000, params: { phases: [{ at: 0, backends: ['b1', 'b2', 'b3'] }], rps: 50 } },
            {
              name: 'Replies out of order',
              blurb: 'Bursts of 10 requests at once over five backends of very different speeds, so answers come back in a different order.',
              nodeCount: 1,
              duration: 3800,
              params: { phases: [{ at: 0, backends: five }], bursts: { every: 200, count: 10 }, latency: { b1: [5, 15], b2: [40, 120], b3: [100, 250], b4: [5, 250], b5: [20, 60] } },
            },
            {
              name: 'The pool changes',
              blurb: 'Starts with three backends, grows to four at 1.5s and shrinks to two at 3s.',
              nodeCount: 1,
              duration: 5000,
              params: { phases: [{ at: 0, backends: ['b1', 'b2', 'b3'] }, { at: 1500, backends: four }, { at: 3000, backends: ['b4', 'b2'] }], rps: 50 },
            },
          ],
          hints: [
            'The starter sends everything to `backends[0]`. Run it and read the counts in the failing check.',
            'Keep a counter `next` next to `backends`. Pick `backends[next % backends.length]` and add one to `next`.',
            'The modulo keeps the index valid when `set_backends` shrinks the pool, so you never need to reset the counter.',
            'Do the pick and the increment before the `await`: everything after an `await` runs later, interleaved with other requests.',
          ],
        },
        {
          slug: 'lb-least-outstanding',
          title: 'Least outstanding requests',
          level: 'Medium',
          minutes: 25,
          summary: 'Send each request to the backend with the fewest requests in flight, so a slow backend gets less.',
          concepts: ['least outstanding requests', 'tail latency', 'adaptive routing'],
          workload: leastOutstanding,
          tests: [
            {
              name: 'One slow backend',
              blurb: '80 requests per second over four backends; b3 takes 250–350ms per request, the others 10–40ms.',
              nodeCount: 1,
              duration: 5000,
              params: { backends: four, rps: 80, slow: [{ id: 'b3', from: 0, to: 99999, range: [250, 350] }] },
            },
            {
              name: 'A backend slows down',
              blurb: 'All four backends are fast, except b2 from 1.5s to 3.5s, when it takes 300–400ms.',
              nodeCount: 1,
              duration: 6000,
              params: { backends: four, rps: 80, slow: [{ id: 'b2', from: 1500, to: 3500, range: [300, 400] }] },
            },
            { name: 'Bursts', blurb: '12 requests arrive at the same instant every 250ms; all backends take 20–60ms.', nodeCount: 1, duration: 4000, params: { backends: four, bursts: { every: 250, count: 12 }, base: [20, 60] } },
          ],
          hints: [
            'Keep `inFlight[id]`: requests sent to that backend and not yet finished.',
            'Add one just before `node.rpc`, and subtract one right after the `await`, before you look at the result.',
            'To pick, scan the pool for the smallest `inFlight`. Break ties at random with `node.random()`, or the first backend in the list gets every tie.',
            'A slow backend holds its requests longer, so its count stays high and new requests go elsewhere. Nobody has to measure its speed.',
          ],
        },
        {
          slug: 'lb-sticky-routing',
          title: 'Sticky routing with consistent hashing',
          level: 'Medium',
          minutes: 25,
          summary: 'Send every request for a key to the same backend, and move as few keys as possible when the pool changes.',
          concepts: ['session affinity', 'rendezvous hashing', 'consistent hashing', 'cache locality'],
          workload: stickyRouting,
          tests: [
            { name: 'Stable pool', blurb: '400 keys, each requested three times, over five backends.', nodeCount: 1, duration: 4300, params: { phases: [{ at: 0, backends: five }], keys: 400, perPhase: 3 } },
            {
              name: 'One leaves, one joins',
              blurb: 'Five backends; b3 is removed at 1.8s and b6 added at 3.4s. 300 keys, each requested twice per stage.',
              nodeCount: 1,
              duration: 5000,
              params: { phases: [{ at: 0, backends: five }, { at: 1800, backends: ['b1', 'b2', 'b4', 'b5'] }, { at: 3400, backends: ['b1', 'b2', 'b4', 'b5', 'b6'] }], keys: 300 },
            },
            {
              name: 'Back again, in another order',
              blurb: 'b2 leaves and comes back, and the list arrives in a different order each time.',
              nodeCount: 1,
              duration: 6600,
              params: { phases: [{ at: 0, backends: four }, { at: 1600, backends: ['b4', 'b3', 'b1'] }, { at: 3200, backends: ['b3', 'b2', 'b4', 'b1'] }, { at: 4800, backends: ['b1', 'b4', 'b2', 'b3'] }], keys: 300 },
            },
          ],
          hints: [
            'The starter balances by load, so a key lands wherever there is room. Run it and read the "same backend" check.',
            'Rendezvous hashing: for a key, compute `hash(key + "/" + id)` for every backend in the pool and pick the backend with the highest score.',
            'When a backend leaves, only the keys it was winning need a new winner; every other key keeps the same highest score. When one joins, it takes only the keys where it scores highest.',
            'Use the hash function from the starter. Do not use list positions anywhere: the same backends in another order must give the same answers.',
          ],
        },
      ],
    },
    {
      title: 'When backends fail',
      tasks: [
        {
          slug: 'lb-health-checks',
          title: 'Health checks and retries',
          level: 'Hard',
          minutes: 35,
          summary: 'Probe every backend, stop routing to dead ones within a second, bring them back when they recover, and retry once elsewhere.',
          concepts: ['active health checks', 'failover', 'retries', 'black-hole backends'],
          workload: healthChecks,
          tests: [
            { name: 'A backend crashes', blurb: 'b2 answers nothing from 1.5s to 4s, then recovers. 60 requests per second.', nodeCount: 1, duration: 7000, params: { backends: four, outages: [{ id: 'b2', from: 1500, to: 4000, kind: 'dead' }] } },
            { name: 'A backend fails fast', blurb: 'b3 answers everything with an error, instantly, from 1.2s to 3.7s.', nodeCount: 1, duration: 6800, params: { backends: four, outages: [{ id: 'b3', from: 1200, to: 3700, kind: 'broken' }] } },
            {
              name: 'One after another',
              blurb: 'b1 dies from 1s to 2.5s; b4 fails fast from 3s to 4.5s.',
              nodeCount: 1,
              duration: 7500,
              params: { backends: four, outages: [{ id: 'b1', from: 1000, to: 2500, kind: 'dead' }, { id: 'b4', from: 3000, to: 4500, kind: 'broken' }] },
            },
          ],
          hints: [
            'Run the starter on "A backend fails fast": a broken backend answers in a millisecond, so it always has the fewest requests in flight and gets almost everything.',
            'Keep `healthy[id]` and `failures[id]`. Every 200ms send `{ type: "health" }` to every backend with a 150ms timeout: `health_ok` resets the failures and marks it healthy; anything else adds one, and two in a row mark it unhealthy.',
            'Choose only among healthy backends. Probe all of them, healthy or not: the probes are how a dead backend gets back in.',
            'Wrap the forward in a loop of at most two attempts with a 250ms timeout. If the answer is not `get_ok`, try again on a backend you have not tried for this request.',
            'Decrement `inFlight` after every attempt, whatever its outcome, or a backend that timed out stays "busy" for ever.',
          ],
        },
        {
          slug: 'lb-slow-start',
          title: 'Slow start',
          level: 'Medium',
          minutes: 25,
          summary: 'Ramp a recovered or newly added backend up to a full share over three seconds instead of all at once.',
          concepts: ['slow start', 'warm-up', 'weighted routing'],
          workload: slowStart,
          tests: [
            { name: 'A backend recovers', blurb: 'b2 is down from 1s to 2.5s, then comes back cold. 120 requests per second.', nodeCount: 1, duration: 9500, params: { backends: four, rps: 120, outages: [{ id: 'b2', from: 1000, to: 2500, kind: 'dead' }] } },
            { name: 'A backend joins', blurb: 'b5 is added to a pool of four at 2s. 120 requests per second.', nodeCount: 1, duration: 9000, params: { backends: four, rps: 120, joins: [{ id: 'b5', at: 2000 }] } },
          ],
          hints: [
            'Remember when each backend came into service: `upSince[id] = node.now()` when a probe puts it back, or when `set_backends` adds it. Backends in the very first list are warm: leave them out.',
            'Its weight is `Math.min(1, (node.now() - upSince[id]) / 3000)`.',
            'Least outstanding cannot do this on its own: a fresh backend has nothing in flight, so it looks like the best choice. Decide in two steps: first, with probability `weight / (sum of weights)`, send the request to the warming backend; otherwise pick among the warm ones by fewest in flight.',
            'With `n` backends and one of them at weight `w`, it gets `w / (n - 1 + w)` of the traffic: close to `w` times an equal share while `w` is small.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Round robin over backends of different speeds',
      symptom: 'Tail latency is set by the slowest backend: a quarter of requests wait on it while the others sit idle.',
      fix: 'Route on load you can see, such as requests in flight, so a slow backend receives less without anyone measuring its speed.',
    },
    {
      title: 'Counting the fast failure as the least loaded',
      symptom: 'One broken backend that answers every request with an error at once attracts almost all the traffic.',
      fix: 'Health-check every backend, treat an error reply as a failure, take failing backends out and retry the request elsewhere.',
    },
    {
      title: 'Never putting a backend back',
      symptom: 'After an outage the pool slowly shrinks; capacity is lost although every backend is healthy again.',
      fix: 'Keep probing backends you have taken out of service and return them as soon as they pass.',
    },
    {
      title: 'hash(key) % N for sticky routing',
      symptom: 'Adding or removing one backend sends almost every key somewhere new and every cache goes cold at once.',
      fix: 'Use rendezvous hashing or a hash ring, built from backend ids, not list positions.',
    },
  ],
  questions: [
    {
      q: 'Why does least outstanding requests beat round robin when backends differ in speed, without measuring speed?',
      a: 'A slow backend holds each request longer, so at any moment it has more of them in flight. Choosing the backend with the fewest in flight automatically sends it fewer new ones; the count is a live signal of how busy it is.',
    },
    {
      q: 'Health checks pass, yet real requests fail on one backend. How can that happen, and what helps?',
      a: 'A health endpoint can be cheaper than real work: it may not touch the database or the disk that is failing. Make the check exercise the real dependencies, and also watch real traffic (passive health checking): take a backend out when its real requests keep failing.',
    },
    {
      q: 'Sticky routing improves cache hit rates. What does it cost?',
      a: 'Load follows keys instead of capacity, so one very popular key can overload its backend while the others idle, and the balancer can no longer route around a slow backend without breaking stickiness. Systems often combine the two: hash to choose, but spill to another backend when the chosen one is overloaded.',
    },
  ],
  reading: [
    {
      kind: 'Blog',
      title: 'Load balancing (Sam Rose)',
      url: 'https://samwho.dev/load-balancing/',
      note: 'An interactive essay comparing round robin, weighted and least-connections routing with animations.',
    },
    {
      kind: 'Book',
      title: 'Site Reliability Engineering: Load balancing in the datacenter',
      url: 'https://sre.google/sre-book/load-balancing-datacenter/',
      note: 'How Google picks a backend: subsetting, weighted round robin and why least-loaded can mislead.',
    },
    {
      kind: 'Docs',
      title: 'Envoy: supported load balancers',
      url: 'https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers',
      note: 'Weighted least request with two random choices, ring hash, Maglev and slow start, as a production proxy does them.',
    },
  ],
};
