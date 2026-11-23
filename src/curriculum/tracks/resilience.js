// Track: Resilience patterns. See docs/authoring.md for the shape of this file.
import { timeouts, retries, breaker, rateLimit, bulkhead } from '../../sim/workloads/resilience.js';

export default {
  id: 'resilience',
  code: 'RS',
  category: 'operations',
  title: 'Resilience patterns',
  level: 'Intermediate',
  blurb: 'Stay up when a dependency is slow or failing: timeouts, retries with backoff, circuit breakers and rate limits.',
  subtracks: [
    {
      title: 'Failing well',
      tasks: [
        {
          slug: 'resil-timeout',
          title: 'Timeouts and deadlines',
          level: 'Easy',
          minutes: 15,
          summary: 'Answer every client within 300ms, even when the upstream never answers.',
          concepts: ['timeouts', 'deadlines', 'fail fast'],
          workload: timeouts,
          tests: [
            { name: 'Healthy upstream', blurb: '40 requests; the upstream answers every call within 150ms.', nodeCount: 1, duration: 4000, params: { ops: 40 } },
            { name: 'Slow and silent upstream', blurb: '60 requests; one call in five takes 400–900ms and one in five is never answered.', nodeCount: 2, duration: 5000, params: { ops: 60, slow: 0.2, silent: 0.2 } },
            { name: 'Upstream goes dark', blurb: 'The upstream answers nothing from 1.5s to 3s, then recovers.', nodeCount: 2, duration: 5500, params: { ops: 70, dark: [1500, 3000] } },
          ],
          hints: [
            'The starter crashes when `node.rpc` resolves to `null`: `res.value` of null throws, and the client never hears back.',
            '`node.rpc(dest, body, { timeout: 200 })` gives up after 200ms. The client\'s 300ms has to cover the trip to you, the upstream call and the trip back.',
            'Pick a timeout above the upstream\'s slowest normal answer (150ms, plus about 25ms of network) and below the deadline minus the network: 200ms works.',
            'When `res` is `null`, reply `{ type: "error", code: "timeout" }` and return. A late answer that arrives afterwards is dropped by `node.rpc`, so you cannot reply twice.',
          ],
        },
        {
          slug: 'resil-retry',
          title: 'Retries with backoff and jitter',
          level: 'Medium',
          minutes: 25,
          summary: 'Retry transient failures with growing, randomised waits, and never more than four calls per request.',
          concepts: ['retries', 'exponential backoff', 'jitter', 'retry budget'],
          workload: retries,
          tests: [
            { name: 'Flaky upstream', blurb: '120 requests; 15% of calls are lost, 15% answer "busy", and a few keys do not exist.', nodeCount: 2, duration: 8000, params: { mode: 'flaky', ops: 120 } },
            { name: 'Upstream overloaded', blurb: 'The upstream answers "busy" to everything; 24 requests arrive at the same instant, twice.', nodeCount: 2, duration: 4800, params: { mode: 'down', perNode: 12, bursts: [300, 2500] } },
          ],
          hints: [
            'Wrap the call in a loop: `for (let attempt = 1; attempt <= 4; attempt++)`. Return from the handler as soon as you have something to say to the client.',
            'Only silence (`null`) and `code: "busy"` are worth another try. `not_found` will be `not_found` next time too: pass it on at once.',
            'A pause inside an async handler: `await new Promise((r) => node.after(ms, r))`. Other requests carry on meanwhile.',
            'Before retry n wait `node.random() * 100 * Math.pow(2, n - 1)` ms: ceilings of 100, 200 and 400ms. Without `node.random()` all 24 requests that failed together come back together.',
            'Check the worst case against the 2000ms deadline: 4 timeouts of 200ms plus at most 700ms of waiting is about 1.5s.',
          ],
        },
      ],
    },
    {
      title: 'Protecting the upstream',
      tasks: [
        {
          slug: 'resil-breaker',
          title: 'Circuit breaker',
          level: 'Medium',
          minutes: 30,
          summary: 'Stop calling an upstream that keeps failing, fail fast, probe after a cool-down, and close when it recovers.',
          concepts: ['circuit breaker', 'half-open', 'fail fast', 'cool-down'],
          workload: breaker,
          tests: [
            { name: 'Occasional failures', blurb: 'One call in three fails, never two in a row: the breaker must stay closed.', nodeCount: 1, duration: 4000, params: { failEvery: 3 } },
            { name: 'Silent outage', blurb: 'The upstream answers nothing from 1.5s to 4.5s; 20 requests per second throughout.', nodeCount: 2, duration: 8000, params: { outages: [[1500, 4500]], kind: 'silent' } },
            { name: 'Two error outages', blurb: 'The upstream answers "busy" to everything from 1.5s to 3.5s and again from 6.5s to 8.5s.', nodeCount: 2, duration: 12000, params: { outages: [[1500, 3500], [6500, 8500]], kind: 'busy' } },
          ],
          hints: [
            'Three states. **Closed**: call, and count failures in a row. **Open**: refuse at once with `circuit_open`. **Half-open**: one probe is out; refuse everything else.',
            'A failure is a `null` (timeout) or any error reply. A success resets the count to zero, so scattered failures never open the breaker.',
            'You need no timer for the cool-down: on each request, if the breaker is open and `node.now() - openedAt >= 1000`, switch to half-open and let this request through as the probe.',
            'A successful probe closes the breaker; a failed one reopens it and restarts the cool-down. Remember whether a call was the probe before you await it.',
            'Calls sent just before the breaker opened come back while it is open. Ignore their results unless you are closed, or a stale success can close the breaker early.',
          ],
        },
        {
          slug: 'resil-rate-limit',
          title: 'Rate limiting with a token bucket',
          level: 'Medium',
          minutes: 20,
          summary: 'Admit at most 20 requests per second with bursts of 10, and refuse the rest at once.',
          concepts: ['token bucket', 'rate limiting', 'burst', 'admission control'],
          workload: rateLimit,
          tests: [
            { name: 'Under the limit', blurb: '15 requests per second for 3 seconds: everything should be admitted.', nodeCount: 1, duration: 3800, params: { load: [{ from: 200, to: 3200, rps: 15 }] } },
            { name: 'Sustained overload', blurb: '60 requests per second for 3.5 seconds.', nodeCount: 1, duration: 4500, params: { load: [{ from: 200, to: 3700, rps: 60 }] } },
            { name: 'Bursts', blurb: '10 requests per second, plus bursts of 30, 15 and 15 requests arriving at one instant.', nodeCount: 1, duration: 4500, params: { load: [{ from: 200, to: 3800, rps: 10 }], bursts: [{ at: 600, count: 30 }, { at: 2000, count: 15 }, { at: 2300, count: 15 }] } },
          ],
          hints: [
            'A bucket holds up to 10 tokens and starts full. Each admitted request takes one; tokens drip back in at 20 per second.',
            'No timer is needed. On each request add `(now - lastRefill) * 20 / 1000` tokens, cap the total at 10, and remember `now`.',
            'Keep the fraction. Rounding down on every request loses the partial tokens and admits too few.',
            'Refilling 10 tokens once a second looks similar but lets 20 calls through within a few milliseconds around each refill. The tests look at every window, not just whole seconds.',
          ],
        },
        {
          slug: 'resil-bulkhead',
          title: 'Bulkhead: bounded concurrency',
          level: 'Easy',
          minutes: 15,
          summary: 'Keep at most 4 upstream calls in flight and shed the rest instead of queueing them.',
          concepts: ['bulkhead', 'load shedding', 'concurrency limit'],
          workload: bulkhead,
          tests: [
            { name: 'Light load', blurb: '8 requests per second on a fast upstream: nothing should be refused.', nodeCount: 1, duration: 3500, params: { rps: 8, allOk: true } },
            { name: 'Slow upstream, bursts', blurb: 'Every call takes 150ms and requests arrive 8 at a time every 300ms.', nodeCount: 1, duration: 4000, params: { work: 150, bursts: { every: 300, count: 8 }, minOk: 40 } },
            { name: 'Lost calls', blurb: '20 requests per second; 30% of upstream calls are never answered.', nodeCount: 1, duration: 4500, params: { work: 60, silent: 0.3, rps: 20, minOk: 25 } },
          ],
          hints: [
            'Keep one counter: upstream calls sent and not yet finished.',
            'If the counter is at 4 when a request arrives, reply `{ type: "error", code: "overloaded" }` straight away. Do not queue it: it would wait past its deadline anyway.',
            'Decrement right after the `await`, before looking at the result. Then a reply, an error and a timeout all give the slot back.',
            'If successes stop after a while in the "Lost calls" test, a slot leaks: some path out of the handler never decrements.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Waiting forever, or for the library default', symptom: 'Clients time out although the service is up; one slow dependency stalls everything.', fix: 'Give every remote call a timeout shorter than the deadline you owe your own caller.' },
    { title: 'Retrying immediately and in lockstep', symptom: 'An overloaded upstream receives a fresh spike every time it starts to recover.', fix: 'Back off exponentially and add jitter so retries from many clients spread out.' },
    { title: 'Retrying errors that cannot change', symptom: 'Load on the upstream multiplies, and a "not found" still comes back after four tries.', fix: 'Retry only transient failures (timeouts, busy, unavailable); pass permanent errors straight on.' },
    { title: 'A breaker that never closes, or a limiter that leaks', symptom: 'Service stays down after the upstream is back; or, under load, every request is refused.', fix: 'Let one probe through after the cool-down; release every concurrency slot on every path, including timeouts.' },
  ],
  questions: [
    { q: 'Why is a timeout not the same as a failure?', a: 'The upstream may have done the work and only the reply was slow or lost. That is why a retried operation must be idempotent, and why a timeout error tells the caller "unknown", not "did not happen".' },
    { q: 'Retries improve availability. How can they also cause an outage?', a: 'Each retry is extra load. When an upstream is overloaded, every client retrying multiplies the traffic exactly when capacity is lowest: a retry storm. Caps on attempts, backoff, jitter and circuit breakers keep that amplification bounded.' },
    { q: 'A token bucket and a concurrency limit both protect the upstream. When does each fit?', a: 'A token bucket bounds the rate of requests, which suits quotas and upstreams priced per call. A concurrency limit bounds work in progress, which adapts on its own when calls get slower: the same limit admits fewer requests per second as latency grows.' },
  ],
  reading: [
    { kind: 'Blog', title: 'Exponential backoff and jitter', url: 'https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/', note: 'Marc Brooker compares jitter strategies with simulations.' },
    { kind: 'Blog', title: 'CircuitBreaker', url: 'https://martinfowler.com/bliki/CircuitBreaker.html', note: 'Martin Fowler on the closed, open and half-open states.' },
    { kind: 'Book', title: 'Site Reliability Engineering: Handling overload', url: 'https://sre.google/sre-book/handling-overload/', note: 'Google on load shedding, client-side throttling and retry budgets.' },
  ],
};
