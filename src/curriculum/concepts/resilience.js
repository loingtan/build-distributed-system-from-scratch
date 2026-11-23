export default [
  {
    term: 'Timeouts',
    aliases: ['timeout', 'request timeout'],
    summary: 'A limit on how long to wait for one call before giving up on it.',
    detail: `A **timeout** stops a caller from waiting forever on a dependency that has gone quiet. Without one, every request that touches a hung dependency holds a thread, a connection or memory until the caller itself runs out and stops answering.

Choose it from both sides: longer than the dependency's slowest normal answer (or healthy calls fail), shorter than your own deadline minus network time (or your reply is late). With an upstream that answers within about 175 ms and a 300 ms client deadline, 200 ms works.

A timeout tells you only that you stopped waiting. The call may still have been processed, which matters when you retry.`,
    related: ['Deadlines', 'Fail fast', 'Retries'],
  },
  {
    term: 'Deadlines',
    aliases: ['deadline', 'deadline propagation'],
    summary: 'A point in time by which a caller must get an answer; every call made on its behalf must fit inside it.',
    detail: `A **deadline** is a promise to your caller: you will hear from me by then. Unlike a per-call timeout, it is a budget for the whole request, so every downstream call, retry and wait must fit within what is left.

Example: a client allows 300 ms. Up to 12 ms each way are spent on the network, leaving about 276 ms for your upstream call and processing.

Systems such as gRPC propagate the deadline with each call, so a service three hops down knows how much time remains and does not keep working on a request whose caller has already given up.`,
    related: ['Timeouts', 'Fail fast'],
  },
  {
    term: 'Fail fast',
    aliases: ['fail-fast'],
    summary: 'Return an error immediately when success is unlikely, instead of making the caller wait to find out.',
    detail: `To **fail fast** is to answer "no" quickly: after a timeout, when a circuit breaker is open, when a rate limit or concurrency limit is reached. A fast error frees the caller to retry elsewhere, use a fallback or report the problem, and frees your own resources at once.

A slow failure is the worst outcome: the caller waits, holds resources, and still gets nothing. During an outage, a breaker that refuses in under a millisecond is far kinder to clients than calls that each wait 200 ms to time out.`,
    related: ['Timeouts', 'Circuit breaker', 'Load shedding'],
  },
  {
    term: 'Retries',
    aliases: ['retry'],
    summary: 'Repeating a failed call in the hope that the failure was temporary.',
    detail: `A **retry** repeats a call after a transient failure: no answer, or an explicit "busy". Permanent errors such as \`not_found\` should be passed on at once, since asking again gives the same answer.

Retries add load exactly when a dependency is struggling, so they need three controls: a cap on attempts (for example four calls per request), growing waits (exponential backoff) and random waits (jitter). The worst case must fit the deadline: four 200 ms timeouts plus 100 + 200 + 400 ms of waiting is 1.5 s.

Only idempotent operations are safe to retry blindly, because a call that timed out may already have taken effect.`,
    related: ['Exponential backoff', 'Jitter', 'Retry budget', 'Transient failure'],
  },
  {
    term: 'Transient failure',
    aliases: ['transient error', 'permanent error', 'retryable error'],
    summary: 'A failure likely to go away if you try again shortly, such as a lost packet or a busy server.',
    detail: `**Transient** failures come from conditions that pass: a dropped packet, a server restarting, a moment of overload ("busy", HTTP 503 or 429). Retrying them, with backoff, usually succeeds.

**Permanent** failures will be the same next time: the key does not exist, the request is malformed, the caller lacks permission. Retrying them only adds load and delays the error.

Classifying errors correctly is the first step in any retry policy; gRPC and AWS SDKs, for instance, list which status codes are retryable.`,
    related: ['Retries'],
  },
  {
    term: 'Exponential backoff',
    aliases: ['backoff', 'exponential back-off'],
    summary: 'Waiting longer before each retry, typically doubling the maximum wait after every failure.',
    detail: `With **exponential backoff**, the wait before retry *n* grows geometrically: ceilings of 100, 200 and 400 ms, for example. A brief glitch costs a short wait; a longer outage gets progressively more breathing room.

Backoff alone does not de-synchronise clients: 24 requests that failed together and all wait exactly 100 ms retry together. It is almost always combined with jitter.

Implementations cap the ceiling (no wait longer than a few seconds) and the number of attempts, so the total stays within the request's deadline.`,
    related: ['Jitter', 'Retries'],
  },
  {
    term: 'Jitter',
    aliases: ['full jitter', 'randomised backoff'],
    summary: 'Randomising retry waits so that clients which failed together do not retry together.',
    detail: `**Jitter** makes each wait random, for example \`random() × ceiling\` ("full jitter"). Requests that failed at the same instant then pick different waits, and their retries spread across the window instead of arriving as one spike.

Without it, retries stay synchronised: every round, the same crowd hits the upstream at the same moment, often failing again for the same reason. AWS's analysis of backoff strategies found full jitter cuts total work and completion time substantially compared with plain exponential backoff.

Jitter is also used on periodic jobs and cache TTLs, so that many machines do not act in lockstep.`,
    related: ['Exponential backoff', 'Retries'],
  },
  {
    term: 'Retry budget',
    aliases: ['max attempts', 'retry cap'],
    summary: 'A limit on how much extra load retries may add, per request or as a share of all traffic.',
    detail: `A **retry budget** bounds retry amplification. The simplest form caps attempts per request: with four calls at most, a dead upstream sees at most four times normal traffic. A stronger form caps retries as a fraction of requests, say 10%, so that when most calls fail, retries stop rather than multiply.

Budgets matter most across layers: if three layers each make three attempts, one failure at the bottom becomes 27 calls. gRPC's retry throttling and the AWS SDKs' retry token bucket are budget mechanisms.`,
    related: ['Retries', 'Exponential backoff'],
  },
  {
    term: 'Circuit breaker',
    aliases: ['breaker'],
    summary: 'A guard that stops calling a dependency after repeated failures, refuses fast for a while, then probes for recovery.',
    detail: `A **circuit breaker** wraps calls to a dependency and has three states. **Closed**: calls go through and consecutive failures are counted; after a threshold (say 5 in a row) it opens. **Open**: calls are refused immediately. After a cool-down (say 1000 ms) it becomes **half-open** and lets a single probe through; success closes it, failure reopens it.

During an outage it turns 200 ms timeouts into instant refusals and nearly removes traffic from the dependency so it can recover. A success resets the failure count, so scattered failures never trip it.

The pattern was popularised by Michael Nygard's *Release It!* and implemented in Netflix Hystrix, Resilience4j and Polly. Envoy's "circuit breaking" settings are, despite the name, concurrency limits.`,
    related: ['Half-open', 'Cool-down', 'Fail fast'],
  },
  {
    term: 'Half-open',
    aliases: ['half-open state'],
    summary: 'The circuit breaker state in which exactly one probe call is allowed through to test whether the dependency has recovered.',
    detail: `When a breaker's cool-down ends, it does not close straight away. It goes **half-open**: the next request becomes a probe and is sent; every other request is refused until the probe reports.

If the probe succeeds, the breaker closes and normal traffic resumes. If it fails, the breaker opens again and the cool-down restarts. This lets the breaker learn about recovery while exposing the recovering dependency to only one call instead of the full load.

Late results from calls sent before the breaker opened must not be mistaken for the probe's; only the probe decides.`,
    related: ['Circuit breaker', 'Cool-down'],
  },
  {
    term: 'Cool-down',
    aliases: ['cooldown', 'open duration', 'reset timeout'],
    summary: 'How long a circuit breaker stays open before letting a probe through.',
    detail: `The **cool-down** is the period after a breaker opens during which it refuses everything. Once it has passed, the next request becomes a half-open probe. It can be checked lazily, by comparing \`now − openedAt\` with the cool-down when a request arrives; no timer is needed.

Short cool-downs detect recovery quickly but probe a dead dependency often; long ones give it more quiet but delay recovery. If the dependency comes back just after a failed probe, users see errors for nearly a whole extra cool-down. Some implementations grow the cool-down after repeated failed probes.`,
    related: ['Circuit breaker', 'Half-open'],
  },
  {
    term: 'Token bucket',
    summary: 'A rate limiter: tokens accumulate at a fixed rate up to a cap, and each request must take one.',
    detail: `A **token bucket** holds up to B tokens and gains r tokens per second. Each admitted request removes one token; with no whole token left, the request is refused. In any window of t seconds it admits at most B + r·t requests, so B is the largest burst and r the long-run rate.

With B = 10 and r = 20 per second: after a quiet period, 10 requests can pass at once; under sustained load, 20 per second pass.

It needs no timer: store the token count and the last update time, and on each request add elapsed × r, capped at B, keeping fractions. It is used in network traffic shaping, API gateways, Envoy's rate limiting and the AWS SDK's retry quota.`,
    related: ['Rate limiting', 'Burst', 'Admission control'],
  },
  {
    term: 'Rate limiting',
    aliases: ['rate limit', 'throttling'],
    summary: 'Capping how many requests are admitted per unit of time and refusing the excess immediately.',
    detail: `**Rate limiting** decides in advance how much traffic to send or accept, and refuses the rest cheaply (\`rate_limited\`, HTTP 429) instead of passing it on to a dependency that would slow down or fail.

Common algorithms are the token bucket, the leaky bucket and sliding-window counters. Fixed windows ("at most 20 per calendar second") are simpler but allow bursts of twice the limit across a window boundary.

A local limit is easy; a global limit across many nodes needs shared state, such as a Redis counter or a central rate-limit service, with its own latency and failure modes.`,
    related: ['Token bucket', 'Burst', 'Admission control'],
  },
  {
    term: 'Burst',
    aliases: ['burst size', 'bursts'],
    summary: 'Many requests arriving at almost the same instant, and the number a rate limiter will admit at once.',
    detail: `Traffic is rarely smooth: a page load fires ten calls together, a cron job starts on the minute. A **burst** is such a cluster of near-simultaneous requests.

A token bucket's capacity sets its **burst size**: with capacity 10, a burst of 30 after a quiet period has 10 admitted at once and 20 refused, while the long-run rate is unaffected. A larger capacity is friendlier to bursty clients but lets more load hit the dependency at one moment.`,
    related: ['Token bucket', 'Rate limiting'],
  },
  {
    term: 'Admission control',
    summary: 'Deciding at the entrance whether a request may be processed at all, before it consumes resources.',
    detail: `**Admission control** checks each request against a policy as it arrives and refuses those that would exceed capacity: too many per second (rate limiting), too many in progress (a concurrency limit) or too little time left (an expired deadline).

The point is to refuse early, while refusing is cheap. A request rejected at the door costs microseconds; one that is accepted and then times out deep in the system has consumed threads, memory and downstream calls for nothing.`,
    related: ['Rate limiting', 'Load shedding', 'Concurrency limit'],
  },
  {
    term: 'Bulkhead',
    aliases: ['bulkheads'],
    summary: 'Isolating a dependency behind a fixed pool of slots, so its slowness cannot consume all of a service\'s resources.',
    detail: `A **bulkhead** gives a dependency a bounded share of resources, for example at most 4 calls in flight, or a dedicated thread pool. When the dependency slows down, only its slots fill up; the rest of the service keeps working. The name comes from the watertight walls that divide a ship's hull.

Requests that find every slot taken are refused at once rather than queued. Each slot must be returned however its call ends; a leaked slot is lost for good, and after a few leaks the service refuses everything.

Resilience4j offers semaphore and thread-pool bulkheads; Envoy's per-cluster connection and request limits play the same role.`,
    related: ['Concurrency limit', 'Load shedding'],
  },
  {
    term: 'Load shedding',
    summary: 'Deliberately refusing some requests when overloaded, so the rest can be served in time.',
    detail: `Under overload, a service can either try to serve everything (and serve everything late, or crash) or **shed load**: refuse some requests quickly so that those it accepts are answered within their deadlines.

Shedding beats queueing because a request that waits in a queue past its caller's deadline is wasted work, and the queue hides the overload from callers who could back off. Good shedding refuses cheaply and early, and may prefer to drop less important traffic (background jobs before user requests).`,
    related: ['Bulkhead', 'Admission control', 'Fail fast'],
  },
  {
    term: 'Concurrency limit',
    aliases: ['max in flight', 'in-flight limit'],
    summary: 'A cap on how many operations may be in progress at the same time.',
    detail: `A **concurrency limit** bounds work in progress rather than requests per second. By Little's law, in-flight = rate × latency, so a fixed limit admits more per second when calls are fast and fewer when they are slow: 4 slots at 50 ms per call allow about 80 calls per second, at 150 ms about 27.

That makes it adapt automatically to a dependency that slows down, which a fixed rate limit does not. Adaptive variants (such as Netflix's concurrency-limits library) adjust the limit itself based on observed latency.`,
    related: ['Bulkhead', 'Load shedding'],
  },
  {
    term: 'Cascading failure',
    summary: 'A failure in one component that overloads or blocks others, spreading until much of the system is down.',
    detail: `In a **cascading failure**, one sick component makes its callers sick. A dependency hangs; callers without timeouts run out of threads waiting for it; their own callers then hang; and so on up the chain. Or a server fails, its load shifts to the others, and they fail in turn under the extra load.

Retries often feed the cascade by multiplying load at the worst moment. Timeouts, retry budgets, circuit breakers, bulkheads and load shedding each cut one path along which the failure spreads.`,
    related: ['Timeouts', 'Circuit breaker', 'Bulkhead'],
  },
];
