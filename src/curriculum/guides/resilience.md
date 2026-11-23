## The problem

A checkout service, n1, calls a payment provider for every order. It has 200 worker threads. On a normal day the provider answers in 80 ms, so each thread is busy for a moment and free again.

At 14:02 the provider's load balancer starts dropping connections silently. Calls do not fail; they hang. n1 has no timeout on the call, so each thread that calls the provider waits, and waits. Requests arrive at 100 per second. Two seconds later all 200 threads are stuck waiting for the provider, and n1 stops answering anything, including requests that never touch payments, like "show my cart". The web tier in front of n1 has its own threads waiting on n1, and within a minute the whole site is down. The provider's problem has become everyone's problem.

At 14:09 the provider recovers. n1's threads are freed, and every client that saw an error retries, immediately and all at once. The provider, still warming up, gets three times its normal load in the first second and falls over again.

Nothing here was a bug in the usual sense. Each piece did something reasonable: wait for an answer, try again after a failure. The failure came from what those reasonable things do when a dependency is slow or down. **Resilience patterns** are the small set of rules that keep one sick dependency from spreading: stop waiting, retry carefully, stop calling something that is clearly broken, and cap how much you send and how much you hold.

## The core idea

In this track your node is a small service in front of an **upstream**: a dependency it calls to answer its own clients. Clients send `get`; your node sends `fetch` upstream and passes the value on.

```json
{ "type": "fetch", "key": "k7" }
{ "type": "fetch_ok", "value": "k7/s123" }
{ "type": "error", "code": "timeout" }
```

### Timeouts and deadlines

A **timeout** is a limit on how long you wait for one call. A **deadline** is a promise to your own caller: you will hear from me by then. The two are linked. If a client gives you 300 ms, everything you do for it has to fit: the trip from the client (up to 12 ms), your upstream call, and the trip back (up to 12 ms). So the upstream timeout must be longer than the upstream's slowest normal answer, or you will give up on calls that were about to succeed, and short enough to leave room for the reply.

In numbers: the upstream normally answers within 150 ms, which with network time is about 175 ms seen from your node. The deadline leaves at most 300 − 24 = 276 ms. Any timeout between those works; 200 ms leaves margin on both sides.

When the timeout fires, reply with an error at once. This is **fail fast**: a quick, clear "no" is more useful than a slow one, because the caller can retry elsewhere, show a fallback or give up, and it is not holding a thread while it waits.

> A timeout does not tell you the upstream failed. It tells you that you stopped waiting. The call may still complete on the other side.

That last point matters for retries: a request that timed out might have been processed.

### Retries, backoff and jitter

Many failures are brief: a lost packet, a server restarting, a moment of overload. Trying again usually works. But a retry is extra load, and it arrives exactly when the upstream is struggling.

First, decide what is worth retrying. **Transient** failures may pass: no answer, or "busy". **Permanent** ones will not: "not_found" will be "not_found" next time too. Retrying a permanent error only adds load.

Second, wait between attempts and make each wait longer than the last. This is **exponential backoff**: the ceiling of the wait doubles after each failure, for example 100, 200, 400 ms. It gives a struggling upstream more time to recover the longer it struggles.

Third, randomise each wait. With **jitter**, the actual wait is `random() × ceiling`. Without it, requests that failed together retry together. Picture 24 requests that all receive "busy" at the same instant. With a fixed 100 ms wait, all 24 retry at exactly t+100, meet the same overloaded upstream, fail again together, and repeat. Jitter spreads them across the whole window so the upstream sees a trickle instead of a spike.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="Retries without jitter arrive as synchronised spikes; with jitter they spread out">
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="10" y="40">no jitter</text><text x="10" y="140">full jitter</text>
  </g>
  <g style="stroke: var(--line)"><line x1="100" y1="80" x2="610" y2="80"/><line x1="100" y1="180" x2="610" y2="180"/></g>
  <g style="fill: var(--bad)">
    <rect x="110" y="20" width="8" height="60"/><rect x="210" y="20" width="8" height="60"/><rect x="410" y="20" width="8" height="60"/>
  </g>
  <rect x="110" y="120" width="8" height="60" style="fill: var(--bad)"/>
  <g style="fill: var(--m2)">
    <rect x="125" y="168" width="8" height="12"/><rect x="140" y="160" width="8" height="20"/><rect x="160" y="165" width="8" height="15"/><rect x="180" y="162" width="8" height="18"/><rect x="200" y="170" width="8" height="10"/>
    <rect x="230" y="172" width="8" height="8"/><rect x="260" y="168" width="8" height="12"/><rect x="290" y="172" width="8" height="8"/><rect x="330" y="170" width="8" height="10"/><rect x="370" y="174" width="8" height="6"/><rect x="420" y="172" width="8" height="8"/><rect x="480" y="174" width="8" height="6"/><rect x="540" y="175" width="8" height="5"/>
  </g>
  <g style="font: 11px var(--mono); fill: var(--muted)">
    <text x="104" y="96">t=0</text><text x="200" y="96">+100</text><text x="400" y="96">+300</text>
    <text x="104" y="196">t=0</text><text x="300" y="196">calls spread over 0–700 ms</text>
  </g>
  <text x="250" y="45" style="fill: var(--bad); font: 12px var(--sans)">24 calls at once, every round</text>
</svg>
<figcaption>The same 24 failed requests retrying with fixed waits (top) and with random waits under a doubling ceiling (bottom).</figcaption>
</figure>

Fourth, cap the attempts. Here a request may make at most four upstream calls: one first try and three retries. A **retry budget** bounds the extra load: when the upstream is completely down, it receives at most four times the normal traffic, not an unbounded amount. Check the worst case against the deadline: four 200 ms timeouts plus at most 100 + 200 + 400 ms of waiting is 1.5 s, inside a 2 s deadline.

Retrying a call that may have succeeded is only safe if the operation is idempotent, or the upstream deduplicates. A `fetch` is a read, so it is safe; a "charge card" call needs an idempotency key.

### Circuit breakers

When the upstream is down for minutes, retries are not enough. Every request still waits 200 ms (or 1.5 s with retries) to learn what the last hundred requests already learned, and the upstream, trying to restart, is hit by a steady stream of calls it cannot answer.

A **circuit breaker** notices the pattern and stops calling for a while. It sits in front of the upstream call and has three states:

<figure>
<svg viewBox="0 0 600 210" role="img" aria-label="Circuit breaker states: closed, open, half-open and their transitions">
  <g style="stroke: var(--line); fill: var(--surface)">
    <rect x="30" y="80" width="130" height="50" rx="25" style="fill: var(--surface); stroke: var(--good)"/>
    <rect x="235" y="80" width="130" height="50" rx="25" style="fill: var(--surface); stroke: var(--bad)"/>
    <rect x="440" y="80" width="130" height="50" rx="25" style="fill: var(--surface); stroke: var(--warn)"/>
  </g>
  <g style="font: 14px var(--sans); fill: var(--ink)" text-anchor="middle">
    <text x="95" y="110">closed</text><text x="300" y="110">open</text><text x="505" y="110">half-open</text>
  </g>
  <g style="stroke: var(--ink); stroke-width: 1.5; fill: none">
    <path d="M150 85 C200 55 230 55 245 82"/><polyline points="237,77 245,82 246,73"/>
    <path d="M355 85 C400 55 430 55 452 82"/><polyline points="444,77 452,82 453,73"/>
    <path d="M450 125 C400 165 340 165 350 128"/><polyline points="345,136 350,128 355,136"/>
    <path d="M505 130 C470 205 130 205 95 132"/><polyline points="92,141 95,132 102,139"/>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)" text-anchor="middle">
    <text x="198" y="48">5 failures in a row</text><text x="403" y="48">1000 ms passed: next request probes</text>
    <text x="400" y="178">probe fails</text><text x="300" y="200">probe succeeds</text>
    <text x="95" y="72">call; count failures</text><text x="300" y="148">refuse at once</text>
  </g>
</svg>
<figcaption>Closed calls normally; open refuses without calling; half-open lets exactly one probe through and lets it decide.</figcaption>
</figure>

- **Closed**: call as normal and count failures *in a row*. A failure is a timeout or any error reply; a success resets the count to zero, so scattered failures never trip it. After five failures in a row, open.
- **Open**: do not call. Reply `circuit_open` immediately: fail fast.
- After a **cool-down** of 1000 ms, the next request becomes a probe and the breaker is **half-open**: exactly one call is out, and everything else is refused. If the probe succeeds, close. If it fails, open again and restart the cool-down.

The breaker trades a few refused requests for two things: answers in microseconds instead of 200 ms during an outage, and quiet for the upstream so it can recover. The single probe is how it learns the outage is over without letting the full load back in at once. No timer is needed for the cool-down: compare `now − openedAt` with 1000 when a request arrives.

One race: calls sent just before the breaker opened come back while it is open or half-open. Their results are old news. Count them only if the breaker is still closed; otherwise a stale success could close a breaker whose probe has not reported yet.

### Rate limiting with a token bucket

Some upstreams fail not because they are broken but because they are asked too much: a partner API with a quota, a database that slows sharply past some load. **Rate limiting** decides in advance how much you will send and refuses the excess yourself, quickly and cheaply. This is **admission control**: deciding at the door whether a request may enter at all.

The usual tool is a **token bucket**. A bucket holds at most 10 tokens and starts full. Tokens drip in at 20 per second. An admitted request takes one token; if there is no whole token, the request is refused with `rate_limited`. The bucket allows a **burst** of up to 10 requests after a quiet period, while holding the long-run rate to 20 per second: over any stretch of *t* seconds it admits at most 10 + 20*t*.

No timer is needed to add tokens. Store the token count and the time you last updated it; on each request, add `elapsed_ms × 20 / 1000`, cap at 10, keep the fraction. Rounding down on each request throws away partial tokens and admits too few. Refilling 10 tokens once a second looks similar but is not: it lets 10 through just before the refill and 10 more just after, 20 within a few milliseconds.

### Bulkheads and load shedding

A rate limit counts requests per second. When the upstream slows down, each request occupies a connection, memory and a place in the upstream's queue for longer, so the same rate means more work in progress. A **bulkhead** bounds that directly with a **concurrency limit**: at most 4 upstream calls in flight. A request that arrives with all 4 slots busy is refused at once with `overloaded`. The name comes from the walls that divide a ship's hull, so one leak cannot sink the ship.

Refusing quickly instead of queueing is **load shedding**. A queued request would usually wait past its deadline anyway, and a queue hides the overload from callers who could otherwise back off or go elsewhere.

A concurrency limit adapts to latency on its own. When calls take 50 ms, 4 slots admit about 80 requests per second; when they take 150 ms, about 27. The limit follows the upstream's real capacity without anyone tuning a rate. The one rule: a slot is given back when the call ends, *however* it ends: answer, error or timeout. A slot that leaks is lost for good, and after a few leaks the node refuses everything.

## A worked example

Node `n1` with a circuit breaker (threshold 5, cool-down 1000 ms, call timeout 200 ms) in front of `upstream`. Requests arrive every 50 ms. The upstream goes silent at t=1500 and recovers at t=3000.

| t (ms) | request | breaker before | what happens | breaker after |
|---|---|---|---|---|
| 1450 | r1 | closed, 0 failures | `fetch_ok` at 1470 | closed, 0 |
| 1500 | r2 | closed, 0 | call sent; upstream now silent | closed, 0 |
| 1550–1700 | r3–r6 | closed | four more calls sent, all hanging | closed, 0 |
| 1700 | (r2's timeout) | | r2 times out, client gets `timeout` | closed, 1 |
| 1750–1900 | r7–r10 sent; r3–r6 time out | | failures 2, 3, 4, 5 | **open** at 1900 |
| 1950–2850 | r11–r29 | open | refused at once with `circuit_open`, no call | open |
| 1950–2100 | (r7–r10's timeouts) | open | results ignored: breaker is not closed | open |
| 2900 | r30 | open, 1000 ms since 1900 | becomes the probe; call sent | half-open |
| 2950 | r31 | half-open | refused: the probe is still out | half-open |
| 3100 | (probe times out) | half-open | probe failed; cool-down restarts | open, openedAt=3100 |
| 4100 | r54 | open, 1000 ms passed | probe sent; upstream is back, `fetch_ok` at 4120 | **closed** at 4120 |
| 4150 | r55 | closed | normal call, succeeds | closed, 0 |

During the 1.5 s outage the upstream received 9 calls from n1 (r2–r10) plus one probe, instead of 30. From t=1950 most clients got an answer in under a millisecond instead of waiting 200 ms. The cost is that recovery was noticed 1.1 s late: the upstream was back at 3000, but the next probe only went out at 4100.

## Why it is correct

These patterns do not make anything succeed. Each bounds a quantity, and the arguments are about those bounds.

**Timeouts bound waiting.** If every outbound call has a timeout T and the handler replies as soon as the call returns or times out, a reply leaves within T plus local processing of receiving the request. Choosing T ≤ deadline − network time guarantees the caller hears back in time, whatever the upstream does.

**The retry cap bounds amplification.** With at most k attempts per request, the upstream sees at most k times the incoming request rate, even if it is completely down. Backoff with jitter does not change that worst case but spreads the retries in time: requests that failed together at t pick independent waits in [0, ceiling), so their next calls are spread across the window instead of coinciding.

**The breaker bounds calls during an outage.** While closed, at most `threshold` consecutive failures are seen before opening, plus whatever was already in flight. While open, zero calls. While half-open, exactly one. So over an outage of length D, calls are at most the threshold plus calls already in flight when it opened, plus about D / cool-down probes. Because only consecutive failures count, a healthy upstream with scattered failures (never two in a row) never opens it.

**The token bucket bounds admissions in every window.** The bucket holds at most B tokens and gains at most r·t tokens in time t, and each admission removes one. So in any window of length t, admissions ≤ B + r·t. This holds for every window, not only aligned seconds, which is exactly the property the fixed once-a-second refill lacks.

**The bulkhead bounds concurrency.** The counter increases by one per call sent and decreases by one per call ended, and a call is sent only if the counter is below the limit. As long as every path out of a call decrements, the number of calls in progress never exceeds the limit. All of this relies on timeouts: without them, a call that never ends holds its slot forever.

## Trade-offs and failure modes

**Timeouts are hard to choose.** Too long and they protect nothing; too short and they fail healthy calls during normal latency spikes, which, combined with retries, adds load exactly during slow periods. Deadlines should shrink as a request travels down a chain: if the edge allows 300 ms, a call three layers down should not wait 500 ms. Passing the remaining deadline along with the request, as gRPC does, makes this automatic.

**Retries multiply across layers.** If each of three layers makes up to three attempts, a failure at the bottom causes up to 3 × 3 × 3 = 27 calls to it. Retry at one layer, usually the one closest to the user, and propagate failures elsewhere. A retry budget per client (retries may be at most 10% of requests, for example) is safer than a per-request cap alone.

**Breakers can be too sensitive or too slow.** A low threshold opens on a brief blip; a long cool-down delays recovery, as in the worked example. Per-node breakers each learn independently, so in a large fleet the upstream still sees a probe from every node. A breaker also cannot tell which requests would have succeeded: if only one key is broken, a breaker per upstream refuses all of them.

**Rate limits need the right number.** A local limit of 20 per second on each of 10 nodes is a global limit of 200 only if traffic is spread evenly. Global limits need shared state (a Redis counter, a central service) with its own latency and failure modes.

**Shedding and refusing are failures too.** Every `overloaded`, `rate_limited` or `circuit_open` is a request that did not succeed. The patterns are worth it because the alternative is worse: everything slow, then everything down. They need monitoring, or they hide real capacity problems.

**Fallbacks need testing.** Answering from a cache or a default when the upstream fails is often better than an error, but a fallback path that runs only during outages is the least tested code you have.

## In real systems

**gRPC** carries a deadline with every call and propagates it to downstream calls, so a server knows how much time its caller has left and fails with `DEADLINE_EXCEEDED` when it runs out. Its retry policy configures maximum attempts, exponential backoff and which status codes are retryable, and a retry throttling setting stops retries when too many calls fail.

**AWS SDKs** retry throttling and transient errors with exponential backoff and jitter by default, cap the attempts, and in their "standard" retry mode spend from a client-side retry token bucket so that a failing service sees fewer retries. The AWS Architecture Blog post on backoff and jitter is the standard reference for why full jitter works.

**Resilience4j** (Java, successor to Netflix's Hystrix) provides each pattern as a separate component: CircuitBreaker with closed, open and half-open states and failure-rate thresholds over a sliding window, RateLimiter, Bulkhead (semaphore or thread-pool based), Retry and TimeLimiter.

**Envoy** applies them at the proxy: per-route timeouts and retry policies with backoff, "circuit breaking" thresholds that are really bulkheads (maximum connections, pending requests and active requests per upstream cluster), outlier detection that ejects failing hosts, and local and global rate limiting with token buckets.

## Common misconceptions

- **"A timeout means the call failed."** It means you stopped waiting. The upstream may have processed the request, which is why retrying non-idempotent calls needs care.
- **"More retries make a service more reliable."** Past a small number, retries mostly add load during the very outages they are meant to survive, and can keep an upstream from recovering.
- **"Exponential backoff alone prevents retry storms."** Without jitter, requests that failed together still retry together, just less often. Randomness is what spreads them out.
- **"A circuit breaker protects the caller only."** Its main value is often to the upstream: it removes nearly all traffic during the outage so the upstream can restart and recover.
- **"Queue requests instead of refusing them."** A queue in front of an overloaded dependency only delays the refusal until after the deadline, and uses memory while it does. Bounded queues are fine; unbounded ones turn overload into an outage.

## How the tasks build it

[RS-1 Timeouts and deadlines](#/c/resil-timeout) asks for one reply within 300 ms per request, whatever the upstream does. The upstream is sometimes slow, sometimes silent, and dark for 1.5 s; the tests check that good answers are passed on and the rest become timeout errors in time.

[RS-2 Retries with backoff and jitter](#/c/resil-retry) retries transient failures with at most four calls per request. The tests check that `not_found` is never retried, that 95% of good requests succeed despite 30% failing calls, and that requests failing together spread their retries and wait longer each round.

[RS-3 Circuit breaker](#/c/resil-breaker) adds the three-state breaker. Scattered failures must not open it; during silent and `busy` outages the upstream may receive at most 30 calls from two nodes, most requests must be refused within 50 ms, and requests must succeed again within 2 s of recovery.

[RS-4 Rate limiting with a token bucket](#/c/resil-rate-limit) admits 20 per second with bursts of 10. The tests examine every window of time, not just whole seconds, under steady overload and sudden bursts, and require most of what a perfect bucket would admit.

[RS-5 Bulkhead: bounded concurrency](#/c/resil-bulkhead) caps upstream calls in flight at 4 and sheds the rest. Under bursts on a slow upstream and with 30% of calls never answered, a slot that leaks on any path shows up as the node refusing everything.

## Further reading

- [Exponential Backoff and Jitter (AWS Architecture Blog)](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) — simulations comparing no jitter, full jitter and decorrelated jitter, with clear graphs.
- [Timeouts, retries, and backoff with jitter (Amazon Builders' Library)](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/) — how Amazon chooses timeouts and limits retries in layered services.
- [Circuit Breaker (Martin Fowler)](https://martinfowler.com/bliki/CircuitBreaker.html) — a short, clear description of the pattern and its states.
- [Google SRE book: Handling Overload](https://sre.google/sre-book/handling-overload/) and [Addressing Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/) — load shedding, retry budgets and deadline propagation from operating very large services.
- [gRPC: Deadlines](https://grpc.io/docs/guides/deadlines/) — how deadlines are set by clients and propagated through a chain of services.
- [Release It! (Michael Nygard)](https://pragprog.com/titles/mnee2/release-it-second-edition/) — the book that named and popularised circuit breakers, bulkheads and many related stability patterns.
