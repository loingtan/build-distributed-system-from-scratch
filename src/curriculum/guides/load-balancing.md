## The problem

A service runs on four identical machines, b1 to b4, behind a single address. Something has to decide, for each incoming request, which machine handles it. Suppose that something sends requests in turn: b1, b2, b3, b4, b1, and so on. On a calm day every backend gets a quarter of the traffic and answers in 20 ms.

Then b3's disk starts failing and each request there takes 300 ms. The turn-taking does not notice. It keeps sending b3 a quarter of all requests, so a quarter of all users wait fifteen times longer than the rest, while b1, b2 and b4 sit mostly idle. If you measure the 95th percentile latency, it has gone from 25 ms to 300 ms because of one machine.

It gets worse. Suppose b3 does not slow down but breaks: its database connection is gone and it answers every request with an error, in one millisecond. A balancer that sends work to whichever backend has the fewest requests waiting will love b3: it is never busy. Within a second, most requests are going to the one machine that cannot serve any of them. A single broken machine fails most of your traffic, and a smarter balancer made that worse than the naive one.

Load balancing is the job of choosing a backend for each request so that capacity is used well, slow machines get less, broken machines get nothing, and requests that benefit from landing in the same place do so.

## The core idea

### The balancer as a reverse proxy

A **load balancer** sits between clients and a pool of **backends**: identical copies of a service. Clients only ever talk to the balancer. For each request it picks a backend, forwards the request, waits for the answer and passes it back. This is a **reverse proxy**: a proxy that acts on behalf of the servers rather than the clients. Clients never learn which backend did the work, which is what lets you add, remove and replace backends freely.

Relaying a request is mostly bookkeeping. Many requests are in flight at once and backends answer in any order, so the balancer must pair each answer with the client that asked. In the tasks each request handler awaits its own call, which keeps the pairing automatic. The **request relay** must also cope with the pool changing under it: a `set_backends` message can arrive at any time, and from then on only the new members may get traffic.

### Round robin

**Round robin** gives backends turns in order. Keep a counter, choose `pool[counter % pool.length]`, add one. It needs no information about the backends, and over any run of requests every backend gets the same count, give or take one. Its weakness is the opening story: it assumes every backend is equally fast, and it keeps that assumption no matter what happens.

One detail matters in an asynchronous balancer: pick and advance the counter *before* waiting for the backend. Code after a wait runs later, interleaved with other requests; a counter advanced after the wait lets a burst of requests all read the same value.

### Least outstanding requests

A better rule is to send each request to the backend with the **fewest outstanding requests**: requests sent to it and not yet answered. The balancer already knows this number; it increments a counter when it forwards and decrements it when the answer (or a timeout) comes back.

Why does this adapt to speed without measuring anything? By Little's law, the number of requests a server holds at once equals its arrival rate times the time each one takes. A backend that takes fifteen times longer per request holds fifteen times as many for the same arrival rate, so its count stays high and new requests go elsewhere. When it speeds up, its count drops and it gets its share back. This is **adaptive routing**: the choice reacts to conditions as they change.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="Balancer with in-flight counts per backend; slow b3 has the highest count and gets no new request">
  <rect x="20" y="88" width="90" height="44" rx="6" style="fill: var(--accent-soft); stroke: var(--accent)"/>
  <text x="65" y="115" text-anchor="middle" style="fill: var(--ink); font: 13px var(--mono)">n1 (LB)</text>
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="330" y="10" width="120" height="40" rx="6"/><rect x="330" y="65" width="120" height="40" rx="6"/>
    <rect x="330" y="120" width="120" height="40" rx="6" style="fill: var(--surface); stroke: var(--warn)"/><rect x="330" y="175" width="120" height="40" rx="6"/>
  </g>
  <g style="fill: var(--ink); font: 13px var(--mono)">
    <text x="345" y="35">b1</text><text x="345" y="90">b2</text><text x="345" y="145">b3</text><text x="345" y="200">b4</text>
  </g>
  <g style="fill: var(--muted); font: 12px var(--mono)">
    <text x="380" y="35">2 out</text><text x="380" y="90">1 out</text><text x="380" y="145" style="fill: var(--warn)">9 out</text><text x="380" y="200">2 out</text>
  </g>
  <g style="stroke: var(--line); stroke-width: 1.5">
    <line x1="110" y1="102" x2="330" y2="30"/><line x1="110" y1="128" x2="330" y2="140"/><line x1="110" y1="132" x2="330" y2="195"/>
  </g>
  <line x1="110" y1="108" x2="330" y2="85" style="stroke: var(--accent); stroke-width: 3"/>
  <text x="180" y="88" style="fill: var(--accent); font: 12px var(--sans)">next request</text>
  <text x="465" y="145" style="fill: var(--warn); font: 12px var(--sans)">300 ms each</text>
  <text x="465" y="90" style="fill: var(--muted); font: 12px var(--sans)">20 ms each</text>
</svg>
<figcaption>Nobody measures b3's latency. It simply holds more requests at once, so least-outstanding routing passes it over.</figcaption>
</figure>

Two details make it work. Count a request the moment you send it: when twelve requests arrive in the same millisecond, the second must already see the first, or all twelve go to the same idle backend. And break ties at random: with a fixed scan order, the first backend in the list wins every tie, which on a quiet system is nearly every request.

The point of all this is **tail latency**: the latency of the slowest few percent of requests (the 95th or 99th percentile). Averages hide a slow backend; the tail exposes it, and users who hit the tail are the ones who notice.

### Health checks, failover and retries

Least outstanding has the blind spot from the opening: a backend that fails fast looks idle. A crashed backend has the opposite problem: it answers nothing, so each request sent to it waits for a full timeout. A **black-hole backend** like this swallows requests silently.

The fix is **active health checks**: every 200 ms, the balancer sends each backend a small probe (`{ "type": "health" }`) with a short timeout. Two failed probes in a row take a backend out of rotation; one good probe puts it back. The balancer keeps probing backends it has taken out, because a probe is the only way a removed backend can get back in. Moving traffic away from a failed backend to the healthy ones is **failover**.

Probes run on a schedule, so there is always a window between a backend failing and the balancer noticing: up to two probe intervals plus a timeout, roughly half a second here. Requests sent in that window still fail. **Retries** cover them: if a forward times out or returns anything other than success, try once more on a *different* backend. One retry is enough to hide a single failure; more than one multiplies load during an outage.

### Slow start

A backend that just recovered, or just joined, is cold: empty caches, unoptimised code, no warm connections to its database. It answers a health probe quickly, but real requests cost it far more than usual. Least outstanding makes this worse: a fresh backend has nothing in flight, so it looks like the best choice and gets a full share at once.

**Slow start** gives a newly in-service backend a **weight** that grows linearly from 0 to 1 over a **warm-up** period (three seconds in the tasks), and sends it about that fraction of a normal share. This is **weighted routing**: each backend's share is proportional to its weight. Least outstanding cannot express weights directly, so choose in two steps: with probability weight / (sum of all weights), send the request to a warming backend; otherwise pick among the warm ones by fewest in flight. With n backends of which one has weight w, it receives w / (n − 1 + w) of the traffic.

### Sticky routing and consistent hashing

Sometimes the right backend is not the least busy one but the one that has seen this key before. Backends often cache what they served recently: a user's profile, a session, a rendered page. Spread requests by load and every key is scattered across every backend, so every backend misses often. **Cache locality** improves when every request for a key goes to the same backend.

That is **session affinity**, or sticky routing. The obvious implementation, `hash(key) % N`, is stable while the pool is, but when N changes from 5 to 4, about four keys in five move, and every cache is cold at once.

**Consistent hashing** is the family of schemes that move only the keys that must move. The simplest is **rendezvous hashing** (highest random weight): for a key, compute `score = hash(key + "/" + backendId)` for every backend and choose the highest score. The answer depends only on the key and the *set* of backends, not their order.

<figure>
<svg viewBox="0 0 600 190" role="img" aria-label="Rendezvous scores for key user:1042 before and after b3 leaves">
  <g style="fill: var(--ink); font: 13px var(--sans)">
    <text x="20" y="22">key user:1042, pool b1..b5</text><text x="320" y="22">b3 leaves</text>
  </g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="20" y="52">b1</text><text x="20" y="80">b2</text><text x="20" y="108">b3</text><text x="20" y="136">b4</text><text x="20" y="164">b5</text>
    <text x="320" y="52">b1</text><text x="320" y="80">b2</text><text x="320" y="136">b4</text><text x="320" y="164">b5</text>
  </g>
  <g style="fill: var(--m1)">
    <rect x="50" y="40" width="120" height="16"/><rect x="50" y="68" width="60" height="16"/>
    <rect x="50" y="124" width="90" height="16"/><rect x="50" y="152" width="200" height="16" style="fill: var(--accent)"/>
    <rect x="350" y="40" width="120" height="16"/><rect x="350" y="68" width="60" height="16"/>
    <rect x="350" y="124" width="90" height="16"/><rect x="350" y="152" width="200" height="16" style="fill: var(--accent)"/>
  </g>
  <rect x="50" y="96" width="160" height="16" style="fill: var(--m3)"/>
  <text x="350" y="108" style="fill: var(--muted); font: 12px var(--sans)">(gone)</text>
  <text x="255" y="165" style="fill: var(--accent); font: 12px var(--sans)">wins</text>
  <text x="555" y="165" style="fill: var(--accent); font: 12px var(--sans)">wins</text>
</svg>
<figcaption>Each backend's score for a key never changes. Removing b3 changes the winner only for keys b3 was winning; this key stays on b5.</figcaption>
</figure>

When a backend leaves, only the keys it was winning need a new winner; for every other key the top score is unchanged. When one joins, it takes exactly the keys where it scores highest, about 1/N of them, and nothing else moves. A hash ring with many virtual points per backend gives the same guarantees and is what most databases use; rendezvous hashing is simpler when the pool is small.

## A worked example

Balancer `n1`, backends `b1`–`b4`, least-outstanding routing with health checks every 200 ms (150 ms timeout), two failures to remove, one success to restore, one retry on another backend with a 250 ms timeout. Traffic is about 60 requests per second. b2 crashes at t=1500 and answers nothing until t=4000.

| t (ms) | event | n1's view |
|---|---|---|
| 1400 | probe round: all four answer | all healthy |
| 1500 | b2 crashes | still all healthy |
| 1510 | request r91 goes to b2 (it had fewest in flight) | b2: 1 out |
| 1600 | probe round; b2's probe outstanding | |
| 1750 | b2's probe times out | b2 failures = 1, still in rotation |
| 1760 | r91's forward times out after 250 ms; retry on b4, success at 1785 | client gets `get_ok` 275 ms after asking |
| 1800 | probe round | |
| 1950 | b2's second probe times out | failures = 2: b2 removed |
| 1950–4000 | requests go to b1, b3, b4 only; b2 is still probed every 200 ms | b2 out |
| 4000 | b2 recovers | |
| 4000 | probe sent at 4000 gets `health_ok` at 4015 | b2 back in, failures = 0 |
| 4020 | b2 has nothing in flight; it receives the next few requests | b2 at full share |

Between t=1500 and t=1950 a handful of requests went to b2 first. Fewer than a quarter: each one hung there for 250 ms, so b2's in-flight count rose and least-outstanding routing passed it over more often. Each of those requests timed out and was retried elsewhere; none failed. From t=1950 on, b2 got nothing. With slow start, the row at t=4020 would differ: b2's weight would start at 0 and reach 1 at t=7015, so it would take about 10% of a share at t=4300 and a full share only after three seconds.

## Why it is correct

Each mechanism makes a specific, limited promise.

**Round robin is fair by counting.** The counter only grows, so between any two moments every backend's turn comes up the same number of times, give or take one. Taking the counter modulo the current pool size keeps the index valid when the pool shrinks; fairness holds within each stable stretch of the pool.

**Least outstanding is self-correcting.** If a backend's count is higher than another's, it receives no new requests until the gap closes. So no backend can pile up much more work than the others unless they are all full, and a backend's share is roughly proportional to its speed, because the faster it drains its requests, the more often its count is lowest. This assumes that every forward eventually decrements its counter. A timeout must decrement too, or a backend that once timed out stays "busy" forever.

**Health checks bound the exposure to a dead backend.** With probes every P ms, a timeout of T and removal after k failures, a backend that fails is out of rotation within about k·P + T ms (here 2 × 200 + 150 = 550 ms). One retry on a different backend turns "one backend fails during the window" into success, provided at least one other healthy backend exists and the retry fits in the client's deadline.

**Rendezvous hashing minimises movement by construction.** A key's winner is the maximum of a set of fixed scores. Removing an element that is not the maximum leaves the maximum unchanged; adding an element changes the maximum only if the new element is larger. So a key moves only if its old backend left or its new backend joined, and the order of the list never matters. With a good hash, each backend is the maximum for about 1/N of keys.

## Trade-offs and failure modes

**The balancer is now a dependency.** Every request passes through it, so it needs its own redundancy: real deployments run several balancers behind DNS, an anycast address or a floating IP.

**Each balancer sees only its own traffic.** With several balancers, least-outstanding counts are per balancer, not global. Each makes locally sensible choices that together can overload a backend. A common remedy is "power of two choices": pick two backends at random and send to the less loaded one, which avoids everyone choosing the same "best" backend.

**Health checks test the probe, not the service.** A backend can answer `/health` and still fail real requests (a broken dependency that the probe does not touch). Probing harder costs traffic: 10 probes per second per backend from each of 50 balancers is real load. **Passive** health checking (watching real responses and ejecting backends that fail, as Envoy's outlier detection does) complements active probes.

**Retries amplify.** One retry per request doubles load on the healthy backends exactly when capacity has dropped. With many layers each retrying, the multiplication compounds. Keep retries to one, and to a different backend.

**Ejecting too eagerly is its own outage.** If a shared dependency slows down, every backend fails its probe and a strict balancer removes all of them. Many balancers have a "panic threshold": below some fraction of healthy hosts, route to everyone.

**Sticky routing fights load balancing.** A hot key sends all its traffic to one backend, however busy. Consistent hashing balances the number of *keys*, not the load they bring. Systems combine the two with bounded loads (send to the next candidate if the first is over capacity) or by replicating hot keys.

## In real systems

**NGINX** balances upstream servers with round robin by default and offers `least_conn` (fewest active connections) and `hash ... consistent` (ketama consistent hashing) as alternatives. Open-source NGINX marks a server unavailable passively after `max_fails` failures within `fail_timeout`; active health checks and a `slow_start` parameter are part of NGINX Plus.

**HAProxy** supports `roundrobin`, `leastconn` and consistent hashing (`hash-type consistent`), with active health checks per server (`check`, `inter`, `rise`, `fall`) and a `slowstart` setting that ramps a recovered server's weight over a chosen time.

**Envoy** offers round robin, least request (by default using power of two choices), ring hash and Maglev for consistent hashing. It combines active health checking with outlier detection, which ejects hosts after consecutive errors, and has a configurable slow-start window for new hosts.

**AWS Elastic Load Balancing** (Application Load Balancer) routes with round robin or "least outstanding requests", checks target health on a configurable interval with healthy and unhealthy thresholds, offers a slow-start duration for newly registered targets, and supports sticky sessions with cookies.

## Common misconceptions

- **"Round robin is fair, so it is good enough."** It is fair in request count, not in work. Requests differ in cost and backends differ in speed, and round robin sends the slowest backend a full share however slow it gets.
- **"Least outstanding needs latency measurements."** It needs only a counter per backend. Slower backends hold requests longer, so their counts rise on their own.
- **"If a backend passes its health check, it is healthy."** The check proves only that the probe path works. A fast-failing backend that returns errors to real requests can pass a shallow probe; retries and passive checks catch what probes miss.
- **"Consistent hashing balances load."** It balances keys. One very popular key still lands on one backend, and with few backends the share per backend can vary noticeably unless you use many virtual nodes or a good hash.
- **"More retries mean more reliability."** Past one retry, extra attempts mostly add load to a system that is already short of capacity.

## How the tasks build it

[LB-1 Round robin](#/c/lb-round-robin) is the relay itself: forward each request, return the backend's answer, take turns. The tests send bursts to five backends of very different speeds, so answers come back out of order, and change the pool twice; request counts may differ by at most 2.

[LB-2 Least outstanding requests](#/c/lb-least-outstanding) switches the choice to fewest in flight. One backend is slow for the whole run or turns slow for two seconds, and the tests check that it then gets at most 12% of traffic, that it gets its share back once fast, and that no backend takes more than 5 of a 12-request burst.

[LB-3 Sticky routing with consistent hashing](#/c/lb-sticky-routing) sends every request for a key to the same backend while the pool changes. Backends leave, join and come back with the list in a different order; a key may move only if its backend left or its new backend joined.

[LB-4 Health checks and retries](#/c/lb-health-checks) adds probes and one retry elsewhere. One backend goes silent, another fails fast with errors; every request must still succeed, with at most two backend calls each, and a failed backend must stop getting traffic within a second.

[LB-5 Slow start](#/c/lb-slow-start) ramps a recovered or newly added backend over three seconds. The tests measure its share in three windows after it comes back: not too much in the first 1.5 s, not too little after, and a full share once the ramp is over.

## Further reading

- [The Power of Two Choices in Randomized Load Balancing (Mitzenmacher)](https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf) — the thesis behind picking the less loaded of two random backends, and why it beats picking one at random by so much.
- [Rendezvous hashing (Wikipedia)](https://en.wikipedia.org/wiki/Rendezvous_hashing) — highest-random-weight hashing, its history (Thaler and Ravishankar, 1996) and how it compares with hash rings.
- [Envoy: load balancing overview](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/overview) — a clear tour of balancing policies, health checking, outlier detection, panic thresholds and slow start in one modern proxy.
- [Google SRE book: Load Balancing in the Datacenter](https://sre.google/sre-book/load-balancing-datacenter/) — how a large operator handles unhealthy backends, subsetting and weighted round robin.
- [HAProxy configuration manual](https://docs.haproxy.org/2.8/configuration.html) — the authoritative reference for `balance`, `check`, `rise`/`fall` and `slowstart`.
