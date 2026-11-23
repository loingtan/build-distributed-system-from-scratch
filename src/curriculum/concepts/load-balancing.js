export default [
  {
    term: 'Reverse proxy',
    aliases: ['load balancer'],
    summary: 'A server that receives clients\' requests on behalf of a pool of backends and forwards each one to a backend.',
    detail: `A **reverse proxy** stands in front of servers rather than clients. Clients connect to its address; it chooses a backend, forwards the request and relays the answer. Clients never learn which backend did the work.

That indirection is what makes a pool manageable: backends can be added, removed, upgraded or taken out when they fail, and clients notice nothing. A **load balancer** is a reverse proxy whose main job is choosing the backend.

NGINX, HAProxy, Envoy and cloud load balancers such as AWS's Application Load Balancer all work this way.`,
    related: ['Request relay', 'Round robin'],
  },
  {
    term: 'Request relay',
    aliases: ['request forwarding', 'proxying'],
    summary: 'Forwarding a client\'s request to a backend and returning that backend\'s answer to the same client.',
    detail: `A balancer **relays** each request: send it to the chosen backend, wait, and pass the answer back unchanged. With many requests in flight and backends answering in any order, the balancer must keep each answer paired with the client that asked.

In an event-driven balancer, a handler that awaits its own backend call keeps that pairing automatically. The relay also needs a timeout per forward, so a silent backend cannot hold a client forever, and it must respect pool changes: after the pool is updated, only current members get new requests.`,
    related: ['Reverse proxy'],
  },
  {
    term: 'Round robin',
    summary: 'Give backends turns in a fixed order: b1, b2, b3, b1, b2, b3, and so on.',
    detail: `**Round robin** keeps a counter and sends each request to \`pool[counter % pool.length]\`, then increments the counter. Over any stretch of requests each backend's count differs from the others' by at most one.

It needs no information about the backends, which is both its strength and its weakness: it sends a backend that has become fifteen times slower exactly the same share as the others. **Weighted round robin** gives some backends more turns, but the weights are set by hand.

Round robin is the default in NGINX and many other proxies.`,
    related: ['Least outstanding requests', 'Weighted routing'],
  },
  {
    term: 'Least outstanding requests',
    aliases: ['least connections', 'least request', 'least-outstanding', 'leastconn'],
    summary: 'Send each request to the backend with the fewest requests currently in flight.',
    detail: `The balancer counts, per backend, requests sent and not yet answered, and sends each new request to the backend with the lowest count. Ties are broken at random, or the first backend in the list wins every tie.

A slow backend holds each request longer, so its count stays high and it receives fewer new ones, with no latency measurement at all. If b3 takes 300 ms and the others 20 ms, b3's count is roughly fifteen times higher at the same arrival rate and it is nearly always passed over.

The count must be taken at send time (so a burst sees each earlier request) and released however the request ends, including on timeout. Its blind spot: a backend that fails fast looks idle and attracts traffic, which is why it needs health checks.`,
    related: ['Round robin', 'Tail latency', 'Adaptive routing', 'Power of two choices'],
  },
  {
    term: 'Tail latency',
    aliases: ['p99 latency', 'p99', 'latency percentile'],
    summary: 'The latency of the slowest requests, usually measured as the 95th or 99th percentile.',
    detail: `**Tail latency** describes the slow end of the response-time distribution. A p99 of 300 ms means 1 request in 100 takes 300 ms or longer.

Averages hide problems that the tail shows. If one of four backends becomes slow, the average rises modestly, but a quarter of requests now land on the slow one and the p95 jumps to that backend's latency.

Tails compound: a page that makes 100 backend calls waits for the slowest, so a p99 problem in a backend becomes a typical-case problem for the page.`,
    related: ['Least outstanding requests'],
  },
  {
    term: 'Adaptive routing',
    summary: 'Choosing where to send each request based on current conditions rather than a fixed rule.',
    detail: `An **adaptive** routing policy reacts as backends speed up, slow down or fail. Least outstanding requests is adaptive through its in-flight counts; latency-weighted policies (such as exponentially weighted moving averages of response time) and health-aware policies are others.

Fixed policies such as round robin or plain hashing make the same choice whatever happens. Adaptive ones need feedback, and can oscillate if the feedback is stale: if many balancers all see the same backend as idle, they may all pile onto it.`,
    related: ['Least outstanding requests', 'Power of two choices'],
  },
  {
    term: 'Power of two choices',
    aliases: ['p2c', 'two random choices'],
    summary: 'Pick two backends at random and send the request to the less loaded one.',
    detail: `With **power of two choices**, the balancer samples two backends at random and chooses the one with fewer outstanding requests. Mitzenmacher showed this reduces the worst-case load dramatically compared with one random choice, and nearly matches checking every backend.

It also avoids herding: when many independent balancers each pick "the least loaded backend" from stale data, they all pick the same one. Random sampling spreads them out. Envoy's least-request policy uses it by default.`,
    related: ['Least outstanding requests', 'Adaptive routing'],
  },
  {
    term: 'Session affinity',
    aliases: ['sticky sessions', 'sticky routing', 'sticky session'],
    summary: 'Sending every request for the same key, user or session to the same backend.',
    detail: `With **session affinity**, a request's key (a user id, a session cookie, a cache key) determines its backend, so repeat requests land where earlier ones did.

It pays off when backends keep per-key state in memory: a cache, a session, a WebSocket connection. Without it, each key is scattered over every backend and every cache misses often.

The costs: a hot key overloads one backend however busy it is, and changing the pool moves keys and their warm state. Consistent hashing keeps those moves to a minimum; cookie-based stickiness, as offered by cloud load balancers, keeps a mapping instead.`,
    related: ['Consistent hashing', 'Rendezvous hashing', 'Cache locality'],
  },
  {
    term: 'Consistent hashing',
        summary: 'Mapping keys to nodes so that adding or removing a node moves only about 1/N of the keys.',
    detail: `Plain \`hash(key) % N\` remaps almost every key when N changes: going from 5 to 4 backends moves about four keys in five. **Consistent hashing** is the family of schemes that move only the keys that must move.

The classic form places nodes at many points on a ring of hash values; a key belongs to the first node clockwise from its own hash. Removing a node hands its arcs to neighbours; adding one takes arcs from them. Rendezvous hashing achieves the same with per-key scores.

Dynamo, Cassandra and Riak partition data this way, and proxies such as NGINX (\`hash ... consistent\`) and HAProxy use it for sticky routing.`,
    related: ['Rendezvous hashing', 'Session affinity'],
  },
  {
    term: 'Rendezvous hashing',
    aliases: ['highest random weight', 'HRW hashing', 'HRW'],
    summary: 'For each key, score every node with hash(key, node) and pick the highest score.',
    detail: `**Rendezvous hashing** computes \`score = hash(key + "/" + nodeId)\` for every node and sends the key to the node with the highest score. The result depends only on the key and the set of nodes, never on their order in a list.

If the winner for user:1042 is b5 and b3 leaves, b5 still has the highest score, so the key stays. Only keys b3 was winning move, each to its second-highest scorer. A new node b6 takes exactly the keys where it outscores everyone, about 1/N of them.

It costs one hash per node per lookup, which is fine for tens of nodes; with thousands, a ring or a hierarchical variant is cheaper.`,
    related: ['Consistent hashing', 'Session affinity'],
  },
  {
    term: 'Cache locality',
    summary: 'Routing requests so they land where the data they need is already cached.',
    detail: `A backend that has recently served a key often has it in memory and answers in a few milliseconds; elsewhere the same request is a miss that goes to the database. **Cache locality** is the property that requests for a key keep hitting the same warm cache.

Spreading requests purely by load destroys it: with five backends, each key's requests are split five ways and each backend's cache holds a fifth of the hot set per key. Sticky routing restores it, at the cost of less even load.`,
    related: ['Session affinity', 'Consistent hashing'],
  },
  {
    term: 'Active health checks',
    aliases: ['health checks', 'health check', 'health probe', 'active health checking'],
    summary: 'The balancer regularly probes each backend and stops routing to those that fail the probe.',
    detail: `With **active health checks**, the balancer sends each backend a small request on a schedule, for example \`{ "type": "health" }\` every 200 ms with a 150 ms timeout. After a number of failures in a row (say 2) the backend is taken out of rotation; after a number of successes (say 1) it is put back.

The balancer keeps probing backends it has removed, since that is the only way they can return. The time to detect a failure is bounded by roughly failures × interval + timeout.

A probe tests the probe path, not every real request; a backend can pass it and still fail traffic. **Passive** checks, which watch real responses (Envoy's outlier detection, NGINX's \`max_fails\`), complement it.`,
    related: ['Failover', 'Black-hole backends', 'Retries'],
  },
  {
    term: 'Failover',
    summary: 'Moving traffic away from a failed component to healthy ones.',
    detail: `**Failover** is what happens when a balancer stops sending requests to a backend it believes has failed and spreads them over the others. In an active–passive setup, it means promoting a standby.

Failover is only as fast as failure detection. Between the failure and its detection, some requests still go to the failed backend; retries on another backend cover that window. Failing back, returning traffic once the backend recovers, is the reverse, and often benefits from slow start.`,
    related: ['Active health checks', 'Retries', 'Slow start'],
  },
  {
    term: 'Retries',
    aliases: ['retry'],
    summary: 'Sending a failed request again, ideally to a different backend, in the hope that the failure was transient.',
    detail: `A **retry** repeats a request that failed or timed out. Behind a load balancer, retrying on a *different* backend hides a single backend's failure from the client.

Retries add load exactly when capacity is short, so they need limits: one retry per request at a balancer, a total cap and growing, randomised waits when retrying the same service. Only idempotent requests, or ones the server deduplicates, are safe to retry blindly.`,
    related: ['Failover', 'Active health checks'],
  },
  {
    term: 'Black-hole backends',
    aliases: ['black hole', 'black-hole backend', 'blackhole'],
    summary: 'A backend that accepts requests and never answers, so every request sent to it waits for a timeout.',
    detail: `A **black-hole** backend swallows traffic silently: the process is hung, the machine is gone or packets are dropped. Nothing comes back, so each request sent there costs a full timeout.

The opposite is a backend that fails fast with errors. Both are dangerous in different ways: a black hole adds latency to every request routed to it, while a fast-failing backend looks idle to least-outstanding routing and attracts most of the traffic. Health checks catch both; per-request timeouts and retries limit the damage until they do.`,
    related: ['Active health checks', 'Failover'],
  },
  {
    term: 'Slow start',
    aliases: ['slow-start'],
    summary: 'Ramping a newly added or recovered backend from a small share of traffic up to a full share over time.',
    detail: `A backend that just started has cold caches and unoptimised code; a full share of traffic at once can knock it over again. **Slow start** gives it a weight that grows from 0 to 1 over a warm-up period, for example linearly over 3 seconds, and routes it that fraction of a normal share.

With 4 backends of which one has weight 0.25, it receives 0.25 / (3 + 0.25), about 8% of requests, instead of 25%.

Least-outstanding routing needs this most, because an idle fresh backend looks like the best choice. HAProxy (\`slowstart\`), Envoy, NGINX Plus and AWS ALB all offer it. (TCP also has a "slow start" for its congestion window; the idea is similar but separate.)`,
    related: ['Warm-up', 'Weighted routing', 'Failover'],
  },
  {
    term: 'Warm-up',
    aliases: ['warmup', 'cold start'],
    summary: 'The period after a process starts during which it is slower than normal: empty caches, unoptimised code, new connections.',
    detail: `A freshly started server is **cold**. Its in-memory caches are empty, a JIT compiler has not yet optimised hot paths, connection pools are empty and the operating system's page cache does not hold its files. The first requests can cost several times more than later ones.

The **warm-up** period ends when those costs settle. Load balancers handle it with slow start; some services also replay recorded traffic or preload caches before they declare themselves healthy.`,
    related: ['Slow start'],
  },
  {
    term: 'Weighted routing',
    aliases: ['weighted load balancing', 'weighted round robin', 'weights'],
    summary: 'Giving each backend a share of traffic proportional to a weight.',
    detail: `With **weighted routing**, a backend of weight 2 gets twice the traffic of one with weight 1. Weights can reflect machine size, a canary percentage, or a warm-up ramp.

A backend with weight w among n − 1 others of weight 1 receives w / (n − 1 + w) of requests. Weighted round robin and weighted random selection implement fixed weights; combining weights with least-outstanding routing needs care, since an idle backend wins on outstanding count regardless of its weight.`,
    related: ['Slow start', 'Round robin'],
  },
];
