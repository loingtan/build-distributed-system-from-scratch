## The problem

A client, `c1`, wants to add 5 to a counter held by node `n1`. It sends `{"type": "add", "delta": 5}`. The counter goes from 100 to 105 and `n1` sends back `{"type": "add_ok"}`. On the way back, the reply is dropped: a switch was busy, a connection was reset, a buffer overflowed. From `c1`'s side, nothing happened. It waited 500 ms and heard nothing.

What should `c1` do? If it gives up, it reports a failure to its user for an operation that actually succeeded. If it sends the request again, `n1` adds 5 a second time and the counter reads 110. Neither choice is right, and `c1` has no way to find out which situation it is in, because the only evidence it could use is the reply that went missing.

This is the smallest real problem in distributed systems, and almost every later problem is a version of it. Two machines that share nothing but a network cannot know what the other one did unless a message says so, and any message can be lost. This track builds the two moves that everything else rests on: answering a request so the sender can recognise the answer, and surviving a sender that asks twice.

## The core idea

### Nodes and messages

A **node** is a process with its own memory that no other process can read. The only way nodes (and clients) influence each other is by sending **messages**: small, self-contained packets of data delivered by the network. In this lab every message is JSON and has the same shape:

```json
{ "src": "c1", "dest": "n1", "body": { "type": "add", "delta": 5, "msg_id": 7 } }
```

`src` and `dest` say who sent it and who should get it. The `body` is the payload, and its `type` field says what kind of message it is, which is how a node chooses which handler to run. This envelope is the same idea as the Maelstrom protocol from the Jepsen project, which this lab is modelled on.

A message is either delivered whole or not at all. What the network does *not* promise is that it is delivered at all, that it arrives quickly, or that two messages arrive in the order they were sent. In the lab you will see all three failures.

### Request and reply

Most interactions are a **request / reply** pair: one side asks, the other answers. Because many requests can be in flight at once, the asker needs to know which answer belongs to which question. The convention is that every request carries a `msg_id` unique to its sender, and the reply carries the same number in `in_reply_to`:

```json
c1 → n1   { "type": "echo", "echo": "hello 1", "msg_id": 1 }
n1 → c1   { "type": "echo_ok", "echo": "hello 1", "in_reply_to": 1 }
```

The client keeps a table of outstanding `msg_id`s. When a message arrives with `in_reply_to: 1`, it looks up request 1 and hands the reply to whoever was waiting for it. A reply without `in_reply_to` is just another message to the client; it matches nothing, so the waiting caller eventually gives up. That is why the lab provides `node.reply(msg, body)`, which fills in `dest` and `in_reply_to` for you.

### Timeouts and the three indistinguishable failures

A client cannot wait forever, so it sets a **timeout**: if no reply has arrived after some fixed time, it stops waiting and treats the request as failed. The timeout is the only failure signal the client gets, and it is an ambiguous one. When it fires, any of these may be true:

1. The request was lost. The server never saw it and did nothing.
2. The server is slow, or the network is slow. The request will still be applied, perhaps after the client has given up.
3. The server applied the request and the reply was lost.

<figure>
<svg viewBox="0 0 600 270" role="img" aria-label="Three runs between c1 and n1 that look identical to the client: request lost, server slow, reply lost">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">c1</text><text x="10" y="104">n1</text>
    <text x="210" y="44">c1</text><text x="210" y="104">n1</text>
    <text x="410" y="44">c1</text><text x="410" y="104">n1</text>
  </g>
  <g style="stroke: var(--line)">
    <line x1="40" y1="40" x2="190" y2="40"/><line x1="40" y1="100" x2="190" y2="100"/>
    <line x1="240" y1="40" x2="390" y2="40"/><line x1="240" y1="100" x2="390" y2="100"/>
    <line x1="440" y1="40" x2="590" y2="40"/><line x1="440" y1="100" x2="590" y2="100"/>
  </g>
  <line x1="50" y1="40" x2="95" y2="80" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="92" y="90" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <line x1="250" y1="40" x2="290" y2="100" style="stroke: var(--accent); stroke-width: 2"/>
  <rect x="290" y="94" width="70" height="12" style="fill: var(--warn); stroke: none"/>
  <line x1="360" y1="100" x2="388" y2="66" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="450" y1="40" x2="490" y2="100" style="stroke: var(--accent); stroke-width: 2"/>
  <circle cx="492" cy="100" r="5" style="fill: var(--good)"/>
  <line x1="495" y1="100" x2="540" y2="60" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="537" y="58" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <g style="stroke: var(--muted); stroke-dasharray: 2 3">
    <line x1="170" y1="20" x2="170" y2="120"/><line x1="370" y1="20" x2="370" y2="120"/><line x1="570" y1="20" x2="570" y2="120"/>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="140" y="16">timeout</text><text x="340" y="16">timeout</text><text x="540" y="16">timeout</text>
  </g>
  <g style="font: 13px var(--sans); fill: var(--ink)">
    <text x="40" y="150">1. request lost</text><text x="240" y="150">2. server slow</text><text x="440" y="150">3. reply lost</text>
    <text x="40" y="170" style="fill: var(--muted)">counter unchanged</text>
    <text x="240" y="170" style="fill: var(--muted)">applied, after timeout</text>
    <text x="440" y="170" style="fill: var(--muted)">applied, client unaware</text>
  </g>
  <text x="40" y="220" style="font: 13px var(--sans); fill: var(--ink)">What c1 sees in all three cases: a request went out, and nothing came back before its timer fired.</text>
  <text x="40" y="242" style="font: 13px var(--sans); fill: var(--muted)">Time flows left to right. A green dot marks the moment n1 applies the add.</text>
</svg>
<figcaption>Three different histories, one observation. A timeout tells the client that it does not know, never what happened.</figcaption>
</figure>

No choice of timeout removes the ambiguity. A longer one turns some case-2 runs into successes but makes the client slower to react to real failures; it does nothing for cases 1 and 3.

### Delivery guarantees

Given that ambiguity, a sender has two basic policies.

- **At-most-once delivery**: send once and never retry. Each request is applied zero or one times. Nothing is applied twice, but requests can silently vanish (case 1).
- **At-least-once delivery**: retry until a reply arrives. Each request is eventually applied one or more times, as long as the server is reachable at some point. Nothing vanishes, but requests can be applied twice (case 3, or case 2 with a retry that overtakes the slow original).

People often ask for **exactly-once delivery**: every request applied once and only once. No network can deliver that by itself, because the sender cannot distinguish case 1 from case 3. What systems actually build is at-least-once delivery plus a receiver that makes the second application harmless. The effect is exactly-once *processing*, even though the message itself may arrive several times.

### Idempotence

An operation is **idempotent** if performing it twice has the same effect as performing it once. "Set the counter to 105" is idempotent: do it again and the counter is still 105. "Add 5 to the counter" is not: every repeat changes the result. Reading is idempotent. Deleting a specific record by its id is idempotent (the second delete finds nothing to delete). Appending to a list, charging a card and sending an email are not.

If every operation were idempotent, **retries** (sending the same request again after a timeout) would be free of risk and the problem above would disappear. Some operations can be redesigned that way: instead of "add 5", the client could send "set to 105". But that only works when the client knows the current value and nobody else is changing it, which in a shared counter is rarely true. We need a way to make *any* operation safe to retry.

### Deduplication with request ids

The trick is to give each logical request an identity that survives retries. The client picks a unique **request id** once, when it first decides to perform the operation, and puts the same id in every copy it sends:

```json
{ "type": "add", "delta": 5, "request_id": "req-12" }
```

The server keeps a record of request ids it has already applied. On every `add`:

```text
on add(request_id, delta):
    if request_id not in applied:
        applied.add(request_id)
        total = total + delta
    reply add_ok            -- in both cases
```

This is **deduplication**: recognising that a message is a repeat of one already handled, and not acting on it again. The pair "check the id, apply the effect" turns a non-idempotent operation into an idempotent one. The first copy changes the state; every later copy finds its id in the table and changes nothing.

The last line matters as much as the check. A duplicate exists *because* the client has not seen an answer. If the server drops the duplicate without replying, the client times out again and retries again, forever. Skip the work, never the answer.

<figure>
<svg viewBox="0 0 600 230" role="img" aria-label="c1 sends req-12; n1 applies it but the reply is lost; c1 retries with the same id; n1 finds it in its table, skips the add, and replies">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">c1</text><text x="10" y="144">n1</text>
  </g>
  <g style="stroke: var(--line)"><line x1="40" y1="40" x2="590" y2="40"/><line x1="40" y1="140" x2="590" y2="140"/></g>
  <line x1="60" y1="40" x2="130" y2="140" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="58" y="30" style="font: 12px var(--mono); fill: var(--ink)">add 5, req-12</text>
  <circle cx="132" cy="140" r="5" style="fill: var(--good)"/>
  <line x1="136" y1="140" x2="200" y2="80" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="198" y="78" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <line x1="300" y1="40" x2="370" y2="140" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="280" y="30" style="font: 12px var(--mono); fill: var(--ink)">retry: add 5, req-12</text>
  <circle cx="372" cy="140" r="5" style="fill: var(--surface); stroke: var(--muted); stroke-width: 2"/>
  <line x1="376" y1="140" x2="446" y2="40" style="stroke: var(--good); stroke-width: 2"/>
  <text x="440" y="30" style="font: 12px var(--mono); fill: var(--ink)">add_ok</text>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="90" y="166">new id: total 100 → 105</text>
    <text x="90" y="182">applied = {req-12}</text>
    <text x="330" y="166">seen before: total stays 105</text>
    <text x="330" y="182">reply anyway</text>
  </g>
  <text x="40" y="216" style="font: 13px var(--sans); fill: var(--ink)">Filled dot: the add is applied. Hollow dot: the copy is recognised and skipped.</text>
</svg>
<figcaption>The retry carries the same request id as the original, so the server can recognise it, leave the state alone and still acknowledge.</figcaption>
</figure>

> At-least-once delivery plus idempotent processing is the only practical way to get the effect of exactly-once. The sender guarantees nothing is lost; the receiver guarantees nothing counts twice.

## A worked example

One client `c1` and one node `n1`. The counter starts at 0 and `applied` is empty. Network latency is 20–80 ms per message, and `c1` retries a request if it has no reply after 60 ms. Times are in milliseconds.

| t | Event | `applied` on n1 | total | Notes |
|---|---|---|---|---|
| 0 | `c1 → n1` add 4, `req-1` | | 0 | |
| 25 | n1 receives `req-1` | {req-1} | 4 | new id, apply |
| 25 | `n1 → c1` add_ok (in_reply_to 1) | | | |
| 48 | c1 receives add_ok for `req-1` | | | done |
| 100 | `c1 → n1` add 7, `req-2` | | | |
| 160 | **no reply yet**; c1 times out and resends `req-2` | | | the original is still in flight |
| 178 | n1 receives the original `req-2` | {req-1, req-2} | 11 | new id, apply |
| 178 | `n1 → c1` add_ok | | | **lost** |
| 195 | n1 receives the retry of `req-2` | unchanged | 11 | seen, skip, reply anyway |
| 195 | `n1 → c1` add_ok | | | |
| 230 | c1 receives add_ok for `req-2` | | | done |
| 300 | `c1 → n1` add 2, `req-3` | | | **lost** |
| 360 | c1 times out and resends `req-3` | | | |
| 390 | n1 receives the retry of `req-3` | {req-1, req-2, req-3} | 13 | first copy n1 has seen, apply |
| 420 | c1 receives add_ok for `req-3` | | | done |
| 500 | `c1 → n1` read | | | |
| 540 | c1 receives read_ok, value 13 | | | 4 + 7 + 2 |

Three things happened here that a naive counter gets wrong. At t = 160 the client retried a request that was merely slow, so two copies of `req-2` were in flight at once; the table made the second one harmless. At t = 178 a reply was lost, which is case 3 from the diagram; the retry was already on its way and got its own acknowledgement. At t = 300 the request itself was lost, which is case 1; the retry was the first copy the server saw, so it was applied. In every case the client did the same thing (retry with the same id), and the server did the same thing (apply if new, always reply). Neither had to know which failure occurred.

A counter without deduplication would have finished at 20: the duplicate `req-2` adds 7 more. The test for this task makes about half of all requests arrive two or three times, so the gap between right and wrong is large and obvious.

## Why it is correct

The guarantee is: **the counter equals the sum of `delta` over the distinct request ids that have reached the node, and every copy of every request that reaches the node gets an `add_ok`.**

It holds because of an invariant maintained by every handler run: *`total` equals the sum of the deltas of exactly the ids in `applied`*. It is true at the start (empty set, total 0). A copy with a new id adds the id and its delta together, so the invariant still holds. A copy with a known id changes neither. Nothing else modifies either variable. So after any sequence of copies, in any order, with any number of repeats, `total` counts each id once.

The argument relies on four assumptions, and each one is a way the scheme can fail in practice.

1. **One id per logical operation.** The client must choose the id when it decides to perform the operation, not when it sends a message. If it generates a fresh id for each retry, the server sees two different requests and applies both.
2. **The check and the update are atomic.** Between "is this id in the table?" and "add it", no other copy of the same request may run. In this lab each handler runs to completion before the next message is processed, so this holds automatically. In a multi-threaded server you need a lock, or a single atomic "insert if absent" operation.
3. **The table outlives the retries.** If the server forgets an id while a client may still retry it, a late retry is applied again.
4. **The table and the state live and die together.** If the server crashes and restarts with the counter restored from disk but an empty `applied` set, every retry after the restart is applied a second time. The id must be recorded in the same durable write as the effect, or not at all.

Correctness also requires that each id is unique across clients. Prefixing the id with the client's own name (`c1-12`) or using a random 128-bit value is enough.

## Trade-offs and failure modes

**Memory.** The `applied` set grows with every request ever made. Real servers bound it in one of two ways. Some forget ids after a fixed **retention window**, chosen to be longer than any client will keep retrying; this is safe exactly as long as clients actually stop retrying before the window ends. Others use **per-client sequence numbers**: each client numbers its requests 1, 2, 3 and sends one at a time, so the server only needs to remember the highest number seen per client. Anything at or below it is a duplicate. That is one integer per client instead of one entry per request, but it requires clients to have stable identities and to not pipeline requests out of order.

**Returning the original result.** For "add", the reply carries no data, so replying `add_ok` to a duplicate is enough. For operations whose reply matters (a "create order" that returns the new order's number), the server must store the original response next to the id and return that stored response to every duplicate. Recomputing it might produce a different answer.

**Concurrent duplicates.** Sometimes the retry arrives while the original is still being processed, for example because the handler is waiting on a database. A server that only records the id after finishing will start the work twice. The fix is to record "in progress" before starting, and make duplicates wait for or report on the first attempt.

**More than one server.** If the client retries against a different node (because the first one seemed down), that node has never seen the id. Deduplication then needs a table that all nodes share, which turns a local trick into a replication problem: later tracks on logs and consensus solve exactly that.

**What it does not protect against.** Deduplication catches repeats of the *same* request. It does nothing if the user clicks "pay" twice and the client generates two ids; that is a product decision, solved by having the id identify the user's intention (the cart, the invoice) rather than the click. It also does not order requests: two different requests can still arrive in either order.

**Latency.** Retries recover from loss, but each lost message costs a full timeout. A short timeout reacts quickly but sends more needless duplicates when the server is merely slow; a long one wastes time on every real loss. The resilience track goes further into timeouts, backoff and retry budgets.

## In real systems

**Stripe.** Stripe's API accepts an `Idempotency-Key` header on POST requests. The client generates a unique key, typically a UUID, and sends it with every retry of the same request. Stripe stores the key with the status code and body of the first response; later requests with the same key get that saved response instead of performing the operation again. Stripe documents that keys may be removed after they are at least 24 hours old, which is the retention window described above.

**Apache Kafka.** Kafka's idempotent producer (enabled by default since Kafka 3.0) assigns each producer a producer id, and numbers the batches it sends to each partition with a sequence number. The broker remembers the latest sequence numbers per producer and partition, and discards a batch it has already written. This is the per-client sequence number design: it lets producers retry after a network error without creating duplicate records in the log.

**Amazon SQS FIFO queues.** A FIFO queue deduplicates messages by a `MessageDeduplicationId` (or a hash of the message body, if content-based deduplication is enabled). A second message with the same id sent within a five-minute interval is accepted but not delivered again. The five minutes is the retention window, made explicit.

**HTTP.** The HTTP specification (RFC 9110) defines GET, PUT and DELETE as idempotent methods and POST as not idempotent. That is why browsers and proxies may retry a GET automatically but ask before resubmitting a form, and why APIs such as Stripe's need an explicit key to make POST safe to retry.

## Common misconceptions

- **"TCP already gives me exactly-once."** TCP removes duplicates and reorders bytes *within one connection*. If the connection breaks after the server acted but before the reply arrived, the client must open a new connection and send again, and TCP knows nothing about the earlier attempt. Application-level retries are still at-least-once.
- **"A timeout means the request failed."** It means the client does not know. The request may have been applied, may be applied later, or may never be applied. Code that rolls back or reports failure on a timeout is guessing.
- **"Deduplication means dropping duplicates."** It means not *applying* them twice. The duplicate still deserves the same answer as the original; ignoring it makes the client retry forever.
- **"I can detect duplicates by comparing message contents."** Two separate requests to add 5 look identical, and both should count. Only an id chosen by the client for each logical operation tells a retry apart from a new request.
- **"Exactly-once delivery is impossible, so exactly-once effects are too."** The message may arrive several times, but its effect can still happen once, by combining retries with idempotent processing. That is what real "exactly-once" features provide.

## How the tasks build it

[MS-1 Echo](#/c/echo) is the request / reply contract on its own: receive a message, answer it so that the client can match the answer to its question. Its tests send requests to one node and then to three nodes running the same code, and check that every request gets a reply of the right type carrying the original payload. The point is to learn the message shape and the difference between sending a message and replying to one.

[MS-2 Idempotent requests](#/c/idempotent-requests) adds the retrying client. About half of all `add` requests arrive two or three times with the same `request_id`, and in the second test the links are slow enough (20–80 ms) that retries overlap the originals. The tests check that every copy of every request is acknowledged, and that the final read counts each request id exactly once. Both halves of the rule are tested: apply once, answer always.

## Further reading

- [Maelstrom protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md) — the message envelope (`src`, `dest`, `body`, `msg_id`, `in_reply_to`) that this lab copies, with the reasoning behind each field.
- [Stripe: Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency) — a production account of idempotency keys, retries with backoff, and what the server stores per key.
- [RFC 9110, section 9.2.2: Idempotent methods](https://www.rfc-editor.org/rfc/rfc9110#section-9.2.2) — the definition of idempotence that the web runs on, and why it permits automatic retries.
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) — the Kafka design proposal for producer ids and sequence numbers, a clear case study of deduplication at scale.
- [Amazon SQS: exactly-once processing in FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html) — how a managed queue exposes deduplication ids and a fixed deduplication window.
