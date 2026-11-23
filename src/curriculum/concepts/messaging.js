export default [
  {
    term: 'Messages',
    aliases: ['message', 'message passing'],
    summary: 'The only way nodes in a distributed system affect each other: self-contained packets sent over a network that may lose them.',
    detail: `A **message** is a unit of data one process sends to another. In a distributed system it is the *only* channel between nodes: no node can read another's memory, so everything a node knows about the rest of the system arrived as a message.

In this lab a message is a JSON envelope: \`{ "src": "c1", "dest": "n1", "body": { "type": "add", "delta": 5, "msg_id": 7 } }\`. The \`type\` field selects the handler that runs on the receiving node.

The network delivers a message whole or not at all, but it promises nothing else: a message may be lost, delayed by an unbounded amount, duplicated by a retry, or overtaken by a later one. Every protocol in this curriculum is a way of getting reliable behaviour out of that unreliable channel.`,
    related: ['Request / reply', 'Timeout', 'At-least-once delivery'],
  },
  {
    term: 'Request / reply',
    aliases: ['request-response', 'request reply', 'RPC'],
    summary: 'A pair of messages where one side asks and the other answers, matched by an id the asker chose.',
    detail: `In a **request / reply** exchange the requester sends a message carrying a \`msg_id\` that is unique among its own requests, and the responder answers with a message whose \`in_reply_to\` holds that same number. The requester keeps a table of outstanding ids and uses \`in_reply_to\` to route each answer to whoever is waiting for it, even when many requests are in flight and replies come back out of order.

Example: \`c1 → n1 { "type": "echo", "echo": "hi", "msg_id": 1 }\`, then \`n1 → c1 { "type": "echo_ok", "echo": "hi", "in_reply_to": 1 }\`. A reply sent without \`in_reply_to\` matches nothing, so the requester times out even though the responder did its job.

A remote procedure call (RPC) is request / reply wrapped to look like a function call. The wrapping hides an important difference: a local call either returns or throws, while a remote call can also end in a **timeout**, where the caller does not know whether the work was done.`,
    related: ['Messages', 'Timeout', 'Retries'],
  },
  {
    term: 'Idempotence',
    aliases: ['idempotency', 'idempotent', 'idempotent operation'],
    summary: 'An operation is idempotent if doing it twice has exactly the same effect as doing it once.',
    detail: `An operation is **idempotent** when repeating it changes nothing beyond the first application. "Set x to 5", "delete order 42" and "add 7 to this set" are idempotent. "Add 5 to x", "append to the log" and "charge the card" are not: each repeat has a further effect.

Idempotence is what makes **retries** safe. A client that times out cannot tell whether its request was applied, so it sends it again; if the operation is idempotent, a duplicate is harmless. This is why at-least-once delivery combined with idempotent processing gives the effect of exactly-once.

A non-idempotent operation can be made idempotent by giving each logical request a unique id and having the receiver remember which ids it has applied (**deduplication**). Some operations are idempotent by design: merging a CRDT state, or taking the union of two sets, can be repeated any number of times. HTTP defines GET, PUT and DELETE as idempotent and POST as not, which is why clients may retry the former automatically.`,
    related: ['Deduplication', 'Retries', 'Idempotency key', 'Exactly-once processing'],
  },
  {
    term: 'Retries',
    aliases: ['retry', 'retrying'],
    summary: 'Sending a request again after a timeout, because the sender cannot tell whether the first attempt was lost.',
    detail: `A **retry** resends a request whose reply did not arrive in time. Retries are the basic tool for surviving message loss: if the request was lost, the retry may get through; if the server was briefly unreachable, a later retry finds it again.

Retrying turns at-most-once delivery into **at-least-once delivery**, and that has a cost. When the reply was lost rather than the request, or the server was merely slow, the retry delivers a second copy of a request that was already applied. Retries are therefore only safe for **idempotent** operations, or with **deduplication** on the receiving side. A retry must reuse the original request's id; a fresh id makes the duplicate look like a new request.

Retries also add load at the worst moment: when a server is overloaded, clients time out and retry, adding more work. Production clients space retries out with exponential backoff and random jitter, and cap them with a retry budget; the resilience track covers these.`,
    related: ['Timeout', 'Idempotence', 'Deduplication', 'At-least-once delivery'],
  },
  {
    term: 'Deduplication',
    aliases: ['dedup', 'dedupe', 'duplicate detection', 'duplicate suppression'],
    summary: 'Recognising that a message repeats one already handled, so its effect is not applied a second time.',
    detail: `**Deduplication** stops repeated copies of a message from having repeated effects. The receiver needs some identity to compare: a request id chosen by the client, a per-sender sequence number, or (when the content *is* the identity, as with values in a set) the value itself.

The basic form keeps a set of applied ids. On each request: if the id is new, record it and apply the effect; if it is known, skip the effect. Either way, reply, because the sender is retrying precisely since it never saw an answer. In flooding broadcast the same idea stops a gossip message circulating forever: a node forwards a value only the first time it sees it.

The cost is memory. Systems bound it with a retention window (Stripe forgets idempotency keys after about 24 hours; SQS FIFO deduplicates over five minutes) or with per-sender sequence numbers, where remembering the highest number per sender is enough (Kafka's idempotent producer). The id table must survive a crash together with the state it protects, or retries after a restart are applied twice.`,
    related: ['Idempotence', 'Idempotency key', 'Retries'],
  },
  {
    term: 'Timeout',
    aliases: ['timeouts'],
    summary: 'A deadline after which a node stops waiting for a reply and treats the silence as a possible failure.',
    detail: `A **timeout** is how a node turns "no reply yet" into a decision. In an asynchronous network there is no upper bound on delay, so silence is ambiguous: when a timeout fires, the request may have been lost, the remote node may be slow or crashed, or the work may have been done and only the reply lost. A timeout tells you that you do not know, never which of these happened.

Choosing the value is a trade-off. If replies normally take 5 ms but occasionally 300 ms, a 50 ms timeout reacts fast to real failures but declares many healthy requests failed and triggers needless retries; a 2 s timeout rarely misfires but leaves callers stuck for two seconds on every real loss. Systems usually set timeouts from observed latency (for example a high percentile plus margin) and keep retry behaviour idempotent so that misfires are harmless.

The same mechanism underlies failure detection: a node that has not sent a heartbeat for several intervals is *suspected* to be down, but can never be known to be down. Leader election, two-phase commit and circuit breakers all depend on timeouts and must be correct even when a timeout is wrong.`,
    related: ['Retries', 'Request / reply', 'At-least-once delivery'],
  },
  {
    term: 'At-most-once delivery',
    aliases: ['at-most-once'],
    summary: 'Send each message once and never retry: nothing is applied twice, but messages can be lost.',
    detail: `Under **at-most-once delivery** a sender transmits each message once and gives up if it hears nothing. Each message takes effect zero or one times. There are no duplicates to handle, which keeps the receiver simple, but every lost request or crash silently drops work.

It is the right choice when a lost message is cheap and a duplicate is expensive or meaningless: periodic metrics samples, position updates in a game (the next one supersedes it), or best-effort notifications. A plain \`node.send\` in this lab is at-most-once.

Compare **at-least-once delivery**, which retries until acknowledged and therefore must cope with duplicates.`,
    related: ['At-least-once delivery', 'Exactly-once processing', 'Retries'],
  },
  {
    term: 'Exactly-once processing',
    aliases: ['exactly-once', 'exactly once', 'effectively-once', 'exactly-once delivery'],
    summary: 'Each request takes effect once, achieved by at-least-once delivery plus idempotent or deduplicated processing.',
    detail: `Users want every request to take effect **exactly once**. The network cannot provide that: a sender who hears nothing cannot tell a lost request from a lost reply, so it must either risk losing the request (at-most-once) or risk sending it twice (at-least-once).

What systems build instead is **exactly-once processing** (sometimes called effectively-once): deliver at least once, and make the receiver apply each logical request once, either because the operation is idempotent or because the receiver deduplicates by request id. The message may arrive three times; its effect happens once.

Kafka's "exactly-once semantics", Stripe's idempotency keys and client sessions in a replicated key-value store are all this pattern. Each depends on the deduplication record being stored atomically with the effect: if the effect survives a crash but the record of having applied it does not, the next retry applies it again.`,
    related: ['At-least-once delivery', 'Idempotence', 'Deduplication'],
  },
  {
    term: 'Idempotency key',
    aliases: ['request id', 'request_id'],
    summary: 'A unique id the client attaches to a request and reuses on every retry, so the server can recognise duplicates.',
    detail: `An **idempotency key** (or request id) names one logical operation. The client generates it once, when it decides to perform the operation, and sends it with the first attempt and with every retry. The server records the key together with the effect (and, if the reply carries data, the original response); a later request with the same key gets the stored response and has no further effect.

Example: a client sends \`{ "type": "add", "delta": 5, "request_id": "c1-12" }\`, times out, and resends the identical message. The server sees \`c1-12\` in its table, skips the add, and replies \`add_ok\` again.

Keys must be unique across clients (a client-name prefix or a random UUID does it) and must be kept at least as long as clients may retry. Generating a new key per attempt defeats the whole scheme.`,
    related: ['Idempotence', 'Deduplication', 'Retries'],
  },
];
