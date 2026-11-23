export default [
  {
    term: 'State',
    aliases: ['node state', 'local state'],
    summary: 'The data a node keeps in its own memory between one message and the next.',
    detail: `A node's **state** is everything it remembers across messages: the values it has seen, a counter, a clock, a table of request ids. Each node's state is private; other nodes learn about it only through messages, and therefore always see a possibly outdated copy.

In this lab, variables declared at the top level of your code are per-node state: every node runs its own copy of the program, so \`const seen = new Set()\` gives each node its own set. A message handler reads and updates that state and may send messages that describe it.

Two questions recur for every piece of state. Which nodes need a copy, and how do copies get back in agreement after they diverge (replication)? And what happens to it when the node crashes? In-memory state is lost; anything that must survive goes to durable storage, covered in later tracks.`,
    related: ['Sets', 'Gossip'],
  },
  {
    term: 'Sets',
    aliases: ['set', 'grow-only set', 'G-Set'],
    summary: 'An unordered collection with no duplicates; adding a value already present changes nothing, which makes duplicates harmless.',
    detail: `A **set** holds each value at most once, in no particular order. For a node that collects broadcast values it is the natural state, because the only question is "which values do I know?".

Its key property for distributed systems is that adding is **idempotent**: \`add(7)\` twice leaves the same set as \`add(7)\` once. Union is idempotent, commutative and associative too, so two nodes can exchange their sets in any order, any number of times, and both end up with the union. That is why a grow-only set is the simplest CRDT, and why retrying a gossip message is safe.

A practical detail in this lab: messages must be plain JSON, and a JavaScript \`Set\` is not. Send \`[...seen]\` and rebuild a set on the other side if needed.`,
    related: ['Idempotence', 'State', 'CRDT'],
  },
  {
    term: 'Gossip',
    aliases: ['gossip protocol', 'gossip protocols', 'epidemic protocol', 'epidemic broadcast'],
    summary: 'Nodes spread information peer to peer, each passing what it knows to others, until every node has it.',
    detail: `In a **gossip** (or epidemic) protocol there is no central distributor: each node that knows something passes it on to some of its peers, who pass it on in turn, the way a rumour spreads. Variants differ in whom a node tells (all neighbours, as in flooding, or a few random peers per round), when (immediately, or on a periodic timer), and what (the new item itself, or a summary that the peer compares with its own state).

Random-peer gossip spreads quickly: if each informed node tells one random peer per round, the number of informed nodes roughly doubles each round at first, so a cluster of 1,000 nodes is covered in a few dozen rounds, and no single node or link failure stops it. Combined with retries or periodic **anti-entropy**, gossip guarantees that every reachable node eventually receives every item, though not in any particular order or by a fixed deadline.

Real uses: Cassandra and Dynamo-style databases spread membership and node state by gossip; Consul and Serf disseminate events with a SWIM-based gossip protocol; Bitcoin floods transactions between peers.`,
    related: ['Flooding', 'Anti-entropy', 'At-least-once delivery', 'Eventual consistency'],
  },
  {
    term: 'Flooding',
    aliases: ['flood', 'flooding broadcast'],
    summary: 'Broadcast by having every node forward a new value to all its neighbours, exactly once, on first sight.',
    detail: `In **flooding**, a node that learns a value sends it to every neighbour. The rule that makes it terminate is deduplication: forward only the *first* time you learn a value. A node that receives a value it already has does nothing further, so each node forwards each value once.

Cost: in a full mesh of *n* nodes, one value causes up to *n* × (*n* − 1) messages (20 for five nodes, 72 for nine), plus acknowledgements if delivery is made reliable. Latency is one hop in a full mesh. Without the first-sight rule, every copy produces more copies and the network fills with a **message storm**.

Flooding is the deterministic, all-neighbours form of gossip. It is robust (any path will do) and simple, which is why it is used for small clusters and in peer-to-peer networks with modest node degree, and expensive, which is why large systems switch to random-peer gossip, batching or sparse topologies.`,
    related: ['Gossip', 'Deduplication', 'Message storm', 'Topology'],
  },
  {
    term: 'Acknowledgements',
    aliases: ['acknowledgement', 'acknowledgment', 'acknowledgments', 'ack', 'acks'],
    summary: 'A reply whose only job is to tell the sender that a message arrived, so the sender can stop resending it.',
    detail: `An **acknowledgement** (ack) is the receiver saying "I have this". It is the only way a sender can learn that a message was delivered: a plain send is fire-and-forget. With acks, the sender can apply a simple rule — resend until acknowledged — and turn a lossy link into **at-least-once delivery**.

Two rules make acks work. The receiver must act on the message (store it) *before* acknowledging, so that an ack really proves delivery. And the receiver must acknowledge *every* copy, including duplicates: a duplicate usually means the previous ack was lost, and staying silent makes the sender retry forever.

When messages are batched, the ack covers the batch: the sender removes exactly the items that were in the acknowledged batch from its pending list, and keeps anything queued after the batch was sent. In message queues the same idea decides when a message can be deleted: a consumer acks after processing, and an unacked message is redelivered.`,
    related: ['Retries', 'At-least-once delivery', 'Fire-and-forget'],
  },
  {
    term: 'At-least-once delivery',
    aliases: ['at-least-once', 'at least once'],
    summary: 'Resend until acknowledged: nothing is lost while the receiver is reachable, but a message may arrive more than once.',
    detail: `Under **at-least-once delivery** the sender keeps retrying a message until the receiver acknowledges it. If the network eventually lets messages through (lossy links, partitions that heal), every message is eventually delivered. It may be delivered more than once: when the ack, not the message, is lost, the sender cannot tell and resends.

Example: \`n1\` sends value 7 to \`n3\`; the message is lost. After 300 ms \`n1\` retries; \`n3\` stores 7 and acks, but the ack is lost. \`n1\` retries again; \`n3\` receives 7 a second time and acks; \`n1\` stops. \`n3\` saw two copies.

At-least-once is the standard choice for anything that must not be lost: message queues (SQS, RabbitMQ, Kafka consumers), replication streams, gossip. It is correct only when processing is **idempotent** or deduplicated, which together give the effect of exactly-once.`,
    related: ['Acknowledgements', 'Retries', 'Idempotence', 'Exactly-once processing', 'At-most-once delivery'],
  },
  {
    term: 'Batching',
    aliases: ['batch', 'batches', 'message batching'],
    summary: 'Sending many items in one message, trading a little latency for far fewer messages.',
    detail: `**Batching** collects items for a while and sends them together. A broadcast node can keep, per neighbour, the values that neighbour has not acknowledged and flush them in one message every 250 ms: if twelve values arrive in that interval, one message (and one ack) replaces twelve of each. Per-message overheads — headers, system calls, round trips, per-request CPU — are paid once per batch instead of once per item.

The cost is latency: an item waits up to one flush interval before it is sent, at every hop. Batching by size as well as time (flush when 100 items are queued or 10 ms have passed, whichever comes first) bounds both.

Two details matter for correctness. Acknowledge and remove exactly the items that were in the batch, not the whole pending list, since items may have been queued while the batch was in flight. And resend unacknowledged items in later batches; the receiver's idempotent handling absorbs duplicates. Kafka producers, database group commit and TCP's Nagle algorithm are all forms of batching.`,
    related: ['Latency vs. cost', 'Topology', 'Acknowledgements'],
  },
  {
    term: 'Topology',
    aliases: ['network topology', 'overlay topology', 'star topology'],
    summary: 'The choice of which nodes talk directly to which; it sets the trade-off between messages, hops and fault tolerance.',
    detail: `A protocol's **topology** is the graph of who sends to whom. In a **full mesh** every node talks to every other: one hop to anyone, the most redundancy, and about *n*² messages to spread a value. In a **star** every node talks only to a hub: at most two hops and about 2*n* messages, but the hub carries all traffic and while it is unreachable nothing spreads. A **ring** needs two links per node but up to *n*/2 hops. **Trees** give logarithmic depth with *n* − 1 links, but a failed inner node cuts off its subtree.

Whatever the shape, broadcast is only guaranteed to reach everyone if the graph is connected: every node reachable from every other through links that will eventually work.

Real systems mix shapes: random-peer gossip creates a different random graph each round, which is robust without being a full mesh; content-delivery networks and multicast trees use hierarchies; consensus protocols use a star around the current leader.`,
    related: ['Batching', 'Latency vs. cost', 'Flooding'],
  },
  {
    term: 'Latency vs. cost',
    aliases: ['latency versus cost', 'latency-cost trade-off', 'latency vs cost'],
    summary: 'Fewer, fuller messages and fewer links make a protocol cheaper but slower; every design picks a point between.',
    detail: `Most ways of cutting a protocol's message count make it slower. **Batching** saves messages by making items wait for the next flush. Sparser **topologies** save links by adding hops. Lazy schemes that announce an id first and send the data only on request save bandwidth at the cost of an extra round trip.

Example from the broadcast track: nine nodes, 50–100 ms links. Unbatched flooding in a full mesh delivers a value everywhere in one hop (about 100 ms) but costs about 128 node-to-node messages per value. A star that flushes batches every 250 ms costs under three messages per value and takes up to about 700 ms (wait, hop, wait, hop).

The right point depends on targets: how many messages the network can afford, and how stale a node may be. Good designs state both as budgets and choose the cheapest design that meets the latency target, without giving up the retries that make delivery reliable.`,
    related: ['Batching', 'Topology'],
  },
  {
    term: 'Message storm',
    aliases: ['broadcast storm'],
    summary: 'A runaway flood where every copy of a message triggers more copies, until the network is saturated.',
    detail: `A **message storm** happens when nodes forward messages without checking whether they have already handled them. With three nodes each forwarding every gossip to both peers, one value produces two copies, which produce four, then eight: the traffic grows without bound and never stops by itself.

The fix is deduplication: forward a value only the first time you see it (or, for versioned state, only when the version is newer than what you have). Then each node forwards each value at most once and the flood ends.

The same failure exists in real networks as the Ethernet *broadcast storm*, caused by a loop between switches; the Spanning Tree Protocol exists to prevent it by disabling redundant links. In the lab, the simulator detects runaway traffic and stops the run.`,
    related: ['Flooding', 'Deduplication'],
  },
  {
    term: 'Fire-and-forget',
    aliases: ['fire and forget', 'one-way message'],
    summary: 'Sending a message without waiting for or expecting any reply, so the sender never learns whether it arrived.',
    detail: `A **fire-and-forget** send hands a message to the network and moves on. It is cheap (one message, no waiting, no state) but gives **at-most-once delivery**: if the message is lost, nobody notices.

In this lab \`node.send(peer, body)\` is fire-and-forget, while \`node.rpc(peer, body, { timeout })\` waits for a reply and resolves with \`null\` if none comes in time. Turning the former into reliable delivery means adding acknowledgements and retries.

Fire-and-forget is the right tool when a lost message does not matter or will be superseded: periodic state pushes in anti-entropy (the next round repairs a loss), heartbeats, metrics.`,
    related: ['At-most-once delivery', 'Acknowledgements'],
  },
  {
    term: 'Deduplication',
    summary: 'Recognising that a message repeats one already handled, so its effect is not applied a second time.',
    detail: `In broadcast, deduplication has two jobs. A node forwards a value only the first time it sees it, which is what stops flooding from becoming a message storm. And a node's set ignores values it already holds, which makes retried gossip harmless. Here the value itself is the identity, so no separate request id is needed. See the Messaging track for the general technique.`,
    related: ['Flooding', 'Message storm', 'Idempotence'],
  },
  {
    term: 'Retries',
    summary: 'Sending a message again after a timeout, because the sender cannot tell whether the first attempt was lost.',
    detail: `A broadcast node retries each value to each peer until that peer acknowledges it. During a partition the retries keep failing; once the network heals, the next retry gets through. Retries create duplicates whenever only the acknowledgement was lost, which is safe because adding to a set is idempotent.`,
    related: ['Acknowledgements', 'At-least-once delivery'],
  },
  {
    term: 'Idempotence',
    summary: 'An operation is idempotent if doing it twice has exactly the same effect as doing it once.',
    detail: `Adding a value to a set is idempotent, and that single fact makes reliable broadcast simple: the sender may retry as often as it likes, and a receiver that gets the same value three times ends up in the same state as one that got it once. It must still acknowledge every copy.`,
    related: ['Sets', 'At-least-once delivery'],
  },
];
