export default [
  {
    term: 'Logical time',
    aliases: ['logical clocks', 'logical clock', 'logical timestamps'],
    summary: 'Ordering events with counters that nodes maintain themselves, instead of reading physical clocks that disagree.',
    detail: `**Logical time** numbers events using counters kept by the nodes and carried on messages. The numbers measure nothing in seconds; they encode only order, and they are built so that this order never contradicts causality: if one event could have influenced another, it gets the smaller timestamp.

It exists because physical clocks cannot be trusted for ordering across machines. With 50 ms of clock skew, a reply sent 30 ms after receiving a message can be stamped earlier than the message itself. A logical clock cannot make that mistake, because the receiver always advances past the timestamp it receives.

The two classic schemes are the **Lamport clock** (one integer per node: compact, gives an order consistent with causality) and the **vector clock** (one counter per node per node: larger, but can also detect that two events are concurrent). Hybrid logical clocks combine a physical timestamp with a logical counter to get both causal order and closeness to real time.`,
    related: ['Lamport clock', 'Vector clocks', 'Happens-before', 'Clock skew'],
  },
  {
    term: 'Causality',
    aliases: ['causal order', 'causal ordering', 'causally related'],
    summary: 'The relation "could have influenced": one event can affect another only through a chain of local steps and messages.',
    detail: `In a distributed system, an event can affect another only if information could flow from the first to the second: through later steps on the same node, or through a message sent after the first event and received before the second. **Causality** is that potential influence. It is captured formally by the **happens-before** relation.

Example: \`n1\` writes x = 1 and sends the write to \`n2\`; \`n2\` reads x and then writes x = 2. The second write causally depends on the first. If \`n3\` meanwhile writes x = 3 without hearing from either, its write is causally unrelated to both: **concurrent**.

Causality matters because it is the order users and programs actually depend on. A reply should not appear before the message it answers; an update made in response to a value must not be overwritten by that value. Systems that preserve causal order (causal consistency) can do so without global coordination, which is why causal consistency is one of the strongest models that stays available during partitions.`,
    related: ['Happens-before', 'Concurrency detection', 'Logical time'],
  },
  {
    term: 'Happens-before',
    aliases: ['happened-before', 'happens before', 'happened before', 'happens-before relation'],
    summary: 'Lamport’s relation a → b: a precedes b on one node, a sends what b receives, or a chain links them.',
    detail: `The **happens-before** relation, written a → b, is the smallest relation such that: (1) if a and b occur on the same node and a comes first, a → b; (2) if a sends a message and b receives that message, a → b; (3) if a → b and b → c, then a → c. If neither a → b nor b → a, the events are **concurrent** (a ∥ b).

It is a *partial* order: some pairs of events are simply not ordered. Example: \`n1\` does event a and then sends a message that \`n2\` receives as event e; meanwhile \`n3\` does event c and sends nothing. Then a → e, but c ∥ a and c ∥ e, no matter what the wall clocks say.

Lamport introduced the relation in 1978 to replace "earlier in time" with something nodes can actually know. Lamport clocks guarantee that a → b implies L(a) < L(b); vector clocks characterise it exactly: a → b if and only if V(a) < V(b).`,
    related: ['Causality', 'Lamport clock', 'Vector clocks', 'Concurrency detection'],
  },
  {
    term: 'Vector clocks',
    aliases: ['vector clock', 'vector timestamps'],
    summary: 'One counter per node, carried on messages, so that comparing two timestamps shows whether one event caused the other or neither.',
    detail: `A **vector clock** is a map from node id to counter. Rules: before each event a node increments its *own* entry; every message carries the sender's vector; on receipt the node takes the entry-wise maximum of its vector and the message's, then increments its own entry. Entry \`k\` of an event's vector is then the number of node \`k\`'s events in its causal past.

Comparison: if every entry of V(a) is ≤ the matching entry of V(b) and they differ, a → b. If each has some entry larger than the other's, a and b are **concurrent**. Example with order [n1, n2, n3]: [2, 1, 0] → [2, 2, 0], but [2, 3, 0] and [3, 0, 3] are concurrent.

The cost is size: one entry per participant on every message and stored version, and a stable set of participants. Dynamo-style stores attach vector clocks (or the closely related **version vectors**) to stored values to detect conflicting writes, and Riak uses dotted version vectors to keep them small.`,
    related: ['Lamport clock', 'Concurrency detection', 'Version vectors', 'Happens-before'],
  },
  {
    term: 'Concurrency detection',
    aliases: ['concurrent events', 'concurrent', 'conflict detection'],
    summary: 'Proving from timestamps alone that two events happened without knowledge of each other, so neither can safely override the other.',
    detail: `Two events are **concurrent** when neither happened before the other: no chain of local steps and messages connects them. **Concurrency detection** means recognising this from the events' metadata. Vector clocks do it exactly: two vectors where each has an entry larger than the other's, such as [2, 3, 0] and [3, 0, 3], belong to concurrent events.

It matters most for writes. If write B causally follows write A, B was made with knowledge of A and may replace it. If A and B are concurrent, each writer acted without seeing the other's value; keeping just one silently loses an update. A replicated store that detects concurrency can keep both versions (siblings), merge them, or ask the application.

Lamport timestamps cannot detect concurrency: unrelated events still get different numbers, and the order between them is arbitrary. Physical timestamps cannot either. Detecting a conflict is also not resolving it; CRDTs are one way to make concurrent updates merge automatically.`,
    related: ['Vector clocks', 'Happens-before', 'CRDT'],
  },
  {
    term: 'Lamport clock',
    aliases: ['Lamport clocks', 'Lamport timestamp', 'Lamport timestamps'],
    summary: 'One integer per node, incremented on every event and advanced past any timestamp received, so causes are always stamped before effects.',
    detail: `A **Lamport clock** is an integer counter on each node. Before each event the node adds one and stamps the event with the result; messages carry the stamp of their send. On receiving a message stamped *t*, the node sets its clock to \`max(clock, t) + 1\`.

This guarantees the **clock condition**: if a → b then L(a) < L(b). Example: idle node \`n3\` sits at 2 when a message stamped 41 arrives; it jumps to 42, so anything it does in response is stamped after its cause. Breaking ties by node id, \`(L, node)\`, gives a **total order** that every node computes identically, which is used for last-writer-wins registers and for fair ordering in mutual exclusion.

The converse does not hold: L(a) < L(b) does not mean a caused b. To tell causally related events from concurrent ones, use vector clocks.`,
    related: ['Logical time', 'Vector clocks', 'Total order'],
  },
  {
    term: 'Version vectors',
    aliases: ['version vector', 'dotted version vectors'],
    summary: 'A per-replica counter map attached to a stored value, used to tell whether one version supersedes another or they conflict.',
    detail: `A **version vector** tracks, for one data item, how many updates from each replica a version includes. When replica \`r\` updates the item it increments entry \`r\`; when versions are merged, the entries take the maximum. Comparing two versions' vectors entry by entry says whether one descends from the other (safe to discard the older) or they are concurrent (a conflict to resolve).

The mechanics match vector clocks, but the purpose differs: vector clocks order every event in a system, while version vectors order the versions of one value and only count updates to it. Example: versions with [r1: 2, r2: 1] and [r1: 2, r2: 2] are ordered; [r1: 3, r2: 1] and [r1: 2, r2: 2] conflict.

Dynamo, Riak and many sync systems use version vectors. Dotted version vectors refine them so that many clients writing through a few servers do not cause the vectors to grow or produce false conflicts.`,
    related: ['Vector clocks', 'Concurrency detection'],
  },
  {
    term: 'Total order',
    aliases: ['total ordering', 'total-order'],
    summary: 'An order in which every pair of items is ranked, so all nodes that apply it put events in the same sequence.',
    detail: `A **total order** ranks every pair of elements: for any a ≠ b, either a comes before b or b before a. Happens-before is only a *partial* order, since concurrent events are not ranked. Many algorithms need a total order anyway, so that every node makes the same choice: which of two writes wins, which request gets a lock first.

Lamport timestamps give one cheaply: order by \`(timestamp, node id)\`. It is consistent with causality (causes come first) and the node id breaks ties, so the order is total and identical everywhere. Example: (4, n1) < (4, n2) < (5, n1).

Such an order is arbitrary for concurrent events, so it is fine for deterministic tie-breaking but not a substitute for knowing what users saw. Agreeing on a total order of *all* operations as they happen, with no later insertions, is total-order broadcast, which is equivalent to consensus.`,
    related: ['Lamport clock', 'Happens-before'],
  },
];
