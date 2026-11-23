## The problem

Node `n1` sets a key to `"draft"` and stamps the change with its clock: 10:00:00.040. It sends the change to `n2`. Thirty milliseconds later `n2` receives it, and a user on `n2` immediately edits the same key to `"final"`. `n2` stamps its change with *its* clock, which happens to run 50 ms slow: 10:00:00.020.

Later, a third node receives both changes and keeps the one with the larger timestamp. It keeps `"draft"`. The user's edit, which was made *after* seeing the draft and in response to it, is thrown away, and nothing in the data shows that anything went wrong. The timestamps say the edit came first.

The cause is that the two timestamps came from two different clocks. Machine clocks drift and are corrected only periodically, so at any moment two of them disagree by some amount, often a few milliseconds and sometimes far more. A timestamp from `n1` and a timestamp from `n2` cannot be compared reliably, even though comparing them is exactly what "which came first?" needs.

This track replaces the question "what time was it?" with one that can be answered without trusting any clock: "could this event have influenced that one?". Two mechanisms answer it. A Lamport clock guarantees that a cause always gets a smaller number than its effect. A vector clock goes further and can tell whether two events are causally related at all.

## The core idea

### Events and causality

Think of each node as a sequence of **events**: handling a request, sending a message, receiving a message. Within one node the events have a clear order, because one process does one thing after another. Across nodes, the only thing that links events is messages. If `n1` sends a message and `n2` receives it, the receive comes after the send, whatever any clock says.

**Causality** is the relation "could have influenced". Information can only travel from one event to another through a node's own sequence of steps or through messages, so these are the only ways one event can affect another. Leslie Lamport made this precise as the **happens-before** relation, written a → b. It is the smallest relation satisfying three rules:

1. If a and b happen on the same node and a comes first, then a → b.
2. If a is the sending of a message and b is the receipt of that same message, then a → b.
3. If a → b and b → c, then a → c.

If neither a → b nor b → a, the events are **concurrent**, written a ∥ b. Concurrent does not mean "at the same instant". It means that no chain of steps and messages connects them, so neither could have known about the other. Two events a minute apart on nodes that never communicated in between are concurrent.

In the opening example, `n1`'s write → the send to `n2` → `n2`'s receive → the user's edit. The edit causally follows the draft. Any timestamp scheme that can put the draft after the edit is broken for this purpose.

### Logical time

**Logical time** assigns numbers to events from counters that nodes maintain themselves, instead of reading physical clocks. The numbers do not measure seconds. They only encode order, and they are designed so that the order they encode is consistent with happens-before.

### Lamport clocks

A **Lamport clock** is a single integer per node, starting at 0, with two rules:

1. **Tick:** before each event (a local action or sending a message), add one to the clock and stamp the event with the new value. A message carries the stamp of its send event.
2. **Receive:** when a message stamped *t* arrives, set the clock to `max(clock, t) + 1`. The receive is itself an event and gets that value.

The receive rule is the whole trick. A node that has been idle, with its clock near 0, can receive a message stamped 41. Without the rule it would stamp its next action 1, earlier than the cause it is reacting to. With the rule it jumps to 42, so everything it does from then on is stamped later than anything that could have caused it.

<figure>
<svg viewBox="0 0 600 270" role="img" aria-label="Three nodes with events a to j stamped by Lamport clocks; messages b to e, h to i, and a lost message from f">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="54">n1</text><text x="10" y="144">n2</text><text x="10" y="234">n3</text></g>
  <g style="stroke: var(--line)"><line x1="40" y1="50" x2="590" y2="50"/><line x1="40" y1="140" x2="590" y2="140"/><line x1="40" y1="230" x2="590" y2="230"/></g>
  <line x1="170" y1="50" x2="260" y2="140" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="380" y1="230" x2="460" y2="50" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="330" y1="140" x2="380" y2="195" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="378" y="206" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <g style="fill: var(--accent)">
    <circle cx="90" cy="50" r="5"/><circle cx="170" cy="50" r="5"/><circle cx="460" cy="50" r="5"/><circle cx="540" cy="50" r="5"/>
    <circle cx="150" cy="140" r="5"/><circle cx="260" cy="140" r="5"/><circle cx="330" cy="140" r="5"/>
    <circle cx="110" cy="230" r="5"/><circle cx="300" cy="230" r="5"/><circle cx="380" cy="230" r="5"/>
  </g>
  <g style="font: 12px var(--mono); fill: var(--ink)">
    <text x="80" y="36">a:1</text><text x="160" y="36">b:2</text><text x="450" y="36">i:4</text><text x="530" y="36">j:5</text>
    <text x="134" y="126">d:1</text><text x="262" y="126">e:3</text><text x="332" y="126">f:4</text>
    <text x="100" y="254">c:1</text><text x="290" y="254">g:2</text><text x="370" y="254">h:3</text>
  </g>
  <text x="400" y="128" style="font: 12px var(--sans); fill: var(--muted)">note from f lost</text>
</svg>
<figcaption>Lamport stamps. e jumps to max(1, 2) + 1 = 3 on receiving b's note; i jumps to max(2, 3) + 1 = 4 on receiving h's. Every arrow goes from a smaller number to a larger one.</figcaption>
</figure>

The guarantee is called the **clock condition**: if a → b, then L(a) < L(b). Causes always get smaller numbers than their effects. Sorting events by Lamport stamp therefore never puts an effect before its cause. To get a **total order**, an order in which every pair of events is ranked, break ties by node id: compare `(L, node id)` pairs. Every node computes the same order from the same stamps, which is useful for things like deciding which of two writes wins.

What a Lamport clock cannot do is the reverse. L(a) < L(b) does *not* imply a → b. In the diagram, d on `n2` has stamp 1 and j on `n1` has stamp 5, yet nothing connects them: `n2`'s only message after d was lost. Given just two numbers, you cannot tell "a caused b" from "a and b were unrelated and a's node was less busy".

### Vector clocks

A **vector clock** keeps the information that a single integer throws away. Each node keeps one counter *per node*: entry `n1` says how many of `n1`'s events this node knows about, entry `n2` how many of `n2`'s, and so on. The rules:

1. **Tick:** before each event, add one to *your own* entry, and only your own.
2. **Send:** a message carries the sender's whole vector.
3. **Receive:** for every entry, take the larger of your value and the message's value; then add one to your own entry for the receive event.

The entry for `n2` on node `n1` changes only by merging, never by `n1` incrementing it. Each entry is therefore a count of real events on its node that are in this node's causal past.

Comparing two vectors V(a) and V(b) entry by entry gives three possible answers:

- every entry of V(a) is ≤ the matching entry of V(b), and they are not equal: **a → b**;
- the other way around: **b → a**;
- each has some entry larger than the other's: **a ∥ b**, concurrent.

That last case is **concurrency detection**: proving from the timestamps alone that two events happened without knowledge of each other. A replicated database needs exactly this. If two writes to the same key are causally ordered, the later one can safely replace the earlier. If they are concurrent, neither writer saw the other's value, so silently keeping one would lose data; the conflict must be resolved by merging or by asking the application.

<figure>
<svg viewBox="0 0 600 270" role="img" aria-label="The same ten events stamped with vector clocks in the order n1, n2, n3">
  <g style="font: 13px var(--mono); fill: var(--ink)"><text x="10" y="54">n1</text><text x="10" y="144">n2</text><text x="10" y="234">n3</text></g>
  <g style="stroke: var(--line)"><line x1="40" y1="50" x2="590" y2="50"/><line x1="40" y1="140" x2="590" y2="140"/><line x1="40" y1="230" x2="590" y2="230"/></g>
  <line x1="170" y1="50" x2="260" y2="140" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="380" y1="230" x2="460" y2="50" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="330" y1="140" x2="380" y2="195" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="378" y="206" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <g style="fill: var(--m2)">
    <circle cx="90" cy="50" r="5"/><circle cx="170" cy="50" r="5"/><circle cx="460" cy="50" r="5"/><circle cx="540" cy="50" r="5"/>
    <circle cx="150" cy="140" r="5"/><circle cx="260" cy="140" r="5"/><circle cx="330" cy="140" r="5"/>
    <circle cx="110" cy="230" r="5"/><circle cx="300" cy="230" r="5"/><circle cx="380" cy="230" r="5"/>
  </g>
  <g style="font: 11px var(--mono); fill: var(--ink)">
    <text x="52" y="36">a[1,0,0]</text><text x="140" y="36">b[2,0,0]</text><text x="400" y="36">i[3,0,3]</text><text x="512" y="36">j[4,0,3]</text>
    <text x="100" y="126">d[0,1,0]</text><text x="222" y="126">e[2,2,0]</text><text x="320" y="126">f[2,3,0]</text>
    <text x="80" y="254">c[0,0,1]</text><text x="268" y="254">g[0,0,2]</text><text x="356" y="254">h[0,0,3]</text>
  </g>
  <g style="font: 12px var(--sans); fill: var(--muted)">
    <text x="420" y="110">f vs i: [2,3,0] vs [3,0,3]</text>
    <text x="420" y="128">each larger somewhere: concurrent</text>
  </g>
</svg>
<figcaption>Vector stamps for the same run. d [0,1,0] and j [4,0,3] are now visibly concurrent: j knows nothing of n2's events. c [0,0,1] ≤ j [4,0,3], so c → j, through h's note.</figcaption>
</figure>

> Lamport clocks: if a caused b, then a's number is smaller. Vector clocks: a caused b *exactly when* a's vector is smaller. Only the second lets you detect concurrency.

## A worked example

Three nodes run the lab's protocol. An `event` ticks once. A `notify` is a single event: tick once, send a `note` carrying the new stamp, and reply with the same stamp. Receiving a `note` applies the receive rule. Vectors are written in the order `[n1, n2, n3]`. This is the run in the two diagrams.

| Step | Node | Action | Lamport | Vector |
|---|---|---|---|---|
| a | n1 | event | 1 | [1, 0, 0] |
| b | n1 | notify n2: sends `note` | 2 | [2, 0, 0] |
| c | n3 | event | 1 | [0, 0, 1] |
| d | n2 | event, before b's note arrives | 1 | [0, 1, 0] |
| e | n2 | receives b's note: max(1, 2) + 1; merge [0,1,0] with [2,0,0], then tick own | 3 | [2, 2, 0] |
| f | n2 | notify n3: sends `note` — **lost** | 4 | [2, 3, 0] |
| g | n3 | event (the note from f never arrives) | 2 | [0, 0, 2] |
| h | n3 | notify n1: sends `note` | 3 | [0, 0, 3] |
| i | n1 | receives h's note: max(2, 3) + 1; merge [2,0,0] with [0,0,3], then tick own | 4 | [3, 0, 3] |
| j | n1 | event | 5 | [4, 0, 3] |

The message from f is lost. Neither clock needs special handling for that: causality only travels with messages that are actually delivered. Because n3 never heard from n2, every event on n3 after the loss is correctly concurrent with f. If the note had arrived just before g, the receive on n3 would have been stamped [2, 3, 2] (Lamport 5), and g would have been [2, 3, 3] with Lamport stamp 6, causally after f.

Now ask questions of the stamps:

| Pair | Lamport says | Vector says | Truth |
|---|---|---|---|
| b, e | 2 < 3 | [2,0,0] ≤ [2,2,0] | b → e (message) |
| c, j | 1 < 5 | [0,0,1] ≤ [4,0,3] | c → g → h → i → j |
| d, j | 1 < 5 | [0,1,0] vs [4,0,3]: concurrent | d ∥ j |
| f, i | 4 = 4 | [2,3,0] vs [3,0,3]: concurrent | f ∥ i |
| f, g | 4 > 2 | [2,3,0] vs [0,0,2]: concurrent | f ∥ g (the note was lost) |

The Lamport column is never wrong about causes (whenever the truth is →, the stamps increase), but it invents an order for d and j and for f and g that does not exist. The vector column matches the truth on every row.

## Why it is correct

**Lamport clocks.** Claim: if a → b then L(a) < L(b). By the definition of happens-before, a → b means there is a chain of steps from a to b, each of which is either "next event on the same node" or "send, then the matching receive". It is enough to show that every single step increases the stamp; transitivity does the rest. For a step on the same node, the tick rule adds one before each event, and the clock never decreases, so the later event has a strictly larger stamp. For a send/receive step, the message carries the send's stamp *t*, and the receive gets `max(clock, t) + 1 > t`. So every step strictly increases the stamp, and so does any chain.

The assumption that matters is that the clock only goes forward: no event may reuse a stamp, and a node that restarts must not start again from 0 unless it has persisted its clock or makes sure its new stamps exceed old ones.

**Vector clocks.** Claim: entry `k` of V(e) equals the number of events on node `k` that happened before or are equal to e. It holds at each node's start (all zeros). A tick adds exactly one event of the node itself to its causal past, and the rule adds one to the node's own entry and nothing else. A receive adds to the causal past everything the sender knew at the send, plus the receive itself; taking the entry-wise maximum computes the union of the two causal pasts (each entry counts a prefix of one node's events, so the larger count contains the smaller), and the own-entry tick counts the receive.

From that, a → b exactly when V(a) ≤ V(b) entry-wise and V(a) ≠ V(b). If a → b, b's causal past contains a's, so every count is at least as large, and b itself adds at least one. Conversely, if V(a) ≤ V(b), then in particular b's count for a's node includes a, so a is in b's causal past. Concurrency is the remaining case.

This relies on each node incrementing *only its own entry*. If a receiver also bumped the sender's entry, the counts would no longer correspond to real events and comparisons would report causality that never happened.

## Trade-offs and failure modes

**Size.** A Lamport stamp is one integer. A vector clock has one entry per node, carried on every message and stored with every versioned value. With five nodes that is trivial; with thousands of clients each acting as a writer, it is not. Systems limit the participants (only servers get entries, not clients), prune old entries (risking false concurrency), or use more compact variants such as dotted version vectors.

**Membership changes.** A vector needs a stable set of node ids. Nodes that join need new entries; nodes that leave forever leave entries that can only be dropped once everyone agrees they are gone.

**Hidden channels.** Logical clocks only see causality that flows through the messages they stamp. If a user reads a value on `n1` and then types it into `n2` by hand, or two services communicate through a database the clocks do not know about, the real causal link is invisible and the events look concurrent (vector) or arbitrarily ordered (Lamport).

**No connection to real time.** Logical timestamps say nothing about seconds. You cannot ask "which writes happened in the last five minutes?" or expire data by age with them. Systems that need both use **hybrid logical clocks**, which combine a physical timestamp with a logical counter that preserves the clock condition when physical clocks disagree.

**Detecting concurrency is not resolving it.** A vector clock tells you two writes conflict. It does not say what the merged value should be. Something else must decide: last-writer-wins by a total order (losing one write), returning both versions to the application, or a data type designed to merge, such as the CRDTs in the next track.

## In real systems

**Amazon Dynamo and Riak.** The Dynamo paper describes using vector clocks on each stored version of a key. When a read finds versions whose clocks are ordered, the older ones are discarded; when it finds concurrent versions, it returns all of them to the client as siblings, and the application merges them (the shopping cart example: take the union of items) and writes back the result. Riak followed this design and later switched to dotted version vectors to keep the clocks small and avoid false conflicts.

**CockroachDB.** CockroachDB timestamps transactions with hybrid logical clocks: each timestamp is a physical time plus a logical counter, and a node that receives a timestamp ahead of its own clock advances past it, exactly like the Lamport receive rule. This keeps causally related transactions ordered even when physical clocks disagree by a bounded offset, while timestamps stay close to real time.

**Git.** A Git history is a happens-before graph made explicit. Each commit names its parent commits, so commit A happened before commit B exactly when A is an ancestor of B. Two commits where neither is an ancestor of the other are concurrent: they were made on different branches without knowledge of each other, and combining them requires a merge. Git does not need vector clocks because it stores the whole graph, which is the most precise and most expensive representation of causality.

## Common misconceptions

- **"A smaller Lamport timestamp means the event happened first."** It means the event did not happen *after*: it cannot be the effect. Unrelated events get arbitrary relative stamps.
- **"Concurrent means at the same time."** Concurrent means neither event could have known about the other. Events seconds apart on nodes that did not exchange messages are concurrent.
- **"Synchronised clocks make logical clocks unnecessary."** NTP keeps clocks within milliseconds at best, and only while it works. Events closer together than the skew can still be stamped in the wrong order, including a cause and its effect.
- **"On receive, increment the sender's entry."** A node increments only its own entry. Other entries change only by taking the maximum with incoming vectors, which is what keeps every entry a true count of events.
- **"Vector clocks resolve conflicts."** They detect them. Choosing the surviving value is a separate decision.

## How the tasks build it

[TM-1 Lamport clocks](#/c/lamport-clock) implements the two rules for `event`, `notify` and `note` messages. The tests check that each node's stamps increase from one event to the next, that a notify's note and its reply carry the same stamp, and that after a node receives a note stamped *t*, everything it does is stamped later than *t*. The second test keeps one node busy and the others idle, so an idle node's clock has to jump a long way when a note arrives; forgetting the receive rule fails immediately there.

[TM-2 Vector clocks](#/c/vector-clock) replaces the integer with one counter per node. The checker maintains its own vector clocks from the messages it sees and compares every reply and every note with yours, entry by entry, so there is no slack: one extra tick, a missing tick on receive, or an increment to the wrong entry shows up as a mismatch. A notify must tick once and use the same clock for the note and the reply.

## Further reading

- [Lamport, Time, Clocks, and the Ordering of Events in a Distributed System (1978)](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — the paper that defined happens-before and logical clocks; short and still the clearest statement of the idea.
- [Vector clock (Wikipedia)](https://en.wikipedia.org/wiki/Vector_clock) — the rules and comparison in compact form, with references to the original 1988 papers by Fidge and Mattern.
- [DeCandia et al., Dynamo: Amazon's Highly Available Key-value Store (2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — section 4.4 shows vector clocks detecting conflicting versions in a production store.
- [Kulkarni et al., Logical Physical Clocks (2014)](https://cse.buffalo.edu/tech-reports/2014-04.pdf) — the hybrid logical clock paper, combining physical time with Lamport's receive rule.
- [Designing Data-Intensive Applications, chapter 8](https://dataintensive.net/) — Kleppmann's account of why physical clocks are unreliable and what that means for ordering.
