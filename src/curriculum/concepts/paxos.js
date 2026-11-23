export default [
  {
    term: 'Paxos',
    aliases: ['single-decree Paxos', 'Basic Paxos'],
    summary: 'A consensus protocol in which any node may propose and a majority of acceptors decides one value, safely, despite lost messages and crashes.',
    detail: `Paxos, published by Leslie Lamport, chooses a single value among nodes that can crash and lose or reorder messages. Every node may act as a **proposer** (suggests values), an **acceptor** (votes) and a **learner** (finds out the result).

A proposer runs two phases, each needing replies from a majority of acceptors: **prepare** with a ballot number, then **accept** with that ballot and a value. A value is chosen once a majority accepts it in one ballot.

Paxos never chooses two values, whatever the timing. It does not promise progress: two proposers can keep pre-empting each other, which randomised backoff or a stable leader fixes in practice.`,
    related: ['Ballot', 'Prepare and accept', 'Chosen value', 'Multi-Paxos'],
  },
  {
    term: 'Ballot',
    aliases: ['ballot number', 'proposal number'],
    summary: 'A unique, totally ordered number a proposer attaches to its attempt; acceptors honour only the highest ballot they have promised.',
    detail: `Every Paxos attempt carries a ballot. Ballots must be unique across proposers and comparable, so ties never happen. A common scheme is \`round * N + nodeIndex\`: with three nodes, n1 uses 3, 6, 9…, n2 uses 4, 7, 10…, and so on.

A proposer that is refused picks a ballot above every one it has seen and tries again. Higher ballots pre-empt lower ones, which is how a stalled attempt is taken over, and also how two proposers can duel.

In Multi-Paxos the ballot doubles as the leader's identity: the node holding the highest ballot leads, much like a term in Raft.`,
    related: ['Paxos', 'Promise'],
  },
  {
    term: 'Prepare and accept',
    aliases: ['phase 1', 'phase 2', 'prepare', 'accept'],
    summary: 'The two rounds of Paxos: prepare collects promises and earlier accepted values from a majority; accept asks that majority to accept one value.',
    detail: `**Phase 1 (prepare).** The proposer sends \`prepare(b)\`. An acceptor that has not promised a higher ballot promises to ignore everything below \`b\` and replies with the last \`(ballot, value)\` it accepted, if any.

**Phase 2 (accept).** With promises from a majority, the proposer picks a value: the accepted value with the highest ballot among the promises, or its own value if none reported one. It sends \`accept(b, v)\`; an acceptor accepts unless it has promised a ballot above \`b\`.

The rule in the middle is what makes Paxos safe: a proposer that ignores reported values can choose a second value after the first was already chosen.`,
    related: ['Ballot', 'Chosen value', 'Promise'],
  },
  {
    term: 'Chosen value',
    aliases: ['chosen'],
    summary: 'The value accepted by a majority of acceptors in the same ballot; once chosen it can never change, even before anyone knows it.',
    detail: `A value is chosen at the moment a majority of acceptors have accepted it in one ballot. No node has to observe this moment: the proposer may crash before counting the replies, and the value is still chosen.

Paxos guarantees that every later ballot proposes the same value. Any later proposer's phase 1 majority overlaps the choosing majority in at least one acceptor, which reports the value, so the proposer must adopt it.

One acceptor having accepted a value proves nothing: that value can still be replaced by a higher ballot. Only a majority in one ballot counts.`,
    related: ['Prepare and accept', 'Learner', 'Majority'],
  },
  {
    term: 'Majority',
    aliases: ['majority quorum'],
    summary: 'More than half of all nodes: floor(N/2) + 1. Any two majorities share at least one node.',
    detail: `With 5 nodes a majority is 3; with 3 nodes it is 2. Because two sets of more than half the nodes cannot be disjoint, anything a majority did is visible to every later majority.

Paxos relies on this twice: a value accepted by a majority is reported to every later proposer, and two proposers can never both gather a majority of promises for conflicting purposes in the same ballot. Count against the size of the whole cluster, never against the nodes that happened to reply.`,
    related: ['Chosen value', 'Network partition'],
  },
  {
    term: 'Promise',
    summary: 'An acceptor\'s commitment, made in phase 1, to reject every ballot lower than the one it answered.',
    detail: `When an acceptor answers \`prepare(b)\`, it promises never to accept a ballot below \`b\`. Its reply also reports what it has accepted so far. The proposer relies on both: the report lets it find any value that may be chosen, and the promise guarantees no lower ballot sneaks a different value in behind its back.

A promise is only worth something if it survives. An acceptor that crashes, forgets \`promised\` and restarts can accept a ballot it swore to refuse, which can lead to two chosen values. Store it on disk before replying.`,
    related: ['Durability', 'Ballot'],
  },
  {
    term: 'Durability',
    aliases: ['persistent state'],
    summary: 'State written to stable storage before it is relied on, so that it survives a crash.',
    detail: `In Paxos only the acceptor's state must be durable: \`promised\` and the accepted \`(ballot, value)\` (per slot in Multi-Paxos). Write it before sending the reply that depends on it; a crash can land the instant the reply leaves.

Proposer and learner state can be rebuilt: a restarted proposer picks a fresh, higher ballot, and a restarted learner hears the chosen value again.`,
    related: ['Promise'],
  },
  {
    term: 'Randomised backoff',
    aliases: ['duelling proposers', 'livelock'],
    summary: 'Waiting a random, growing time before retrying, so competing proposers stop pre-empting each other forever.',
    detail: `Two proposers can duel: A's prepare pre-empts B's accept, B retries with a higher ballot and pre-empts A's accept, and so on. Nothing is ever chosen, although nothing unsafe happens either.

Breaking the symmetry fixes it in practice: after a failed round wait, say, 20ms plus a random amount that grows with each failure. One proposer soon gets both phases done before the other wakes up. A stable leader (Multi-Paxos) avoids most duels altogether.`,
    related: ['Ballot', 'Stable leader'],
  },
  {
    term: 'Network partition',
    aliases: ['partition'],
    summary: 'A failure that splits the nodes into groups that cannot reach each other, while each group keeps running.',
    detail: `During a partition every side believes the others may have crashed. With majority-based protocols like Paxos only the side holding a majority can choose anything; the minority side waits. When the partition heals, minority nodes learn what was chosen and their stale ballots are refused.`,
    related: ['Majority'],
  },
  {
    term: 'Learner',
    aliases: ['learning'],
    summary: 'The Paxos role that finds out which value was chosen, from the proposer or from acceptors.',
    detail: `Choosing and learning are separate. The proposer knows a value is chosen once a majority accepted it; everyone else must be told. The usual approach: the proposer announces the chosen value and keeps retrying until each node confirms.

A node that misses the announcement can still find out by running a proposal of its own (phase 1 will lead it to the chosen value) or, in Multi-Paxos, by asking the leader.`,
    related: ['Chosen value', 'Catch-up'],
  },
  {
    term: 'Multi-Paxos',
    summary: 'Running one Paxos instance per log slot behind a stable leader, so a whole sequence of commands is agreed on.',
    detail: `A replicated state machine needs agreement on an order of commands, not on one value. Multi-Paxos keeps a log of slots and runs single-decree Paxos in each one.

The saving comes from phase 1: a leader's single \`prepare(b)\` covers every slot, and acceptors reply with everything they have accepted. As long as nobody prepares a higher ballot, the leader runs only phase 2 per command: one round trip instead of two.

A new leader re-proposes, in each slot, the highest-ballot accepted value reported to it, and fills holes with no-ops. Chubby, Spanner and many other systems run Multi-Paxos variants.`,
    related: ['Slot', 'Stable leader', 'No-op'],
  },
  {
    term: 'Slot',
    aliases: ['log slot', 'instance'],
    summary: 'One position in the Multi-Paxos log, with its own independent Paxos decision.',
    detail: `Slot 1 holds the first command, slot 2 the second, and so on. Each slot is chosen independently, so slot 7 can be chosen before slot 6. A node applies commands only in slot order, and only once every earlier slot is known.`,
    related: ['Multi-Paxos', 'No-op'],
  },
  {
    term: 'Stable leader',
    aliases: ['distinguished proposer'],
    summary: 'A single proposer that keeps its ballot across many slots, skipping phase 1 for each new command.',
    detail: `Once a node has promises for ballot \`b\` from a majority, those promises apply to every slot, so it can go straight to \`accept(b, slot, value)\` for each command. It sends heartbeats so other nodes do not start elections.

If it crashes or is cut off, another node prepares a higher ballot and takes over; the old leader's next accept is refused and it steps down. A stable leader is a performance device: safety never depends on there being only one.`,
    related: ['Multi-Paxos', 'Ballot'],
  },
  {
    term: 'No-op',
    aliases: ['no-op entry', 'noop'],
    summary: 'A command that does nothing, chosen to fill a log slot no proposer has a value for.',
    detail: `A new Multi-Paxos leader may find slot 9 accepted somewhere but no report for slot 8: the old leader's accept for 8 reached too few acceptors, or none. Slot 8 cannot stay empty, since nobody could ever apply slot 9. The leader proposes a no-op for slot 8 (in this track, \`null\`).`,
    related: ['Slot', 'Multi-Paxos'],
  },
  {
    term: 'Catch-up',
    aliases: ['state transfer'],
    summary: 'A lagging node fetching chosen values it missed, after lost messages, a partition or a restart.',
    detail: `Announcements of chosen slots can be lost, and a partitioned or restarted node misses many. If the leader advertises how many slots it knows, say in every heartbeat, a node that knows fewer can ask for the values from its first unknown slot onwards, and retry if the answer is lost.

Real systems also send a snapshot of the state machine instead of a long list of commands once the log is compacted.`,
    related: ['Learner', 'Slot'],
  },
];
