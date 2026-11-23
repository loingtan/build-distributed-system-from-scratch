export default [
  {
    term: 'SWIM',
    aliases: ['SWIM protocol'],
    summary: 'A membership protocol that detects failures by probing one random member per period and spreads changes by gossip.',
    detail: `**SWIM** (Scalable Weakly-consistent Infection-style process group Membership) was published by Das, Gupta and Motivala in 2002. It splits membership into two parts: a **failure detector** that probes one member per protocol period, directly and then indirectly through k helpers, and a **dissemination** component that piggybacks membership updates on those probe messages.

Each node sends a constant number of messages per period whatever the cluster size, and a crash is detected and spread to everyone in O(log N) periods. Later refinements added a suspect state and incarnation numbers so that a slow node can refute a false accusation.

HashiCorp's memberlist library (used by Consul, Nomad and Serf) is a widely deployed SWIM implementation.`,
    related: ['Protocol period', 'Indirect probing', 'Gossip dissemination', 'Incarnation number'],
  },
  {
    term: 'Protocol period',
    aliases: ['probe period'],
    summary: 'The fixed interval in which each SWIM node probes exactly one other member.',
    detail: `In each **protocol period** (200ms in this track) a node picks one member, pings it, and if needed asks others to ping it indirectly. The whole probe must finish inside the period, so the ack timeout and the indirect timeout together must be shorter than it.

Choosing targets by walking a shuffled list of members, and reshuffling after each pass, bounds the gap between two probes of the same member: with N nodes it is at most 2(N-1)-1 periods. Picking uniformly at random has the same average but no bound.

The period sets the trade-off between detection speed and load: halving it doubles every node's message rate.`,
    related: ['SWIM', 'Message budget'],
  },
  {
    term: 'Failure detector',
    aliases: ['failure detection'],
    summary: 'A component that tells a node which other nodes it currently believes have failed.',
    detail: `A **failure detector** turns silence into a judgement. Because a crashed node and a slow one look the same, it can only suspect, and is described by two properties: **completeness** (every crashed node is eventually suspected by every correct node) and **accuracy** (correct nodes are not suspected, or rarely).

All-to-all heartbeats are the simplest design but cost O(N) messages per node per interval. SWIM keeps completeness and good accuracy at O(1) messages per node per period by probing one member at a time and asking others for a second opinion.`,
    related: ['SWIM', 'Suspicion mechanism'],
  },
  {
    term: 'Message budget',
    aliases: ['message load', 'per-node load'],
    summary: 'A cap on how many messages each node may send per second, independent of cluster size.',
    detail: `A **message budget** makes scalability testable: a protocol whose per-node traffic grows with N will exceed any fixed budget once the cluster is big enough.

With heartbeats every 100ms to every peer, a node in a cluster of 16 sends 150 messages per second. A SWIM node sends one ping per 200ms period and answers about one ping, roughly 10 messages per second, whether there are 5 members or 500. The tests in this track allow 30 per node per second.`,
    related: ['SWIM', 'Protocol period'],
  },
  {
    term: 'Indirect probing',
    aliases: ['indirect probe', 'indirect ping'],
    summary: 'Asking k other members to ping a target that missed its ack, before suspecting it.',
    detail: `When a direct ping gets no ack, the prober sends a **ping-req** to k randomly chosen members (k = 3 here). Each pings the target itself and reports back. The target is suspected only if none of them got an ack.

This separates "I cannot reach it" from "nobody can reach it". If the link n1–n2 drops everything, n1's helpers n3, n4 and n5 still reach n2 over their own links, so n2 stays alive in n1's view. With 2% random loss, a probe fails falsely only if the direct exchange and all three indirect paths fail together: about 1 probe in 50,000 instead of 1 in 25.`,
    related: ['Ping-req', 'False positives'],
  },
  {
    term: 'Ping-req',
    aliases: ['ping_req', 'ping request'],
    summary: 'The SWIM message asking another member to ping a target on the sender\'s behalf and report the result.',
    detail: `A **ping-req** names a target. The helper pings it with its own short timeout and replies with the outcome, in this track \`{ "type": "ping_req_ok", "ok": true }\`.

The prober sends its ping-reqs in parallel and needs only one positive answer. They cost messages only when a direct ping fails, so in a healthy cluster indirect probing adds almost nothing.`,
    related: ['Indirect probing'],
  },
  {
    term: 'False positives',
    aliases: ['false positive', 'false suspicion', 'false alarm'],
    summary: 'Declaring a live node failed because of lost messages, a bad link or a pause.',
    detail: `A **false positive** in failure detection is an alarm about a node that is actually alive. It is costly: data gets re-replicated, leaders are replaced and clients are redirected for nothing, and a node that is wrongly thrown out may have to rejoin.

SWIM reduces false positives at three levels: indirect probes rule out a single bad path; the suspect state gives an accused node time to answer; and incarnation numbers let its answer override the accusation everywhere.`,
    related: ['Indirect probing', 'Suspicion mechanism'],
  },
  {
    term: 'Gossip dissemination',
    aliases: ['dissemination', 'gossip'],
    summary: 'Spreading an update by having each node that knows it pass it on to a few others, round after round.',
    detail: `In **gossip dissemination** a node that learns an update tells a few others, who tell a few others, and so on. The number of informed nodes roughly doubles each round, so all N nodes know after about log₂ N rounds, plus a few more to reach the stragglers.

Gossip tolerates loss well: every update travels on many messages along many paths, so losing some of them only slows it down a little. Its guarantees are probabilistic; sending each update about 3·log₂(N+1) times makes the chance that some node never hears it negligible.`,
    related: ['Piggybacking', 'Infection-style dissemination'],
  },
  {
    term: 'Piggybacking',
    aliases: ['piggyback'],
    summary: 'Attaching extra information to a message that is being sent anyway, instead of sending a new one.',
    detail: `SWIM **piggybacks** membership updates on its pings, acks and ping-reqs: each carries a few entries from the sender's buffer of recent updates, least-sent first. Spreading news therefore costs no extra messages, only a few bytes per message.

Each entry is retransmitted a bounded number of times (about 3·log₂(N+1)) and then dropped, so buffers stay small. A node that learns something new buffers it with a fresh count, which is what makes the update keep spreading.`,
    related: ['Gossip dissemination'],
  },
  {
    term: 'Infection-style dissemination',
    aliases: ['epidemic dissemination', 'epidemic protocol'],
    summary: 'Spreading information the way an infection spreads: every informed node informs others it happens to contact.',
    detail: `The "I" in SWIM. An update is like an infection: an informed node passes it on to every node it exchanges a message with. Since each node contacts at least one new member per period, the informed fraction grows exponentially until nearly everyone knows, then the last few catch it.

Epidemic algorithms were studied for replicated databases by Demers et al. (1987). Their appeal is that no node is special and no message is essential.`,
    related: ['Gossip dissemination', 'Piggybacking'],
  },
  {
    term: 'Suspicion mechanism',
    aliases: ['suspect state', 'suspicion timeout'],
    summary: 'Marking an unresponsive member suspect first, and declaring it dead only if nobody refutes within a timeout.',
    detail: `With the **suspicion mechanism**, a failed probe does not kill a member. It is marked **suspect**, the suspicion is gossiped, and every node starts a timer. If the suspect is still suspect at the same incarnation when the timer fires (2.5 seconds here), it is declared dead.

The timeout must exceed the time a live suspect needs to hear the rumour and spread its refutation. Too short and brief pauses become deaths; too long and real crashes stay undetected for longer.`,
    related: ['Incarnation number', 'Refutation', 'False positives'],
  },
  {
    term: 'Incarnation number',
    aliases: ['incarnation'],
    summary: 'A counter each node owns about itself; only it increases it, to override rumours about its own state.',
    detail: `Every membership update carries an **incarnation number**. Of two updates about a node, the higher incarnation wins; at equal incarnation, dead beats suspect beats alive. Because only the node itself increments its number, a higher number always means "the node spoke after this rumour started".

Example: n3 is suspect at incarnation 2. n3 hears it, sets its incarnation to 3 and gossips alive(3), which beats suspect(2) everywhere. A stale suspect(2) arriving later loses. Persisting the number lets a restarted node come back above any old rumour.`,
    related: ['Refutation', 'Rejoin', 'Suspicion mechanism'],
  },
  {
    term: 'Refutation',
    aliases: ['refute'],
    summary: 'A suspected node announcing it is alive with a higher incarnation, cancelling the suspicion.',
    detail: `A **refutation** happens when a node learns it is suspected (or declared dead) at an incarnation at least its own. It raises its incarnation above the rumour's and gossips alive at the new number.

The suspect must hear about the rumour quickly, so SWIM implementations put updates about the receiver first when piggybacking. An alive message at the *same* incarnation would lose to the suspicion, which is why the number must go up.`,
    related: ['Incarnation number', 'Suspicion mechanism'],
  },
  {
    term: 'Rejoin',
    aliases: ['rejoining'],
    summary: 'A node that was declared dead returning to the membership after a restart.',
    detail: `After a crash, every other node remembers the restarted node as dead at its old incarnation. To **rejoin** it must announce itself alive at a higher one, so it keeps its incarnation on disk and adds one at every start.

If it restarted at incarnation 0, its alive(0) would lose to dead(0) everywhere and it would stay out of the cluster although it is running.`,
    related: ['Incarnation number'],
  },
];
