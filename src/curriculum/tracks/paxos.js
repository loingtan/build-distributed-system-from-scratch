// Track: Paxos. See docs/authoring.md for the shape of this file.
import { paxosSingle, paxosLog } from '../../sim/workloads/paxos.js';

export default {
  id: 'paxos',
  code: 'PX',
  category: 'consensus',
  title: 'Paxos',
  level: 'Advanced',
  blurb: 'Reach agreement without a fixed leader: single-decree Paxos with proposers, acceptors and ballots, then Multi-Paxos for a whole log.',
  subtracks: [
    {
      title: 'Choosing one value',
      tasks: [
        {
          slug: 'paxos-single',
          title: 'Single-decree Paxos',
          level: 'Hard',
          minutes: 40,
          summary: 'Every node can propose; prepare and accept on a majority so that all nodes learn the same single value.',
          concepts: ['ballot', 'prepare and accept', 'chosen value', 'majority'],
          workload: paxosSingle,
          tests: [
            { name: 'One proposer', blurb: 'One client proposes at n1; later another proposes a different value at n3 and must be told the first.', nodeCount: 3, duration: 3500, params: { scenario: 'one' } },
            { name: 'Concurrent proposers', blurb: 'Five clients propose five different values at five nodes within 4ms of each other.', nodeCount: 5, duration: 4500, params: { scenario: 'concurrent' } },
            { name: 'Lost messages', blurb: 'The same race with one node-to-node message in five lost.', nodeCount: 5, duration: 5000, dropRate: 0.2, params: { scenario: 'concurrent' } },
            { name: 'Chosen, but nobody knows', blurb: 'A value is chosen and only its proposer knows; a second proposer that cannot reach it must still end up with that value.', nodeCount: 3, duration: 4500, params: { scenario: 'hidden' } },
          ],
          hints: [
            'Two phases, each needing replies from a **majority** of all nodes (count yourself). Phase 1: `prepare { ballot }`. Phase 2: `accept { ballot, value }`. A value is chosen when a majority accepts it in one ballot.',
            'Ballots must be unique and ordered. `round * N + index` gives every node its own ballots; a retry takes the next round above any ballot it has seen in a reply.',
            'An acceptor keeps two things: `promised`, the highest ballot it answered in phase 1, and `accepted`, the last `{ ballot, value }` it accepted. It refuses an `accept` with a ballot below `promised`, and every `prepare` reply carries `accepted`.',
            'The step everyone gets wrong: after phase 1, if **any** promise carries an accepted value, propose the one with the highest ballot, not your own. That value may already be chosen; your majority overlaps the one that chose it in at least one acceptor (the *Chosen, but nobody knows* test).',
            'When a round fails (refused or timed out), wait a random time before the next, or two proposers can pre-empt each other forever. Once a value is chosen, tell every peer with `node.rpc` and keep retrying the ones that have not confirmed: with lost messages a single `send` is not enough.',
          ],
        },
        {
          slug: 'paxos-durable',
          title: 'Crashes and partitions',
          level: 'Hard',
          minutes: 30,
          summary: 'Keep each acceptor\'s promise and accepted value on disk, so the decision survives crashes, partitions and duelling proposers.',
          concepts: ['durability', 'promise', 'randomised backoff', 'partition'],
          workload: paxosSingle,
          tests: [
            { name: 'Minority partition', blurb: 'Two of five nodes are cut off for 2.4s while clients propose on both sides.', nodeCount: 5, duration: 5000, params: { scenario: 'partition' } },
            { name: 'Promise forgotten in a crash', blurb: 'n2 promises two proposers, crashes the instant it answers the second, and restarts before either phase 2 arrives.', nodeCount: 3, duration: 4500, params: { scenario: 'crash-promise' } },
            { name: 'Whole cluster restarts', blurb: 'Every node crashes the moment the first propose_ok is sent, and restarts 400ms later.', nodeCount: 5, duration: 4500, params: { scenario: 'full-restart' } },
            { name: 'Duelling proposers', blurb: 'All five nodes propose at the same instant, twice, on slow links (20–60ms) that lose 1 message in 20.', nodeCount: 5, duration: 5500, latency: [20, 60], dropRate: 0.05, params: { scenario: 'duel' } },
          ],
          hints: [
            'An acceptor\'s reply is a promise about its future. If a crash erases `promised`, it can accept a ballot it swore to refuse; if it erases `accepted`, a chosen value can vanish from the majority that chose it.',
            'Keep `promised` and `accepted` on `node.disk` and read them back at the top of the file: `let promised = node.disk.promised ?? 0;`.',
            'Write the disk **before** you reply. The *Promise forgotten* test crashes a node the instant its reply is on the wire.',
            'The proposer and learner need nothing on disk: a restarted node answers `get` with `null` until it learns the value again, and a client whose node crashed retries elsewhere. Saving the learned value as well is fine.',
            'With five proposers on slow links, fixed retry delays keep colliding. Grow the random backoff with every failed round, and stop proposing as soon as any reply tells you the value is chosen.',
          ],
        },
      ],
    },
    {
      title: 'Multi-Paxos',
      tasks: [
        {
          slug: 'paxos-multi',
          title: 'A log of slots',
          level: 'Hard',
          minutes: 50,
          summary: 'Run one Paxos instance per log slot behind a stable leader that skips phase 1, so every node applies the same sequence of commands.',
          concepts: ['Multi-Paxos', 'slot', 'stable leader', 'no-op'],
          workload: paxosLog,
          tests: [
            { name: 'Healthy network', blurb: 'Three nodes, two clients appending; every node must learn every slot.', nodeCount: 3, duration: 5000, params: { scenario: 'stable' } },
            { name: 'Slow links', blurb: 'Five nodes on 20–60ms links, so messages overtake each other all the time.', nodeCount: 5, duration: 6000, latency: [20, 60], params: { scenario: 'stable', gap: 60 } },
            { name: 'Leader crashes', blurb: 'The leader crashes the instant it acknowledges an append, before anyone else knows the slot is chosen. It never returns.', nodeCount: 5, duration: 9000, params: { scenario: 'leader-crash' } },
            { name: 'Stable leader', blurb: 'Five nodes. The cluster idles for a second, then clients append steadily: each append may cost at most 3.5 messages per follower beyond the idle background traffic.', nodeCount: 5, duration: 6500, params: { scenario: 'stable', startAt: 2600, measure: { idle: [1500, 2500], busy: [2800, 5300], perFollower: 3.5 } } },
          ],
          hints: [
            'Every slot is its own single-decree Paxos, but one phase 1 can cover all of them: `prepare { ballot }` makes an acceptor promise `ballot` for every slot, and it replies with everything it has accepted. After that, the leader runs only phase 2, `accept { ballot, slot, value }`, one round trip per command.',
            'Leader election is just phase 1. A node that hears nothing from a leader for a random 300–600ms prepares a higher ballot; with a majority of promises it is the leader. The leader sends a heartbeat every 100ms; any node that sees a higher ballot (in a heartbeat, a prepare or a refusal) stops leading.',
            'A new leader must, for every slot any promise mentions, re-propose the accepted value with the highest ballot (the *Leader crashes* test). Slots below the highest one that nobody reports get a no-op (`null`), so the log has no holes.',
            'Reply `append_ok` only when the slot is chosen (a majority accepted it) and every slot before it is known too; then announce `{ slot, value }` to the other nodes. `log` answers the values of slots 1..k with no gap.',
            'When a leader is refused, or hears of a higher ballot, it steps down and answers every waiting client `not_leader`. The client retries elsewhere; a command that lands in two slots is allowed.',
          ],
        },
        {
          slug: 'paxos-catchup',
          title: 'Catching up',
          level: 'Hard',
          minutes: 35,
          summary: 'Fill the holes a node has after lost messages, a partition or a crash, and keep acceptor state on disk so the log survives restarts.',
          concepts: ['learner', 'catch-up', 'durability'],
          workload: paxosLog,
          tests: [
            { name: 'Lost messages', blurb: 'One node-to-node message in seven is lost, so some nodes miss announcements of chosen slots.', nodeCount: 5, duration: 6000, dropRate: 0.15, params: { scenario: 'stable' } },
            { name: 'Leader cut off', blurb: 'The leader and one follower are cut off for 3.5s with appends that can never be chosen; the other three carry on, and both must catch up after the heal.', nodeCount: 5, duration: 12000, params: { scenario: 'cut-leader' } },
            { name: 'Follower restarts', blurb: 'A follower crashes at 2.5s and comes back at 4s with only its disk.', nodeCount: 3, duration: 7500, params: { scenario: 'follower-restart' } },
            { name: 'Whole cluster restarts', blurb: 'Every node crashes at 3s and restarts at 3.5s. No acknowledged append may be lost.', nodeCount: 3, duration: 8500, params: { scenario: 'full-restart' } },
          ],
          hints: [
            'A node can tell it is behind: put the leader\'s count of known slots in every heartbeat. A follower that knows fewer asks the leader for the values from its first unknown slot on.',
            'Lost messages hurt the leader too: an `accept` that gets no majority must be sent again, for as long as this node leads with that ballot.',
            'The acceptor state must survive a crash: `promised` and every slot\'s `accepted { ballot, value }`. Keep them on `node.disk` and write them before replying, exactly as in the single-value task.',
            'What a node has learned need not be on disk. After a restart a follower learns it again from the leader; a new leader learns it from the accepted values in phase 1 and re-proposes them.',
            'After the whole cluster restarts, nobody knows anything is chosen. The first leader\'s phase 1 recovers every accepted slot; ask from slot 1 when you know nothing.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Proposing your own value after phase 1', symptom: 'Two nodes report different chosen values after concurrent proposals.', fix: 'If any promise in your majority carries an accepted value, propose the one with the highest ballot. Your own value is only for when none does.' },
    { title: 'Counting replies instead of a majority of the cluster', symptom: 'A minority partition chooses a value on its own, and the other side chooses another.', fix: 'A phase succeeds only with replies from floor(N/2)+1 nodes, the proposer included, N being the size of the whole cluster.' },
    { title: 'Keeping acceptor state only in memory', symptom: 'After a restart an acceptor accepts a lower ballot it had promised to refuse, or forgets a value it helped choose.', fix: 'Write promised and accepted to node.disk before replying, and read them back on start.' },
    { title: 'Retrying immediately after a failed round', symptom: 'Two proposers keep pre-empting each other; nothing is ever chosen.', fix: 'Pause for a random, growing time before retrying, and stop as soon as a reply says a value is chosen.' },
  ],
  questions: [
    { q: 'Why must a proposer adopt the highest-ballot accepted value it sees in phase 1?', a: 'That value may already be chosen. Any majority shares an acceptor with the majority that chose it, and every ballot between then and now proposed the same value, so the highest accepted ballot in the promises carries it. Proposing anything else could choose a second value.' },
    { q: 'An acceptor has accepted (ballot 7, value 42). Does that mean 42 is chosen?', a: 'No. A value is chosen only when a majority accepted it in the same ballot. One acceptor\'s accepted value may be overwritten by a later ballot; it can be lost, unless a majority also has it.' },
    { q: 'What does Multi-Paxos save over running single-decree Paxos for every slot?', a: 'Phase 1. One prepare with a ballot covers every future slot, so while the leader keeps its ballot each command needs only the accept round trip: half the messages and half the latency.' },
  ],
  reading: [
    { kind: 'Paper', title: 'Paxos Made Simple', url: 'https://lamport.azurewebsites.net/pubs/paxos-simple.pdf', note: 'Leslie Lamport\'s own short explanation: the two phases and the invariant, in eleven pages.' },
    { kind: 'Paper', title: 'Paxos Made Live: An Engineering Perspective', url: 'https://research.google.com/archive/paxos_made_live.pdf', note: 'Google on building Chubby\'s Multi-Paxos: disk corruption, leases, master churn and testing.' },
    { kind: 'Paper', title: 'Paxos Made Moderately Complex', url: 'https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf', note: 'Robbert van Renesse\'s pseudocode for Multi-Paxos with replicas, leaders and acceptors.' },
  ],
};
