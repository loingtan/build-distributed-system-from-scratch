// Track: Distributed transactions. See docs/authoring.md for the shape of this file.
import { tpcBasic, tpcConcurrent, tpcPartitions, tpcCrashes } from '../../sim/workloads/two-phase-commit.js';

export default {
  id: 'two-phase-commit',
  code: 'TP',
  category: 'convergence',
  title: 'Distributed transactions',
  level: 'Advanced',
  blurb: 'Commit across several nodes or not at all: two-phase commit, timeouts, and recovering after a crash.',
  subtracks: [
    {
      title: 'Atomic commit',
      tasks: [
        {
          slug: 'tpc-prepare-commit',
          title: 'Prepare, then commit',
          level: 'Medium',
          minutes: 25,
          summary: 'Move money between accounts on different nodes so that every node applies the transfer, or none does.',
          concepts: ['two-phase commit', 'atomicity', 'votes'],
          workload: tpcBasic,
          tests: [
            { name: 'Transfers that fit', blurb: 'One transfer at a time between two nodes, every one affordable.', nodeCount: 3, duration: 3500, params: { ops: 20 } },
            { name: 'Some overdraw', blurb: 'Four in ten transfers ask for more than the account holds and must change nothing.', nodeCount: 3, duration: 4000, params: { ops: 24, overdraw: 0.4 } },
            { name: 'Three-way transfers', blurb: 'Four nodes; some transfers credit two accounts, sometimes on two different nodes.', nodeCount: 4, duration: 4500, params: { ops: 28, overdraw: 0.3, threeWay: 0.5 } },
          ],
          hints: [
            'Run the starter against "Some overdraw" and open the Timeline: the credit lands on one node while the debit is refused on the other.',
            'Split the work into two rounds. In the first, each participant only *checks* its moves and answers yes or no. Nothing changes yet.',
            'Remember what a participant voted yes to (a `Map` from `txn` to its moves). Apply exactly those moves when `commit` arrives, and forget them on `abort`.',
            'The coordinator can send `prepare` to itself with `node.rpc(node.id, ...)`, so it does not need a special case when it is also a participant.',
          ],
        },
        {
          slug: 'tpc-isolation',
          title: 'Concurrent transfers',
          level: 'Medium',
          minutes: 25,
          summary: 'Two transfers that each fit must not both be promised the same money.',
          concepts: ['locking', 'reservations', 'no-wait'],
          workload: tpcConcurrent,
          tests: [
            { name: 'Racing withdrawals', blurb: 'Three transfers take 60% of one account at the same moment; then the rest is drained.', nodeCount: 4, duration: 4000, params: { scenario: 'race' } },
            { name: 'Crossing transfers', blurb: 'Two accounts send money to each other at the same time, then both must be usable again.', nodeCount: 3, duration: 4000, params: { scenario: 'cross' } },
            { name: 'Busy bank', blurb: 'A new large transfer every 20ms for two seconds, then every account is drained.', nodeCount: 4, duration: 5000, params: { scenario: 'storm' } },
          ],
          hints: [
            'Between its yes vote and the outcome, a participant has made a promise. Something must stop a second transaction from being promised the same money.',
            'Simplest: lock each account a prepared transaction touches. A `prepare` that needs a locked account votes no.',
            'Refusing straight away (rather than waiting for the lock) means two transactions can never wait for each other, so there is no deadlock to detect.',
            'Unlock in both `commit` and `abort`. A forgotten lock shows up as a later transfer that is refused although the money is there.',
          ],
        },
        {
          slug: 'tpc-timeouts',
          title: 'Timeouts and partitions',
          level: 'Hard',
          minutes: 35,
          summary: 'A coordinator that cannot reach everyone aborts; a participant that voted yes waits for the outcome, however long it takes.',
          concepts: ['timeouts', 'blocking', 'in-doubt transactions'],
          workload: tpcPartitions,
          tests: [
            { name: 'Participant unreachable', blurb: 'A participant is cut off before the transaction starts; the coordinator must abort and answer in time.', nodeCount: 3, duration: 9500, params: { scenario: 'unreachable', rounds: 3 } },
            { name: 'Cut off after voting', blurb: 'A participant is isolated right after sending its yes vote, so it cannot hear the outcome for 1.5s.', nodeCount: 3, duration: 9000, params: { scenario: 'voted', rounds: 3 } },
            { name: 'Coordinator cut off', blurb: 'The coordinator is isolated just after its first prepare leaves; one participant is left prepared.', nodeCount: 3, duration: 9000, params: { scenario: 'coordinator', rounds: 3 } },
            { name: 'Lossy network', blurb: 'One message in five between nodes is lost.', nodeCount: 3, duration: 8000, dropRate: 0.2, params: { scenario: 'lossy' } },
          ],
          hints: [
            'Give each `prepare` a timeout of a few hundred milliseconds, and treat a missing vote as a no. Before the decision, aborting is always safe.',
            'Answer the client as soon as the decision is made. Delivering it to the participants can take as long as the partition lasts.',
            'A message saying commit or abort can be lost. Keep resending it until the participant acknowledges, and make handling it twice harmless.',
            'A participant that voted yes must **not** time out and decide on its own: the coordinator may already have told the others to commit. It waits, locks held.',
            'An abort can reach a node that never prepared the transaction (its vote was no, or the prepare was lost). Acknowledge it anyway.',
          ],
        },
        {
          slug: 'tpc-recovery',
          title: 'Crash recovery',
          level: 'Hard',
          minutes: 40,
          summary: 'Log votes and decisions on disk so that nodes finish every transaction consistently after a crash.',
          concepts: ['write-ahead logging', 'recovery', 'presumed abort'],
          workload: tpcCrashes,
          tests: [
            { name: 'Participant crashes', blurb: 'A participant crashes the instant it sends its yes vote, and later one is down for a whole transaction.', nodeCount: 3, duration: 13000, params: { scenario: 'participant', rounds: 3 } },
            { name: 'Coordinator crashes early', blurb: 'The coordinator crashes just before the first vote reaches it; both participants are left prepared.', nodeCount: 3, duration: 8000, params: { scenario: 'early', rounds: 3 } },
            { name: 'Coordinator crashes mid-decision', blurb: 'The coordinator crashes after sending its decision to only one participant.', nodeCount: 3, duration: 8000, params: { scenario: 'decision', rounds: 3 } },
            { name: 'Random crashes', blurb: 'A steady stream of transfers while nodes crash and restart at random.', nodeCount: 4, duration: 8000, params: { scenario: 'random' } },
          ],
          hints: [
            'Everything in memory is gone after a crash. Keep balances, committed ids, prepared transactions and coordinator decisions on `node.disk`.',
            'Order matters: write to disk first, then send the message that depends on it. A yes vote sent before it is on disk is a promise the node may forget.',
            'A coordinator should note "I started txn T with these participants" before the first prepare. After a restart, a started-but-undecided T can be aborted: no one can have committed it.',
            'A decided T is resent to every participant that has not acknowledged it yet, so keep the list of who still has to answer.',
            'Locks need not be on disk: rebuild them at startup from the prepared transactions.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Applying the change during prepare',
      symptom: 'Money appears on one node while the other node refused the transfer; the cluster total drifts.',
      fix: 'Prepare only checks and promises. Balances change on commit, after every participant has voted yes.',
    },
    {
      title: 'Voting yes without reserving anything',
      symptom: 'Passes when transfers arrive one at a time; under concurrency an account goes negative.',
      fix: 'A yes vote is a promise the moves can still be applied. Lock or set aside what was promised until commit or abort.',
    },
    {
      title: 'Letting a prepared participant time out and abort',
      symptom: 'After a partition heals, one node has committed a transaction that another node aborted.',
      fix: 'Only the coordinator decides. A participant that voted yes waits, with its locks held, until it learns the outcome.',
    },
    {
      title: 'Writing the log after sending the message',
      symptom: 'Works until a crash lands between the send and the write; then a node forgets a vote or a decision it already announced.',
      fix: 'Write to node.disk first, then send. On restart, finish whatever the log says is still open.',
    },
  ],
  questions: [
    {
      q: 'Why is two-phase commit called a blocking protocol?',
      a: 'A participant that has voted yes cannot decide on its own: the coordinator may already have told others to commit, or to abort. If the coordinator is down or unreachable, the participant waits, holding its locks, until it comes back. No timeout can make that wait safe.',
    },
    {
      q: 'Before the decision, a coordinator may abort when a vote is missing. Why is that safe, while the same timeout on a participant is not?',
      a: 'Until the coordinator decides, no one can have committed, because a commit needs the decision. After a yes vote the participant no longer knows that: the decision may already exist. The coordinator is the one node that always knows whether a decision has been made.',
    },
    {
      q: 'What is "presumed abort", and what does it save?',
      a: 'If a coordinator finds no decision for a transaction (after a crash, or when asked about one it forgot), it answers abort. So aborts need not be logged or acknowledged with the same care as commits; only commit decisions must be forced to disk before they are sent. Most real implementations use it. The reference solution for TP-4 is simpler and logs every decision, aborts included (sometimes called presumed nothing); presumed abort is the optimisation you would add on top.',
    },
  ],
  reading: [
    { kind: 'Book', title: 'Designing Data-Intensive Applications, chapter 9', url: 'https://dataintensive.net/', note: 'Atomic commit, two-phase commit and why the coordinator is a single point of blocking.' },
    { kind: 'Paper', title: 'Gray and Lamport: Consensus on Transaction Commit', url: 'https://www.microsoft.com/en-us/research/uploads/prod/2004/01/twophase-revised.pdf', note: 'Why two-phase commit blocks when the coordinator fails, and how Paxos Commit avoids it.' },
    { kind: 'Docs', title: 'PostgreSQL: PREPARE TRANSACTION', url: 'https://www.postgresql.org/docs/current/sql-prepare-transaction.html', note: 'The participant half of 2PC in a real database, and its warning about prepared transactions left hanging.' },
  ],
};
