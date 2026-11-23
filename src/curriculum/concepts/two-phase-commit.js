export default [
  {
    term: 'Two-phase commit',
    aliases: ['2PC', 'two phase commit'],
    summary: 'A coordinator asks every participant to vote, then sends one decision, commit or abort, that all of them carry out.',
    detail: `**Two-phase commit** makes a transaction that spans several nodes atomic. In phase one the coordinator sends each participant its part and asks it to prepare: check that it can apply the part, promise to keep that possible, and vote yes or no. In phase two the coordinator decides commit if every vote was yes and abort otherwise, and sends the decision to everyone.

Example: moving 30 from n2.a to n3.b. Both nodes vote yes, the coordinator decides commit, and each applies its move. If n2 had voted no (insufficient funds), both would have been told to abort and nothing would change.

2PC is safe under message loss and crashes, given a durable log. Its weakness is blocking: a participant that voted yes cannot finish until it hears from the coordinator. PostgreSQL, MySQL (XA), Spanner and Kafka transactions all use it.`,
    related: ['Atomicity', 'Votes', 'Blocking', 'In-doubt transactions'],
  },
  {
    term: 'Atomicity',
    aliases: ['atomic commit'],
    summary: 'All of a transaction\'s effects happen, or none of them do, even when they are spread over several nodes.',
    detail: `**Atomicity** rules out partial outcomes. A transfer that debits one account and credits another either does both or neither; there is no state in which the money has left one account and not reached the other.

On one machine this is achieved with a log and undo/redo. Across machines it is the **atomic commit** problem: the nodes must reach the same decision, commit or abort, even though messages can be lost and nodes can crash. Two-phase commit is the standard solution.

Atomicity is not isolation. Two transfers can each be atomic and still together overdraw an account, unless concurrent transactions are prevented from relying on the same money.`,
    related: ['Two-phase commit', 'Locking'],
  },
  {
    term: 'Votes',
    aliases: ['vote', 'prepare vote'],
    summary: 'A participant\'s answer to prepare: yes is a promise it can still apply its part when told; no ends the transaction.',
    detail: `In two-phase commit each participant answers prepare with a **vote**. A no vote (the account would go negative, or a needed lock is taken) means the coordinator must abort. A yes vote is a promise: "whenever you tell me to commit, I will be able to."

Keeping that promise is the participant's job. It must reserve or lock what it promised, so that no other transaction can use it, and it must record the promise on disk before sending the vote, so that a crash cannot erase it.

After voting yes, the participant has given up its right to decide. It waits for the coordinator. A missing vote, from the coordinator's point of view, counts as no.`,
    related: ['Two-phase commit', 'In-doubt transactions', 'Locking'],
  },
  {
    term: 'Locking',
    aliases: ['locks', 'lock'],
    summary: 'Marking data as in use by one transaction so that others cannot change or rely on it until that transaction ends.',
    detail: `In two-phase commit, a participant that votes yes **locks** every account the transaction touches, from the vote until commit or abort. While locked, another transaction's prepare on that account votes no (or waits). This is what makes the yes vote a promise that can be kept: nothing else can spend the money in between.

Example: n1.a holds 100 and transfers T1 and T2 each want 60. T1 prepares and locks \`a\`. T2's prepare finds \`a\` locked and votes no. Without the lock, both would commit and \`a\` would reach −20.

Locks must be released on commit and on abort; a forgotten lock makes an account unusable. Holding locks across network round trips limits throughput, which is a central cost of distributed transactions.`,
    related: ['Reservations', 'No-wait', 'Deadlock'],
  },
  {
    term: 'Reservations',
    aliases: ['reservation', 'escrow'],
    summary: 'Setting aside the amount a prepared transaction needs, so later checks treat promised money as already spent.',
    detail: `A **reservation** is a finer-grained alternative to locking an account. When a participant votes yes to a debit of 30, it adds 30 to the account's reserved total. The next prepare checks the balance minus everything reserved.

Example: an account of 100 can accept two prepared debits of 30 (reserved 60), but a third of 60 votes no, because 100 − 60 − 60 < 0. With a lock, the second debit of 30 would already have been refused.

Reservations allow more concurrency for commutative operations like additions. They must be released on commit (when the change becomes real) and on abort. The technique is also known as escrow.`,
    related: ['Locking', 'Votes'],
  },
  {
    term: 'No-wait',
    aliases: ['no wait'],
    summary: 'A locking policy: a request that finds data locked is refused at once instead of waiting, so deadlock cannot occur.',
    detail: `Under **no-wait**, a transaction that needs a lock held by another does not queue for it: it fails immediately. In two-phase commit, the prepare simply votes no.

Because no transaction ever waits for another, no cycle of waiting can form, and deadlock is impossible without any detection. The cost is unnecessary aborts under contention: a transfer that would have succeeded a millisecond later is refused, and the client must retry.

Alternatives are waiting with deadlock detection, timeouts, or ordering rules such as wait-die and wound-wait, which let some transactions wait based on their age.`,
    related: ['Locking', 'Deadlock'],
  },
  {
    term: 'Timeouts',
    aliases: ['timeout'],
    summary: 'A limit on how long to wait for a reply, after which the sender acts as if the reply will not come.',
    detail: `In a network that can lose messages, a node cannot tell a slow peer from a dead one or an unreachable one. A **timeout** turns "no answer yet" into a decision.

Whether that decision is safe depends on who makes it. In two-phase commit, the coordinator may time out on a missing vote and abort, because before its decision nobody can have committed. A participant that voted yes may not time out and decide, because the coordinator may already have decided the other way.

Timeouts also bound how long a client waits: the coordinator answers within a deadline even if a participant is cut off, and keeps delivering the decision in the background.`,
    related: ['Blocking', 'In-doubt transactions'],
  },
  {
    term: 'Blocking',
    aliases: ['blocking protocol'],
    summary: 'A protocol is blocking if a single failure can leave other nodes unable to finish until the failed node returns.',
    detail: `Two-phase commit is the standard example of a **blocking** protocol. A participant that has voted yes cannot decide on its own: the coordinator might have told others to commit, or to abort. If the coordinator crashes or is cut off at that moment, the participant waits, with its locks held, until it comes back. No timeout makes that wait safe.

The practical cost is that locked data stays unusable for as long as the coordinator is gone, and transactions touching it fail or queue.

Non-blocking alternatives replicate the decision: Paxos Commit and Spanner make the coordinator a consensus group, so a minority of failures cannot stop it. Three-phase commit avoids blocking only under timing assumptions that real networks do not meet.`,
    related: ['In-doubt transactions', 'Two-phase commit', 'Timeouts'],
  },
  {
    term: 'In-doubt transactions',
    aliases: ['in-doubt transaction', 'in doubt', 'prepared transaction'],
    summary: 'A transaction a participant has voted yes to but whose outcome it has not yet learned.',
    detail: `Between sending a yes vote and receiving commit or abort, a participant's transaction is **in doubt**. The participant knows only that it promised; it does not know what was decided.

It must keep the promise: hold its locks, keep the prepared moves (on disk, so a crash does not lose them), and wait for the coordinator. After a restart it rebuilds its locks from its prepared transactions and keeps waiting.

Real databases expose in-doubt transactions: PostgreSQL lists them in \`pg_prepared_xacts\`, MySQL in \`XA RECOVER\`. Operators can resolve them by hand, at the risk of contradicting the coordinator.`,
    related: ['Blocking', 'Votes', 'Recovery'],
  },
  {
    term: 'Write-ahead logging',
    aliases: ['write-ahead logs'],
    summary: 'Record a fact on durable storage before acting on it or telling anyone, so a crash cannot erase a promise already made.',
    detail: `**Write-ahead logging** fixes an order: write to the log first, then apply the change or send the message that depends on it. If the node crashes after the write, recovery finds the record; if it crashes before, nobody was told anything.

In two-phase commit: a participant logs its prepared moves before sending yes; a coordinator logs "started T with these participants" before the first prepare, and its decision before sending it. A yes vote sent before it is logged is a promise the node may forget after a crash.

Databases use the same principle for single-node durability (PostgreSQL's WAL, MySQL's redo log). The cost is a forced disk write on the critical path, which systems amortise by batching (group commit).`,
    related: ['Recovery', 'Durability', 'Presumed abort'],
  },
  {
    term: 'Recovery',
    aliases: ['transaction recovery'],
    summary: 'After a restart, read the log and finish every transaction that was left open, consistently with what others may have done.',
    detail: `**Recovery** in two-phase commit rebuilds state from the log and completes unfinished work:

- a transaction the coordinator started but never decided cannot have committed anywhere, so it is aborted and the participants are told;
- a decided transaction is resent to every participant that has not acknowledged it;
- a participant restores its balances and committed ids, rebuilds locks from prepared transactions, and keeps waiting for their outcomes.

The rule that makes recovery safe is that nothing was ever sent that the log does not justify. Recovery must also be idempotent, since a node can crash again while recovering.`,
    related: ['Write-ahead logging', 'Presumed abort', 'In-doubt transactions'],
  },
  {
    term: 'Presumed abort',
    summary: 'If a coordinator has no record of a transaction, the answer is abort, so only commit decisions must be forced to disk.',
    detail: `Under **presumed abort**, the absence of information means abort. A coordinator that crashes before deciding needs no record of the transaction: when an in-doubt participant asks about it, the coordinator finds nothing and answers abort. Aborts therefore need not be logged before they are sent, nor acknowledged with care; only commit decisions must be durable before the first commit message leaves.

Since most failures lead to aborts, this saves disk writes and messages on exactly the path that is most common under trouble. It was introduced in IBM's R* system and is used by most commercial implementations.

The course's design is coordinator-driven (participants never ask), so it logs a "started" record to know whom to tell; the principle that undecided means abort is the same.`,
    related: ['Recovery', 'Write-ahead logging', 'Two-phase commit'],
  },
  {
    term: 'Deadlock',
    summary: 'Two or more transactions each wait for a lock another holds, so none can ever proceed.',
    detail: `A **deadlock** forms when T1 holds lock x and waits for y while T2 holds y and waits for x. Neither releases what it holds, so both wait forever.

Three ways to deal with it: prevent waiting altogether (no-wait), allow waiting only in a safe direction (wait-die, wound-wait, or always acquiring locks in a fixed order), or detect cycles in the waits-for graph and abort a victim.

In distributed transactions detection is harder, because the cycle spans nodes. Two crossing transfers, n1.a → n2.b and n2.b → n1.a, are the minimal example.`,
    related: ['No-wait', 'Locking'],
  },
];
