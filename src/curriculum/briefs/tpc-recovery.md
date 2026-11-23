Partitions end. Crashes are worse: a crashed node loses everything in memory. A participant that voted yes and then crashed has forgotten its promise. A coordinator that crashed halfway through has forgotten which transactions it was running and what it decided.

## The log

Anything a node has told another node must survive a crash, so it goes on `node.disk` **before** the message is sent. The tests crash nodes the instant after a send, so anything written after the send is lost.

- **Participant:** the moves of each transaction it voted yes to, until the outcome arrives. Balances and committed ids too.
- **Coordinator:** "started txn T with these participants" before the first prepare; then the decision, until every participant has acknowledged it.

## Recovery

After a restart the file runs from the top with the same `node.disk`. Use it to finish what was open:

- A transaction the coordinator **started but never decided** cannot have committed anywhere. Abort it and tell the participants.
- A **decided** transaction is sent again to every participant that has not acknowledged it.
- A participant rebuilds its locks from its prepared transactions, and keeps waiting for their outcome.

A client whose coordinator crashes may never get a reply. That is fine; what matters is that the nodes agree.

## What you build

The same messages as before. `status` after a restart must report the state from disk.

## Timing

A crashed node stays down for about **0.6s**. Within **1s of its restart**, every transaction it was part of must be finished and its locks released.

## The tests check

- a participant crashes right after voting yes, and later one is down for a whole transaction
- a coordinator crashes before any vote reaches it, leaving both participants prepared
- a coordinator crashes after sending its decision to only one participant
- random crashes under steady load: still all or nothing, and no money lost
