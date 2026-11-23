So far every message has arrived. Now the network partitions, and two-phase commit shows its hardest property.

## The coordinator may give up early

Until the coordinator has decided, nobody can have committed. So when a vote does not come back in time, the coordinator can treat it as **no**, abort, and tell the client. The tests expect an answer within **one second** even when a participant is cut off, so use a prepare timeout of a few hundred milliseconds.

The decision must still reach everyone, including the participant that is cut off. Keep resending commit or abort until each participant acknowledges it. A participant must handle the same outcome twice, and an abort for a transaction it never prepared, without harm.

## The participant may not

A participant that has voted yes is **in doubt**: the coordinator may have decided commit, and told the others. If the participant times out and aborts on its own, one node commits what another aborted. So it waits, with its locks held, until it hears the outcome. This is why two-phase commit is called **blocking**.

## What you build

The same messages as before. Coordinators time out on missing votes and retry the outcome; participants never decide on their own.

## Timing

- Partitions last up to **1.5s**.
- Within **1s after the network heals**, every transaction must be finished: the outcome delivered, locks released. A transfer sent then that the accounts can afford must commit.
- A transfer whose coordinator cannot reach a participant for the whole transaction must be answered `committed: false`.

## The tests check

- a participant cut off before the transaction: the coordinator aborts in time
- a participant cut off right after voting yes: it waits, then applies the outcome
- a coordinator cut off after its first prepare: nobody commits, and the prepared participant is released after the heal
- a network that loses one message in five: still all or nothing, and at least a quarter of transfers commit
