Before a cluster can replace a dead node, somebody has to notice that it is dead. In a distributed system that is harder than it sounds: a crashed node, a slow node and a node behind a broken network link all look identical from the outside. They are silent.

So nodes do not *know* a peer has failed. They **suspect** it, after not hearing from it for a while.

## What you build

A heartbeat failure detector. Each node regularly tells its peers "I am alive" and tracks when it last heard from each of them. The tests look at a node only through `status`:

```json
→ { "type": "status" }
← { "type": "status_ok", "suspected": ["n4"] }
```

`suspected` lists the peers this node currently believes are unreachable.

## The trade-off

One number decides how your detector behaves: how long a peer may stay silent before it is suspected.

- **Too short** and a single delayed or lost heartbeat raises a false alarm. In a real system that triggers failovers nobody needed.
- **Too long** and a real failure goes unnoticed while requests pile up.

The tests fix both ends: react within **1.5 seconds**, and raise **no false alarms** on a network that loses one message in ten.

## The tests check

- with everyone reachable, nobody is suspected (after a one-second warm-up)
- when one node is cut off, every other node suspects it, and only it, within 1.5s
- the isolated node suspects everybody else
- suspicion is withdrawn within 1.5s of the network healing
- no false alarms on a lossy network
