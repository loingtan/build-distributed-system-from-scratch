In the last task one failed probe killed a member for good. A node that pauses for half a second (a garbage-collection pause, a brief network blip) is thrown out by everyone, and a crashed node that restarts can never come back: "dead" beats everything.

SWIM adds two things: a **suspect** state between alive and dead, and **incarnation numbers** that let a node argue with rumours about itself.

## What you build

Every update now carries the incarnation the rumour is about:

```json
{ "id": "n3", "status": "suspect", "inc": 2 }
```

- Each node owns its incarnation. Only it ever increases it. Keep it in `node.disk` and **add one every time the node starts**, then spread `alive` about yourself at the new number.
- Of two updates about a node, the higher `inc` wins; at equal `inc`, `dead` beats `suspect` beats `alive`. Ignore (and do not re-gossip) anything that does not win.
- A failed probe makes the target `suspect` at its current `inc`. If it is still `suspect` at that `inc` after the **suspicion timeout** (2.5 seconds is a good value), declare it `dead`.
- A node that hears it is `suspect` or `dead` at an `inc` at least its own **refutes**: it sets its incarnation to that `inc + 1` and spreads `alive`.

Piggyback updates about the receiver first, so a suspect hears about itself quickly. `members` reports only the status.

## The one idea

Ordering updates by (incarnation, status) gives every node the same answer whatever order rumours arrive in, and the node in question always has the last word about itself.

## The tests check

Eight nodes, 200ms periods.

- **Cut off briefly:** one node is partitioned from all others for 0.5s. Nobody may ever be reported `dead`, and within **2.5s** of the network healing every view is all `alive`
- **Crash, 2% loss:** the crashed node is `dead` everywhere within **6 seconds**
- **Crash and rejoin:** a node crashes, is `dead` everywhere within 6s, restarts, and is `alive` everywhere within **2.5s** of restarting
- no node that never crashed is ever `dead`; at most **30 node messages per node per second**
