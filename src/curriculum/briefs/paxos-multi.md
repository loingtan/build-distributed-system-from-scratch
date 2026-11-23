One agreed value is not much use on its own. A replicated service needs an agreed *sequence* of commands, and Multi-Paxos gives it one by running a separate Paxos instance for each position, or **slot**, in a log. Run naively, every command would cost two round trips. The trick is that a single phase 1 can cover every slot at once. After that, a **stable leader** needs only phase 2 for each new command.

## What you build

Clients append values. The leader accepts them, and any other node points to the leader it believes in (or `null`):

```json
→ { "type": "append", "value": 17 }
← { "type": "append_ok" }
← { "type": "not_leader", "leader": "n3" }
```

Reply `append_ok` only once the value's slot is chosen and every slot before it is known. Every 100ms the tests ask each node for its log:

```json
→ { "type": "log" }
← { "type": "log_ok", "entries": [17, 4, null, 9] }
```

`entries` lists the values chosen for slots 1, 2, 3, … in order, stopping at the first slot this node doesn't know. A `null` entry is a **no-op**. A client that times out retries elsewhere, so one value may end up in two slots. That's allowed.

## The idea

- `prepare { ballot }` makes an acceptor promise `ballot` for **every** slot. It replies with everything it has accepted, slot by slot. A node that wins a majority this way is the leader.
- For each command the leader sends `accept { ballot, slot, value }`. A majority of acceptances means the slot is chosen. Then tell the other nodes.
- A new leader re-proposes, in every slot a promise mentions, the accepted value with the highest ballot. Slots that nobody mentions, below the highest one, get a no-op.
- A node that sees a higher ballot stops leading. A node that hears from no leader for a while (300–600ms works) runs phase 1 itself.

## Timing

Clients start at 1s. Each append must get `append_ok` within **2s**, except within 2.5s of a crash. A client waits 500ms before trying another node. Every running node must know every chosen slot by 250ms before the end (600ms on the local runner).

## The tests check

- no two nodes ever report different values for one slot, and logs only grow
- every acknowledged append is in the log, in the order each client sent them
- the leader crashes the instant it sends `append_ok`, before anyone knows the slot is chosen; the new leader must keep it there
- **a stable leader skips phase 1**: after the cluster has idled for a second, clients append steadily, and each append may cost at most **3.5 node-to-node messages per follower** on average, beyond the background traffic (heartbeats and so on) measured while idle. Phase 2 needs 2 per follower (accept, accepted), 3 with a decide notice; re-running phase 1 for every command costs at least 4
