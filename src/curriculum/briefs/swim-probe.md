The heartbeat failure detector from the first tracks has every node tell every other node "I am alive" several times a second. With 5 nodes that is cheap. With 500 nodes each one sends and receives thousands of messages per second, just to learn that nothing happened.

**SWIM** (Scalable Weakly-consistent Infection-style Membership) fixes the cost first: in each **protocol period** a node checks on exactly *one* member.

## What you build

Every 200ms, each node picks one peer and pings it. If an ack comes back within a short timeout, the peer is `alive`; if not, it is `suspect`. A later successful ping makes it `alive` again. Clients read a node's view with `members`:

```json
→ { "type": "members" }
← { "type": "members_ok", "members": { "n1": "alive", "n2": "alive", "n3": "suspect" } }
```

`members` must contain **every node id, the node itself included**, each mapped to `"alive"`, `"suspect"` or `"dead"` (this task only needs the first two). Answer it at once from memory.

The node-to-node messages are yours to design; the starter's names `ping` and `ack` are a good choice.

## The one idea

Each node sends about two messages per period (its own ping, and an ack for the ping it receives), **whatever the size of the cluster**. Detection stays fast because every node probes someone each period. Choose targets by going round a **shuffled list** of peers, reshuffling after each pass, so no peer goes unprobed for long.

## The tests check

- after a 600ms warm-up, with no faults, no node ever reports a live node as anything but `alive`
- a node that crashes at 1.5s is reported as not `alive` by every other node within **3 seconds**
- every node sends at most **30 node messages per second**, averaged over the run, in a cluster of 5 and of 16 (the starter's heartbeats send 40 and 150)
- every node answers `members` within 300ms with the format above
