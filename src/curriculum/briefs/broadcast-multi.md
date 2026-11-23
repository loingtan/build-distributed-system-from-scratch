Now there are five nodes, and a client may tell any one of them a value. A read from **any** node should return **all** values. Nodes have to talk to each other.

## What you build

The client-facing messages are the same as before: `broadcast` and `read`. What is new is the conversation *between* nodes, and that protocol is yours to design. Pick any message type you like, for example:

```json
n1 → n3   { "type": "gossip", "message": 1007 }
```

`node.peers` lists the other nodes. `node.send(peer, body)` sends a message without waiting for an answer.

## The trap

If a node forwards everything it hears, and its peers do the same, one value circulates forever. The simulator will cut the run short and report a **message storm**. Think about when forwarding is actually useful.

## The tests check

- every broadcast is acknowledged
- 1.5 seconds after the last broadcast, every node returns every value
- the cluster does not drown in its own messages

Open the **Timeline** after a run. You will see each client request fan out across the cluster.
