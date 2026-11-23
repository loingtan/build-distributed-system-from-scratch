In primary-backup, the primary talks to every backup and carries the whole load. **Chain replication** (van Renesse and Schneider, 2004) arranges the nodes in a line instead. Writes enter at one end, the **head**, and flow node by node to the other end, the **tail**. Each node sends to one successor. Key-value stores such as FAWN-KV are built on it.

## What you build

The chain is `node.nodeIds` in order: `n1` is the head, the last node is the tail.

Writes go to the head. The write is complete when it reaches the tail, so the **tail** acknowledges it, not the head:

```json
→ n1  { "type": "write", "key": "k3", "value": 4, "msg_id": 12 }
← n3  { "type": "write_ok", "in_reply_to": 12 }
```

Reads go to the tail and are answered from its copy:

```json
→ n3  { "type": "read", "key": "k3" }
← n3  { "type": "read_ok", "value": 4 }
```

A request at the wrong end is refused, and a refused write must not be applied:

```json
← { "type": "error", "code": "not_head", "head": "n1" }
← { "type": "error", "code": "not_tail", "tail": "n3" }
```

Nodes may only send messages to their neighbours in the chain (replies to clients excepted: the tail answers the client directly).

## The one idea

Everything the tail has, every node has. So the tail can answer reads alone and they are still **linearizable**: once a write is acknowledged, every later read sees it, and a read never sees a write that might still vanish. No quorum, no coordination at read time.

The cost is that the chain is only as reliable as its weakest link. Every hop must deliver every write, once, in the head's order, even when messages are lost or overtake each other.

## The tests check

- `write_ok` comes from the tail, for every write sent to the head
- tail reads see every acknowledged write and never go backwards
- requests at the wrong end get the errors above
- messages travel only between neighbours
- chains of three to five nodes, with uneven and lossy links
