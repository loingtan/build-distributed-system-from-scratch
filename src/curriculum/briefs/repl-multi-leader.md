With one leader per region, users write to the nearby node and a network split between regions does not stop anyone from writing. The price is **write conflicts**: two regions can change the same key at the same moment, and each learns of the other's write only later. Unless every node resolves the conflict the same way, the replicas disagree forever.

## What you build

Every node accepts writes and reads for any key, answers from its own copy, and replicates asynchronously. Clients write shared keys through any node, so writes to one key cross in flight.

```json
→ { "type": "write", "key": "k1", "value": "c2:7" }
← { "type": "write_ok", "ts": [12, "n2"] }

→ { "type": "read", "key": "k1" }
← { "type": "read_ok", "value": "c2:7" }
```

The conflict rule is fixed: **last write wins by timestamp**. Each write gets `ts = [counter, nodeId]`, the id of the node that accepted it, and `write_ok` returns it. Of two writes to a key, the one with the higher counter wins; on equal counters, the higher node id, compared as strings. The counter must be a **Lamport clock**: a node adds one for each write it accepts and never lets its counter fall below one it has received, so a write always beats every value its node had already shown. Node wall clocks disagree by up to 400ms in one test, so they will not do.

In one test the last node is cut off for 1.5s. Both sides keep accepting writes, and messages sent in that time are lost; within about a second of the partition healing, every node must hold the same value for every key.

## The one idea

Arrival order differs from node to node; a rule that depends only on the writes themselves does not. If merging picks the same winner whatever order versions meet in, nodes that have seen the same writes hold the same data.

## The tests check

- every write is accepted, on both sides of a partition; reads are answered with real values
- `write_ok` carries a unique `[counter, node]` timestamp
- a new write beats every value of that key its node had already acknowledged or returned
- all nodes converge, on the write with the highest timestamp
