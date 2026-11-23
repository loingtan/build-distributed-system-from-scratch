Your broadcast is correct. It is also expensive. Run the starter and look at the stats: over a hundred node-to-node messages for every value. Each of those messages costs CPU, bandwidth and money.

## What you build

The same system on **nine nodes** with slow links (50–100ms per message), inside a budget:

| | Budget |
|---|---|
| Node-to-node messages per broadcast | at most **6** (acknowledgements count too) |
| Time for a value to reach every node | at most **1.5s** |

A second test brings back the partition, with no budget. Cheaper must not mean less reliable.

## Two levers

**Batching.** One message can carry many values. A node that waits a little before forwarding sends fewer, fuller messages. The cost is latency.

**Topology.** In a full mesh every node tells every other node. You choose who talks to whom: a ring, a tree, a star. Fewer links mean fewer messages, more hops, and a different answer to "what happens when this node is unreachable?"

There is no single right design here. Every choice trades messages against latency against fault tolerance, and the two budgets keep you honest about the first two.

## The tests check

- messages per broadcast and propagation time, on a healthy network
- full delivery after a partition
