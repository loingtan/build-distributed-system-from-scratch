Counters were the first replicated data type. The next is a set: tags on a photo, users who liked a post, nodes that have joined a cluster. Any node accepts an add on its own, even when it cannot reach anyone else, and every node must still end up with the same set.

The starter stores each add and sends it to every peer once. Run it: it passes on a healthy network and loses elements as soon as messages are dropped.

## What you build

```json
→ { "type": "add", "element": "e7" }
← { "type": "add_ok" }

→ { "type": "read" }
← { "type": "read_ok", "value": ["e1", "e7", "e3"] }
```

- `add` is acknowledged at once, partition or not. Adding an element that is already there is fine and changes nothing.
- `read` returns this node's current set, in any order. It may be behind while the network is broken, but it must include every element this node has acknowledged, and it never loses an element.
- Once the network settles, every node must return exactly the acknowledged elements. Allow at most about **one second** after a heal for this.

## The idea: anti-entropy

The state of a grow-only set (**G-Set**) only ever gets bigger, and two states merge by **union**. Union can be applied in any order, any grouping, and any number of times with the same result. So there is no need to track which message reached whom: every node keeps sending its whole set to its peers on a timer, and each peer unions what it receives. A lost message is repaired by the next round; a repeated one changes nothing.

This background exchange is called **anti-entropy**. It is the backbone of every task in this track.

## The tests check

- healthy network, a partition of five nodes, and links that lose a quarter of messages
- adds are acknowledged during the partition
- a node reads its own adds and never loses an element
- every node converges on exactly the acknowledged elements
