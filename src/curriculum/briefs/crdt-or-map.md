A shopping cart maps items to quantities. Two phones on the same account add apples at once; one removes the pears while the other changes the pear count. A **map of CRDTs** handles this by giving each key a replicated value of its own, here a counter, and the keys observed-remove semantics.

The starter nests what you have built: an OR-Set of item names and a PN-Counter per item. Run it: remove an item, add it again, and the old quantity is back.

## What you build

```json
→ { "type": "add", "item": "apples", "qty": 3 }
← { "type": "add_ok" }

→ { "type": "remove", "item": "apples" }
← { "type": "remove_ok" }

→ { "type": "read" }
← { "type": "read_ok", "value": { "pears": 2, "milk": 1 } }
```

`qty` is a non-zero integer and may be negative (take some out). All requests are answered at once, partition or not. The rules:

1. **Quantity changes add up.** The quantity is the sum of every `qty` added, from every node, including changes made concurrently on both sides of a partition.
2. **A remove clears what its node has seen.** The item disappears together with every quantity change that had reached this node. A later add starts again from zero.
3. **An add concurrent with a remove wins.** The item stays, with only the quantity the remove could not have seen.
4. An item is in the cart while it has a change that has not been removed, even if its quantity is 0 or negative.

Replicas must converge within about **one second** of the network settling.

## Why nesting is not enough

Hiding a key does nothing to the value under it. When an inner CRDT can only grow, a remove has to record *which part of it* was cleared, just as an OR-Set remove records the tags it saw. Treat each add as a tagged quantity change; a remove tombstones the changes it has seen.

## The tests check

- concurrent increments and decrements all count
- removes stick, including one made during a partition
- re-adding after a remove starts from zero; an add concurrent with a remove wins
- three replicas, five, lossy links; all replicas converge
