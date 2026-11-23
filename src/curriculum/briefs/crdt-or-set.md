A set people can only add to is rarely enough. A shopping list needs remove; so does a set of active sessions. The starter is your G-Set with a `remove` that deletes the element locally. Run it: removed elements come straight back, because the next merge from a peer that still has the element unions it in again.

## What you build

```json
→ { "type": "add", "element": "milk" }
← { "type": "add_ok" }

→ { "type": "remove", "element": "milk" }
← { "type": "remove_ok" }

→ { "type": "read" }
← { "type": "read_ok", "value": ["eggs"] }
```

All three are answered at once, partition or not. Removing an element that is not present is allowed and has no effect. The rules, which the tests check exactly:

1. **A remove removes what its node has seen.** Every add of the element that had reached this node before the remove is undone, on every replica, and stays undone after any partition heals.
2. **An add concurrent with a remove wins.** If one node adds an element while another removes it, and neither had seen the other's operation, the element is present afterwards.
3. **Re-adding works.** An add made after a remove makes the element present again.

Replicas must converge within about **one second** of the network settling.

## Why the obvious fixes fail

Remembering removed *elements* in a second grow-only set (a **2P-Set**) makes removes stick, but forever: rule 3 fails, and so does rule 2.

The fix is to remove something finer than the element. Give every add a **unique tag**. A remove records, as **tombstones**, the tags its replica holds for that element at that moment. An element is present while it has a tag that is not tombstoned. Tags and tombstones only grow, so both merge by union. This is the **observed-remove set** (OR-Set).

## The tests check

- three and five replicas, lossy links and slow links, each with a partition in the middle
- removes stick, including one made on one side of a partition
- an element can be added again; an add concurrent with a remove survives
- a remove covers every add it has seen; all replicas converge
