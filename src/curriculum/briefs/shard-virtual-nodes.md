The ring from the last task moves few keys, but it shares them badly. Six members at six hashed positions cut the circle into six arcs of whatever length the hash happens to give. In the starter one member owns about 1% of the keys and another over 40%. The busiest node sets the limit for the whole cluster, so an uneven split wastes most of the hardware.

There is a second problem. When a member leaves, its whole arc lands on one neighbour, which may double that neighbour's load.

## What you build

The protocol is unchanged from the previous task:

```json
→ { "type": "set_members", "members": ["n1", "n2", "n4", "n5", "n6"] }
← { "type": "set_members_ok" }

→ { "type": "lookup", "keys": ["user:1000", "user:1001"] }
← { "type": "lookup_ok", "owners": ["n4", "n2"] }
```

`put`, `get` and `local_keys` also work as before. What changes is how even the shares are.

## The idea: many points per node

Put each member on the ring many times instead of once, for example at `hash(id + "#" + i)` for `i` from 0 to 199. These points are called **virtual nodes**. Each one still stands for its real node; a key belongs to the real node behind the first point at or after it.

A member's share is now the sum of many small arcs. One random arc can be far from average, but the sum of a couple of hundred is close to it: the spread shrinks roughly with the square root of the number of points. And when a member leaves, its small arcs fall to many different neighbours, so the extra load is shared too.

A ring of over a thousand points is searched for thousands of keys per `lookup`, so keep it sorted and use binary search.

## The tests check

- with 4,000 keys, every member owns between **0.7× and 1.3×** its fair share (keys ÷ members), for every member list from six nodes down to three. About 100 points per node is borderline; 150 or more is comfortable
- as before: owners are members, all nodes agree, and a key changes owner only if its old owner left or its new owner just joined
- keys written at the end are stored on the node `lookup` names
