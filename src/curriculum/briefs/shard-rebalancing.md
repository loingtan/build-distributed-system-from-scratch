So far a membership change only moved *ownership*. The values stayed where they were. Ask the new owner for a key and it has never heard of it, while the old owner sits on data nobody will ask it for again. Moving the data to match the ring is called **rebalancing**, and it is where a sharded store most easily loses writes.

## What you build

The client protocol is unchanged: `put`, `get`, `set_members`, `lookup`, `local_keys`. What is new happens between your nodes. After `set_members`, a node that stores keys it no longer owns must **hand them off** to their new owners. The handoff messages are yours to design, for example:

```json
→ { "type": "handoff", "entries": [["user:1003", 4], ["user:1010", 11]] }
← { "type": "handoff_ok" }
```

A node removed from the members keeps running and must hand over everything it holds.

## What the tests do

1. Optionally set an initial member list, then write 200 keys through random nodes.
2. Send `set_members` to every node and wait for all the replies.
3. In some tests, immediately overwrite 80 of the keys, all at once, and wait for the replies.
4. Wait **1500ms**. Reads are not checked before this, so a `get` during the handoff may miss.
5. Check everything, then go back to step 2 for the next change.

So every node knows the new members before clients write again, and one handoff has settled before the next change starts.

## Two rules

**Delete only after the new owner confirms.** If the handoff message is lost and the old owner has already deleted the key, the value is gone. Keep it until the reply arrives, and retry while it has not.

**A handoff never replaces a value that is already there.** In step 3 a `put` can reach the new owner before the old value does. The value already there is the newer one.

In the last test 10% of messages between nodes are lost. Clients wait 1000ms for a reply, so forwarded `put` and `get` requests need retries as well.

## The tests check

- every `put` and `get` is answered
- after settling, every key reads as its last acknowledged value through any node: none lost, none reverted to an older value
- after settling, each key is listed in `local_keys` by its `lookup` owner and by no other node
- all nodes agree on owners, and owners are members
