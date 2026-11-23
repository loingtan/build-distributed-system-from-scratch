With asynchronous replication, `write_ok` only means "the primary has it". A client that writes through one node and then reads from another can miss its own write, and if the primary dies, acknowledged writes can be lost with it. **Synchronous** replication changes the promise: `write_ok` means every node has the write.

## What you build

The same store and the same messages as before, with a stronger rule:

```json
→ { "type": "write", "key": "k3", "value": 8 }
← { "type": "write_ok" }
```

The primary may answer `write_ok` only after **every backup has confirmed** that it applied the write. From then on, a read sent to **any** node must return that value or a newer one. The tests read the key from every node right after each `write_ok`.

If a backup cannot be reached, the primary cannot keep its promise. It must not wait forever, and it must not pretend. Give up after about **500ms** and answer:

```json
← { "type": "error", "code": "unavailable" }
```

Clients wait 1s for any reply, including replies relayed through a backup.

## The one idea

Every guarantee has a price. Here the price is **availability**: one unreachable backup makes every write fail, even though the primary itself is fine. The partition test shows it. While the last node is cut off, writes fail; when the network heals, they succeed again. Real systems soften this by waiting for one backup, or a majority, instead of all of them, which weakens the promise accordingly.

Lost messages are a different matter. A backup that did not confirm may simply not have received the request, or its reply was lost. Send it again; make sure that applying the same write twice, or an old write late, does no harm.

## The tests check

- every write gets an answer: `write_ok` or an error
- an acknowledged write is visible on every node straight away
- at least 80% of writes succeed when 15% of messages are lost
- during a partition writes fail honestly, and afterwards they work again
- no node goes back to an older value
