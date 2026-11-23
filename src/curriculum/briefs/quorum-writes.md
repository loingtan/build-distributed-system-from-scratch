A database with one leader has one place where writes can stall. Amazon's Dynamo, and Cassandra and Riak after it, removed the leader: every node accepts writes, and a write counts as done when *enough* replicas have it.

## What you build

A key-value store in which every node is a replica of every key. A client may send a `put` to any node. That node becomes the **coordinator** for the request:

```json
→ { "type": "put", "key": "k1", "value": "apple" }
← { "type": "put_ok" }
```

The coordinator sends the write to **all N** replicas and answers `put_ok` once **W** of them have stored it. It counts as one of them. Here N is the cluster size and W is a majority: `Math.floor(N / 2) + 1`.

If W replicas cannot be reached, the coordinator must say so, within **800ms**:

```json
← { "type": "error", "text": "only 2 of 3 replicas acknowledged" }
```

The tests look inside single replicas with one extra request, answered from local state only:

```json
→ { "type": "local_get", "key": "k1" }
← { "type": "local_get_ok", "value": "apple" }
```

`value` is `null` for a key this replica does not hold. The messages between nodes are yours to design. Each key is written once in this task.

## The idea

W is a dial between safety and availability. Waiting for one replica is fast and loses the write if that replica dies. Waiting for all N means a single dead node blocks every write. With a majority, the write survives any minority of failures and the cluster keeps working through them.

An `error` does not mean the write left no trace. The replicas that were reached keep it. A leaderless store has no rollback, only "not enough copies yet".

## The tests check

- every request is answered, with `put_ok` or `error`
- straight after a `put_ok`, at least W replicas hold the value
- puts succeed while the coordinator can reach W replicas, with two of five nodes partitioned away or crashed
- puts are refused by a coordinator that can reach fewer than W
- on a healthy network every replica ends up with every write
