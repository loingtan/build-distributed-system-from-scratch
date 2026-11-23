One machine can only hold so much data and serve so many requests. The way past that limit is to split the keys between several nodes, so each node stores and serves only a part. This is called **partitioning**, or **sharding**.

The starter keeps a key on whichever node the client happened to talk to. Write `user:7` through n1, read it through n2, and it is not there.

## What you build

A key-value store where a client may send any request to any node.

```json
→ { "type": "put", "key": "user:1042", "value": 17 }
← { "type": "put_ok" }

→ { "type": "get", "key": "user:1042" }
← { "type": "get_ok", "value": 17 }
```

A `get` for a key that was never written returns `"value": null`.

Each key has exactly **one owner node**, and only the owner stores it. A node that receives a request for a key it does not own forwards the request to the owner, waits for the answer and relays it to the client. The messages between nodes are yours to design.

The tests look inside each node with one more request. It lists the keys stored on that node itself:

```json
→ { "type": "local_keys" }
← { "type": "local_keys_ok", "keys": ["user:1042", "user:1077"] }
```

## The idea

Every node must be able to work out the owner of a key by itself, and all nodes must reach the same answer. So the owner can only depend on things every node already knows: the key and the list `node.nodeIds`.

The starter gives you `hash(str)`, which turns a string into a number from 0 to 2^32 − 1, the same on every node. A hash scatters similar keys, so the keys spread evenly over the nodes.

## The tests check

- every `put` and `get` is answered within 1000ms, with the right reply type
- a `get` through any node returns the last value written, whichever node took the `put`
- every written key is listed by exactly one node's `local_keys`
- each node stores between 0.5× and 1.5× its fair share of the keys
- at most 2 messages between nodes per client request, on average
