Almost every system needs identifiers: for orders, for messages, for rows. The obvious answer is a counter, and the starter code uses one. Run it and look at what three nodes do with the same counter.

## What you build

A node that answers `generate` with an ID no other node will ever produce.

```json
→ { "type": "generate" }
← { "type": "generate_ok", "id": "anything-unique" }
```

The `id` can be a string, a number or an array, as long as it is unique across the whole cluster.

## The catch

During one test the network **partitions**: some nodes cannot reach the others for two seconds. Requests keep arriving on both sides, and every one must still be answered.

That rules out asking a central node for the next number. Each node has to mint IDs entirely on its own. This property is called **total availability**: any node that is up can answer, whatever the network is doing.

## The tests check

- every request is answered, including during the partition
- no ID is ever handed out twice, by one node or by two
