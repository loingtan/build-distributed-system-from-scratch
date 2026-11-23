Most databases you have used replicate this way: one node, the **primary**, decides the order of all writes, and the other nodes, the **backups**, copy what it did. PostgreSQL streaming replication, MySQL replicas and Redis replicas all start here. Backups add copies of the data and spread the read load, but they do not make decisions.

In this track the primary is fixed: it is `node.nodeIds[0]`, always. Nobody elects anything.

## What you build

A key-value store. Clients may send writes and reads to **any** node:

```json
→ { "type": "write", "key": "k3", "value": 7 }
← { "type": "write_ok" }

→ { "type": "read", "key": "k3" }
← { "type": "read_ok", "value": 7 }
```

A missing key reads as `null`. Only the primary may accept a write. A backup that receives one passes it to the primary and gives its client the primary's answer.

Replication is **asynchronous**: the primary applies the write, answers the client at once, and only then sends the write on to the backups. Every node answers reads from its own copy, so a backup's answer can be a little out of date. That is allowed. Two things are not:

- a node must never answer with an older value than one it has already given for the same key
- once writes stop, every node must hold exactly what the primary holds, within about a second

## The one idea

The network does not keep messages in order. If the primary sends "k3 = 7" and then "k3 = 8", a backup may get them the other way round and finish on 7. A backup cannot decide which is newer from arrival order. The primary is the one place that knows the order, so the primary has to write it down in the messages.

## The tests check

- writes are acknowledged, wherever they were sent
- reads are answered, from values that were actually written
- no node ever goes back to an older value
- every node converges on the primary's data, including while a backup is cut off for good
