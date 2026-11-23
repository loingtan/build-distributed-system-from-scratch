Some changes cannot be made by adding a field. Version 2 of the directory forwards in a new way: the home answers the client itself, which saves a hop. Version 1 cannot take part, because it would not know what to do with such a message. So a node may use the new format only with a peer it **knows** runs version 2.

The service and the rollout are the same as in the previous task. A key's home is `node.nodeIds[sum of its character codes % node count]`. The harness drains a node (no new traffic involving it from 250ms before), stops it, sets `node.disk.version = 2` and restarts it 100ms later, one node at a time. Leave `runVersion1` as it is.

## What you build

To a peer known to run version 2, a node forwards a client request as

```json
{ "type": "fwd2", "op": "get", "key": "u7", "client": "c1", "reply_to": 41 }
{ "type": "fwd2", "op": "put", "key": "u7", "name": "Ada Lovelace", "client": "c1", "reply_to": 42 }
```

where `client` and `reply_to` are the client's `msg.src` and `msg.body.msg_id`. The forwarding node does not reply. The home sends the answer straight to the client:

```js
node.send(client, { type: 'get_ok', name, in_reply_to: reply_to });
```

With every other peer, keep using `fwd_put` / `fwd_get`.

How a node finds out which peers run version 2 is up to you: a handshake when it starts, or a version field on the messages it already sends. A node that restarts has forgotten everything it learned, and peers are upgraded after it as well as before.

## The one idea

The sender has to know what the receiver understands before it uses anything new. Long-lived protocols such as TLS, HTTP/2 and Kafka's ApiVersions agree on a version before they use it.

## The tests check

- not one client request fails during the rollout, and gets return the latest name
- a version-1 node never receives a `fwd2`
- from 500ms after the last node restarts, every forward between nodes is a `fwd2`
- version-1 nodes send exactly what version 1 sends
