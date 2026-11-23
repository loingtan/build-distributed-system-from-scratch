So far nodes could crash, but never lied. A **Byzantine** node can: through a bug, corrupted memory or an attacker, it sends wrong values, or different values to different peers (**equivocation**). This track tolerates **f = 1** Byzantine node among **n = 4**.

The tests play the traitor. Your code runs on every node, but the network rewrites or drops what the traitor sends. One thing it cannot fake is `msg.src`: links are authenticated.

## What you build

Each node may be asked once to broadcast a string:

```json
→ { "type": "broadcast", "value": "apple" }
← { "type": "broadcast_ok" }
```

Honest nodes must **deliver** the same value for that sender, or all deliver nothing, even if the sender is the traitor. The tests read what each node delivered, by sender:

```json
→ { "type": "read" }
← { "type": "read_ok", "delivered": { "n1": "apple", "n3": "plum" } }
```

## The protocol (Bracha)

Send every message below to all nodes, yourself included.

1. The sender sends `{ "type": "propose", "value": "apple" }`.
2. On the first `propose` from a sender S (S is `msg.src`), send `{ "type": "echo", "sender": "S", "value": "apple" }`. Echo once per sender.
3. On **2f+1 = 3** echoes for the same (sender, value) from distinct nodes, send `{ "type": "ready", "sender": "S", "value": "apple" }`. Once per sender.
4. On **f+1 = 2** readies for the same (sender, value) from distinct nodes, send your ready too if you have not yet.
5. On **2f+1 = 3** such readies, deliver the value.

Why 3 echoes? Two groups of 3 among 4 nodes share 2 nodes, at least one honest, and an honest node echoes one value only, so two values can never both reach 3. Step 4 makes delivery all-or-nothing: 2 readies include an honest node, so joining in is safe.

## The tests check

- an honest sender's value is delivered by every honest node within about 1 s
- honest nodes never deliver different values for one sender
- if one honest node delivers for a sender, every honest node does
- nothing is delivered for a node that never broadcast
