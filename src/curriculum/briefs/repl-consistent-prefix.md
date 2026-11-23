Now drop the single primary: **every node accepts writes** and replicates them to the others, as databases spread across regions do. Each key in this task is written exactly once, so writes never conflict (that is the next task). Something subtler goes wrong instead.

Alice asks a question: `q7 = "Q7"`, written through n1. Bob reads it and answers: `a7 = "A7"`, written through n2. The link n2 → n3 is fast, the link n1 → n3 slow. If n3 applies writes as they arrive, a reader there sees the answer to a question that does not exist yet. DDIA calls the guarantee against this **consistent prefix reads**: whoever reads writes that are causally related sees them in the order they happened.

## What you build

The same store and the same token protocol as RP-5: every request carries `token` (`null` at first), every `write_ok` and `read_ok` carries one, and the client keeps the latest. Any node may take any write.

```json
→ { "type": "write", "key": "a7", "value": "A7", "token": { "n1": 4, "n2": 2, "n3": 0 } }
← { "type": "write_ok", "token": { "n1": 4, "n2": 3, "n3": 0 } }

→ { "type": "read", "key": "q7", "token": null }
← { "type": "read_ok", "value": "Q7", "token": { "n1": 4, "n2": 3, "n3": 1 } }
```

A node behind the client's token waits until it has caught up (give up after about 600ms with `error`). Nothing is lost on the network in this task, but links add 0–260ms of delay, different for each direction.

## The one idea

A write depends on everything its writer could have seen. Record that with the write: name each write by its origin and number there, and send along the origin's `applied` vector as its **dependencies**. A replica applies a write only after its origin's earlier writes and all its dependencies. The same vector is a good token.

## The tests check

- writes and reads are answered; reads come from real writes; replies carry a token
- no node shows an answer and later says its question is missing
- no client sees an answer and then fails to find its question, on any node
- every node ends up with every write
