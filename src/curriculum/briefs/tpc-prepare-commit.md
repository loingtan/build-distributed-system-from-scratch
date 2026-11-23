A bank keeps its accounts on several machines. Moving money from an account on one machine to an account on another means two writes on two machines, and they must both happen or neither: money that leaves one account without arriving at the other is gone. This guarantee is called **atomic commit**.

## What you build

Every node holds two accounts, `a` and `b`, starting at **100** each. A client sends a `transfer` to any node. That node is the **coordinator**; the nodes named in the moves are the **participants** (the coordinator may be one of them).

```json
→ { "type": "transfer", "txn": "t7",
    "moves": [ { "node": "n1", "account": "a", "delta": -30 },
               { "node": "n3", "account": "b", "delta": 30 } ] }
← { "type": "transfer_ok", "txn": "t7", "committed": true }
```

A move that would take an account **below zero** makes the whole transfer fail: reply `committed: false` and change nothing anywhere. Answer within **one second**.

The tests read each node's state with:

```json
→ { "type": "status" }
← { "type": "status_ok", "balances": { "a": 70, "b": 100 }, "committed": ["t7"] }
```

`committed` lists the transactions this node has applied. Each balance must equal 100 plus the moves of those transactions.

## Two-phase commit

The starter tells each participant to apply its moves at once. If one refuses, the other has already applied. The fix is to separate deciding from doing:

1. **Prepare.** The coordinator asks every participant whether it can apply its moves. Each checks and votes yes or no, and changes nothing.
2. **Commit or abort.** Only if every vote is yes does the coordinator send commit; otherwise abort. Participants apply on commit and forget on abort.

The message names between nodes are yours to choose.

## The tests check

- every participant applied a transaction, or none did
- replies match what the nodes did, and no money is created or destroyed
- a transfer the accounts can afford commits; one they cannot afford does not
