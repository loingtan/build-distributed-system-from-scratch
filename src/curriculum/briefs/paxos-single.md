Raft gets agreement by electing one leader first. Paxos doesn't need a leader at all. Any node may propose a value at any moment, many may do so at once, messages get lost, and still exactly one value is chosen. Every node then learns it. This is the smallest piece of consensus there is, and Multi-Paxos, Chubby and Spanner are all built on it.

## What you build

Every node is a **proposer**, an **acceptor** and a **learner**. A client asks any node to propose a value:

```json
→ { "type": "propose", "value": 42 }
← { "type": "propose_ok", "value": 17 }
```

The reply carries the **chosen** value, which may be someone else's. Reply only once this node knows which value was chosen. The tests also ask every node, every 100ms:

```json
→ { "type": "get" }
← { "type": "get_ok", "value": 17 }
```

`value` is `null` until the node knows. Once it reports a value it must never report another, or `null`.

The node-to-node protocol is up to you. The classic one has two phases, and each needs replies from a **majority** of all nodes, yourself included:

1. `prepare(ballot)`. An acceptor promises to ignore every lower ballot and reports the last `(ballot, value)` it accepted.
2. `accept(ballot, value)`. An acceptor accepts unless it has promised a higher ballot. When a majority accepts, the value is chosen.

## The idea

A value counts as chosen once a majority has accepted it, even if no node knows that yet. So a proposer must never pick its own value blindly. If any promise in its majority reports an accepted value, it proposes the one with the highest ballot instead. Any two majorities share at least one acceptor, and that is why this works.

## Timing

Each proposal must be answered within **2s** once the network is healthy. A client waits 1s for an answer, then tries the next node with the same value. Every node must report the chosen value by 300ms before the end (600ms on the local runner).

## The tests check

- agreement: no two reports of a chosen value ever differ
- validity: the chosen value was proposed by a client
- termination: every proposal gets an answer within 2s, with concurrent proposers and with 20% of messages lost
- a value chosen while only its proposer knows (the tests hide it) still wins against a later proposer
