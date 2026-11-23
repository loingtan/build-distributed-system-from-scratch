So far each node finds a crash on its own, the next time it happens to probe the crashed node. In a cluster of twelve that can take a dozen periods, and different nodes hold different opinions for seconds. A membership service has to give everyone the **same** view, quickly.

## What you build

When a probe fails (direct and indirect), declare the target **`dead`**, stop probing it, and spread the news. Not with extra messages: SWIM **piggybacks** membership updates on the pings and acks it is already sending.

Every node keeps a small buffer of recent updates. Each outgoing `ping`, `ack`, `ping_req` and `ping_req_ok` carries a few of them, least-sent first:

```json
→ { "type": "ping", "updates": [{ "id": "n7", "status": "dead" }] }
← { "type": "ack",  "updates": [{ "id": "n3", "status": "dead" }] }
```

When you receive an update you did not already know, apply it and put it in **your** buffer too. Drop an entry from the buffer once it has been sent about **3 · log₂(N + 1)** times (12 for twelve nodes).

`dead` is final in this task: nothing turns a dead node back into a live one.

## The one idea

This is **infection-style dissemination**: every node that knows an update tells one or two more each period, so the number who know roughly doubles every round, and the whole cluster knows in O(log N) periods. It costs no extra messages, and losing a few changes little because every update travels on many of them.

## The tests check

Twelve nodes, 200ms periods; one test adds 2% message loss and a second crash.

- a crashed node is reported `dead` by every other node within **3 seconds** of the crash
- once any node reports it `dead`, every node does within **2 seconds** (10 periods)
- after a 600ms warm-up, no node that never crashed is ever reported `dead`
- at most **30 node messages per node per second**: spreading news must not add messages
