A missing ack tells you less than it seems. The target may have crashed, or the ping or the ack may have been lost, or just the link between *you* and the target may be bad while everyone else reaches it fine. Suspecting a live node is expensive: in a real system it moves traffic and data around for nothing.

SWIM's answer is to get a second opinion before suspecting anyone.

## What you build

Keep last task's probing. When a direct ping gets no ack, pick **k = 3** other members at random (never the target) and ask each to ping the target for you:

```json
→ { "type": "ping_req", "target": "n4" }
← { "type": "ping_req_ok", "ok": true }
```

A helper pings the target itself and reports whether it got an ack. Mark the target `suspect` only if **no** helper got one. Send the requests in parallel, and keep the whole probe inside the 200ms period: for example, a 60ms timeout on the direct ping and 120ms on the `ping_req`s.

`members` works exactly as before.

## The one idea

A failure that only one path sees is a network problem, not a node failure. Asking members who reach the target over **different paths** turns "I cannot reach it" into "nobody can reach it". The cost is a few extra messages, paid only when a ping goes unanswered.

## The tests check

- **One broken link:** two random nodes cannot exchange any message for the whole run (5 nodes). After a 600ms warm-up nobody may report anyone as anything but `alive`
- **Lossy network:** 2% of node messages are lost and a node crashes at 1.5s. Every other node must report it as not `alive` within **3 seconds**, and nobody else may ever be suspected
- at most **30 node messages per node per second**, averaged over the run
