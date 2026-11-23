A backend that has just started is not at full strength: its caches are empty and its runtime has not optimised the hot code yet. It answers health checks, even quickly, but each request costs it, and the database behind it, far more than usual. Send it a full share straight away and it can fall over again.

Your health-checking balancer makes this worse. A backend that comes back has nothing in flight, so least-outstanding routing hands it a full share at once.

## What you build

The balancer from the previous task, with **slow start**. When a backend comes into service, give it a **weight** that grows in a straight line from **0 to 1 over 3000ms**, and send it about that fraction of an equal share. A backend comes into service when:

- a health check puts it back after an outage, or
- a later `set_backends` adds it to the pool.

The backends in the very first `set_backends` are warm: no ramp for them.

```json
→ { "type": "set_backends", "backends": ["b1", "b2", "b3", "b4", "b5"] }
← { "type": "set_backends_ok" }
```

## The idea

Least outstanding cannot do this on its own, because an idle backend always looks like the best choice. Choose in two steps instead. Send the request to a warming backend with probability `weight / (sum of all weights)`, and otherwise pick among the warm backends by fewest in flight.

## The tests check

Times are measured from the earliest moment your node could know the backend is usable: the first health check it answers after recovering, or the `set_backends` that adds it. Traffic is 120 requests per second.

- in the first **1.5s**: **at most 0.6** of an equal share
- from 1.5s to 3s: **at least 0.25** of an equal share (it is ramping, not waiting)
- from 3.5s to 5.5s: **at least 0.5** of an equal share (the ramp is over)
- its first request comes within **2000ms**
- everything from the previous task still holds: every request succeeds, failed backends are taken out within 1000ms, at most 10 health checks per second
