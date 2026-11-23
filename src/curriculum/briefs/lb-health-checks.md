Backends fail. A crashed one answers nothing, so every request sent to it waits for a timeout. Worse is one that **fails fast**, answering every request with an error in a millisecond. To least-outstanding routing it looks perfect, never busy, so it gets almost all the traffic. One broken machine can fail most of your requests.

A load balancer has to notice failures, stop sending traffic to failed backends and bring them back when they recover.

## What you build

The balancer from the least-outstanding task (four backends, the same `set_backends` and `get`), plus **active health checks**. Every backend answers this:

```json
→ { "type": "health" }
← { "type": "health_ok" }
```

A healthy backend answers a health check within about **40ms** and a `get` within about **100ms**. A dead one answers nothing. A broken one answers everything at once with:

```json
← { "type": "error", "code": "unavailable" }
```

Probe every backend regularly; every 200ms with a 150ms timeout works. Take a backend out after two failed probes in a row, and put it back after one good one. Keep probing the backends you took out: that is how they get back in.

Some requests will still be sent to a backend just as it fails. When a forward times out (250ms works) or comes back as anything but `get_ok`, **retry once on a different backend**.

## The idea

A health check asks each backend directly, on your schedule, instead of waiting for real requests to fail. The retry covers requests already in flight when a backend fails.

## The tests check

- every request gets a `get_ok` within **1000ms** (the tests always keep at least three backends healthy, so this is always possible)
- **at most 2** backend calls per request
- no requests go to a failed backend from **1000ms** after it fails until it recovers
- a recovered backend gets a request again within **1000ms**
- outside the 1000ms after each failure or recovery, every healthy backend gets **at least a third** of an equal share
- **at most 10** health checks per second per backend
