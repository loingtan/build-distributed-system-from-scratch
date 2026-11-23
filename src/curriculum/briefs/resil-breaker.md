When the upstream is down for minutes, timeouts and retries are not enough. Every request still waits 200ms to learn what the last hundred requests already learned, and the upstream, trying to restart, is hit with a steady stream of calls it cannot answer. A **circuit breaker** notices the pattern and stops calling for a while.

## What you build

The same service, with a breaker in front of the upstream call (no retries in this task). It has three states:

- **Closed**: call the upstream as normal and count **failures in a row**. A failure is a timeout (200ms) or any error reply. A success resets the count. After **5 failures in a row**, open.
- **Open**: do not call. Reply at once:

  ```json
  ← { "type": "error", "code": "circuit_open" }
  ```

  After a **cool-down of 1000ms**, the next request becomes a probe and the breaker is half-open.
- **Half-open**: exactly one probe is out; refuse everything else. If the probe succeeds, close. If it fails, open again and restart the cool-down.

Each node keeps its own breaker. Two nodes share the traffic, about 20 requests per second in all.

## The idea

A breaker trades a few refused requests for two things: fast answers while the upstream is down, and quiet for the upstream so it can recover. The probe is how the breaker finds out the outage is over without letting the full load back in at once.

## The tests check

- every request gets a reply within 300ms; successes carry the upstream's value
- scattered failures (never two in a row) do not open the breaker: while the upstream is healthy, requests succeed
- during an outage the upstream receives **at most 30 calls** from both nodes together
- from 1s into an outage, at least **80%** of requests are refused within **50ms**
- once the upstream recovers, requests succeed again within **2000ms**
- this works for an outage that is silent (3s) and for two outages that answer `busy` (2s each)
