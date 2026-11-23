Fencing needs a resource that checks tokens. Many cannot: a printer, an email gateway, an old API. Then the only thing keeping two holders apart is **timing**: each holder must stop acting before its lease ends, and its idea of "before" must be right.

Two mistakes are common. Comparing the lock server's timestamp with your own clock is wrong whenever the two clocks differ, and they always do. Counting the lease from when the grant *arrived* is wrong when the reply was slow, or your process was paused as it arrived: the lease began when the server granted it, earlier. Count from when you **sent** the request instead; the real lease cannot end before that estimate. Then stop a **safety margin** early, because the request you send still has to travel.

## What you build

Workers n1, n2 and n3. The lock and the printer are services run by the test.

```json
→ n2 { "type": "job", "job": "j4", "pages": 6 }          ← { "type": "job_ok", "done": true }
→ lock { "type": "acquire" }
← { "type": "acquire_ok", "granted": true, "lease_ms": 300, "expires_at": 1234 }
→ printer { "type": "print", "job": "j4", "page": 1 }    ← { "type": "print_ok" }
→ lock { "type": "release" }                             ← { "type": "release_ok" }
```

- The lock grants a **300ms** lease if it is free or you already hold it (then it is extended to 300ms from now). `expires_at` is on the lock's clock.
- Print pages 1 to `pages` in order, one at a time; the printer answers after about 40ms. A job outlives one lease, so acquire again (as the holder) when your lease is running out.
- Every page must **reach** the printer while you hold the lease. Stop at least **50ms** before your estimate of its end.
- Clocks are off by up to 400ms, some grants take an extra 150ms, and workers are paused for 450ms as a grant arrives. Use 1000ms timeouts for calls to the lock.

## The tests check

- every page reaches the printer while its sender holds the lease
- every job is done and all its pages printed
