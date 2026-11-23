Round robin assumes the backends are alike. They rarely are. One runs on an older machine, one is busy with garbage collection, one shares its host with a noisy neighbour. Round robin still gives the slow one a full share, so a quarter of all clients wait on it while the other backends sit idle. Your **tail latency**, the time the slowest requests take, is set by the worst backend.

## What you build

The same balancer, with the same messages:

```json
→ { "type": "set_backends", "backends": ["b1", "b2", "b3", "b4"] }
← { "type": "set_backends_ok" }
→ { "type": "get", "key": "r42" }
← { "type": "get_ok", "key": "r42", "value": "r42@b1#s123", "backend": "b1" }
```

What changes is how you choose. Send each request to the backend with the **fewest requests in flight**: sent by you and not yet answered.

Nobody tells you which backend is slow, and it can change during the run. In the tests, fast backends take 10–40ms; a slow one takes 250–400ms.

## The idea

A slow backend holds each request longer, so at any moment more of its requests are still in flight. Picking the backend with the fewest sends it fewer new requests, automatically, with no timing or measuring. When it speeds up again, its count drops and it gets its share back.

Count a request the moment you send it, not when it comes back. When 12 requests arrive in the same millisecond, the second must already see the first.

## The tests check

- as before: one reply per request within 1000ms, the backend's answer, one backend per request, only pool members
- from 200ms after a backend turns slow until it recovers, it receives **at most 12%** of requests (round robin gives it 25%), and **90%** of requests in that time are answered within **150ms**
- from 500ms after it is fast again, it receives **at least 15%**
- when 12 requests arrive together, **no backend gets more than 5** of them
