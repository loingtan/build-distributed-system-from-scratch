Many failures are brief: a lost packet, a server restarting, a moment of overload. Trying again a little later usually works. But retries are also extra load, and they arrive exactly when the upstream is struggling. Done carelessly, retries turn a small hiccup into an outage.

## What you build

The same service as before, with the same 200ms upstream timeout. Now the upstream is flaky. A call can:

- succeed: `{ "type": "fetch_ok", "value": … }`
- be lost: no answer, so `node.rpc` resolves to `null`
- be refused because the upstream is overloaded: `{ "type": "error", "code": "busy" }`
- fail for good: `{ "type": "error", "code": "not_found" }`

Silence and `busy` are **transient**: retry them. Anything else is **permanent**: asking again gives the same answer, so pass it straight on as `{ "type": "error", "code": "not_found" }`. If every attempt fails, reply:

```json
← { "type": "error", "code": "unavailable" }
```

## The rules

- At most **4 upstream calls per request**: one first try and up to three retries.
- Wait between attempts, and let the wait **grow**: each one should average at least 1.5 times the one before. Doubling works: ceilings of 100, 200 and 400ms.
- Make each wait **random**: `node.random() * ceiling`. This is called **jitter**.
- Every reply within **2000ms**, retries included.

## The idea

Picture 24 requests that all fail at the same instant. With a fixed wait they all retry at the same instant too, and the upstream faces the same spike again, and again. Jitter spreads them out so the upstream recovers between attempts. Backoff gives it more time each round. The cap stops a dead upstream from receiving four times the normal traffic for ever.

## The tests check

- with 30% of calls failing at random, at least 95% of requests for existing keys succeed
- `not_found` is passed on after one call, never retried
- no request causes more than 4 upstream calls
- when the 24 requests of a burst fail together (the test sends two bursts and pools them), their first retries are spread over at least 25ms, and second waits average at least 1.5 times the first
