A client sends a request and hears nothing back. Was the request lost, or only the reply? The client cannot tell, so it does the only sensible thing: it sends the request again.

That makes **duplicates** a fact of life for every server. If the request was "add 5", applying it twice is a bug the client can never detect.

## What you build

A counter that survives retries.

```json
→ { "type": "add", "delta": 5, "request_id": "req-12" }
← { "type": "add_ok" }

→ { "type": "read" }
← { "type": "read_ok", "value": 137 }
```

A retried request carries the **same `request_id`** as the original. About half of all requests in the test arrive two or three times.

## The idea

An operation is **idempotent** if doing it twice has the same effect as doing it once. "Set x to 5" is idempotent; "add 5 to x" is not. A unique request id turns any operation into an idempotent one: remember which ids you have already applied, and skip the work the second time.

Skip the work, not the answer. The client retried because it is still waiting; it needs its `add_ok`.

## The tests check

- every copy of every request is acknowledged
- the final value counts each `request_id` exactly once
