Almost every service depends on another one: a database, a payment provider, a team's internal API. Sooner or later that dependency stops answering. It does not send an error. It just goes quiet. A service that waits politely goes quiet too, and so does everything that depends on it.

In this track your node is a small service in front of an **upstream**. Clients ask your node, and your node asks the upstream.

## What you build

A client asks for a key:

```json
→ { "type": "get", "key": "k7" }
← { "type": "get_ok", "value": "k7/s123" }
```

To answer, call the upstream with `node.rpc('upstream', ...)` and pass its value on:

```json
→ { "type": "fetch", "key": "k7" }
← { "type": "fetch_ok", "value": "k7/s123" }
```

On a good day the upstream answers within **150ms**. On a bad day it takes up to a second, or never answers. Every client request must still get **exactly one reply within 300ms**. If the upstream has not answered in time, give up and say so:

```json
← { "type": "error", "code": "timeout" }
```

## The idea

A **deadline** is a promise to your caller: you will hear from me by then. Every call you make on the way must fit inside it. Here that means the trip from the client (up to 12ms), the upstream call, and the trip back (up to 12ms). So the upstream timeout must be longer than the upstream's slowest normal answer, about 175ms seen from your node, and short enough to leave time for the reply. Around 200ms works.

A timeout does not tell you the upstream failed, only that you stopped waiting. That matters in the next task.

## The tests check

- every request gets one reply, within 300ms
- answers the upstream gave within 200ms are passed on with the upstream's value
- calls the upstream answered late, or never, become `{ "type": "error", "code": "timeout" }`
- the same holds while the upstream is completely dark for 1.5 seconds
