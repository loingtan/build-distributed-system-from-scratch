Some upstreams fail not because they are broken but because they are asked too much: a partner API with a quota, a database that slows down sharply past a certain load. The fix is to decide in advance how much you will send, and to refuse the excess yourself, quickly and cheaply, instead of passing it on.

## What you build

One node, the same service as in the first task, with an admission check in front of the upstream call. It admits at most **20 requests per second on average, with bursts of up to 10**. A request that is not admitted gets an answer at once and never reaches the upstream:

```json
→ { "type": "get", "key": "k42" }
← { "type": "error", "code": "rate_limited" }
```

## The token bucket

Picture a bucket that holds at most **10 tokens** and **starts full**. Tokens drip in at **20 per second**. Each admitted request takes one token; with no whole token in the bucket, the request is refused.

You do not need a timer to drip tokens in. Store the token count and the time you last updated it. On each request, add the elapsed time × 20 / 1000, cap the result at 10, and keep the fraction.

The bucket allows short bursts, since a quiet period refills it, while holding the long-run rate to 20 per second. Over any stretch of *t* seconds it releases at most 10 + 20 × *t* requests.

## The tests check

- in **every** window of time, upstream calls never exceed 10 + 20/s × the window length (plus one call of slack)
- at least **85%** of the requests a perfect bucket would have admitted succeed; under the limit, that means nearly all of them
- refused requests get `{ "type": "error", "code": "rate_limited" }` within **50ms**, with no upstream call
- this holds under steady overload (60 per second) and under bursts of 15 and 30 requests at one instant
