A lease keeps a dead client from holding a lock forever. It does not stop a client that is merely **paused**. A garbage-collection pause or a VM migration can freeze a program for longer than its lease. When it resumes it still believes it holds the lock and carries on writing, though another client has taken the lock meanwhile. A slow message does the same.

No check by the client can prevent this: the pause can fall between the check and the write. The resource has to refuse the stale request. That is what a **fencing token** is for: a number that grows with every grant. The storage remembers the highest token it has seen and refuses anything lower.

## What you build

n1 is the lock server from LS-1 (same rules), now with tokens. n2 holds one list. Workers n3 to n5 each get one job at a time and append it to the list under the lock.

```json
→ n3 { "type": "job", "job": "j7" }                     ← { "type": "job_ok", "done": true }
→ n1 { "type": "acquire" }      ← { "type": "acquire_ok", "granted": true, "lease_ms": 300, "token": 12 }
→ n2 { "type": "read", "token": 12 }                    ← { "type": "read_ok", "entries": ["j1", "j3"] }
→ n2 { "type": "write", "token": 12, "entries": ["j1", "j3", "j7"] }   ← { "type": "write_ok" }
→ n2 { "type": "inspect" }                              ← { "type": "inspect_ok", "entries": [...] }
```

- A worker acquires (retrying for up to 1.5s), reads, writes, releases, then answers `done: true`, or `done: false` if it could not finish.
- Every new grant gets a token higher than any before; the holder acquiring again keeps its token.
- n2 refuses (`{ "type": "rejected" }`) any `read` or `write` whose token is lower than the highest it has seen. `inspect` comes from the test and is always answered.
- Use 1000ms timeouts for calls to n2; pauses last 700ms or 450ms.

## The tests check

- no job answered `done: true` is missing from the list at the end
- each new grant has a higher token
- jobs that no fault touched succeed
