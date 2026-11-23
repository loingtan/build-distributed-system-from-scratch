```go
reply, err := n.Call(ctx, peer, map[string]any{"type": "gossip", "message": 7}, 300*time.Millisecond)
// err == quorumlab.ErrTimeout if nothing came back in 300ms; otherwise reply is the answer
```
