```go
found, err := n.KV().Read(ctx, key, &v)     // found is false if the key is missing
err = n.KV().Write(ctx, key, value)
ok, err := n.KV().CAS(ctx, key, from, to)   // true if it held `from` and now holds `to`
```
