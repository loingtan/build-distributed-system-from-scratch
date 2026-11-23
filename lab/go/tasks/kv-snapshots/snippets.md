```go
n.Disk().Update(func(tx *quorumlab.Tx) error {
	tx.Set("snap", Snapshot{Index: 120, Term: 4, Store: store, Sessions: sessions})
	return tx.Set("log", log[121-firstIndex:]) // the log now starts with entry 121
})
```
