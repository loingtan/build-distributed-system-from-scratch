```go
var log []Entry
n.Disk().Get("log", &log)   // empty on first start, the old log after a restart
var term int
n.Disk().Get("term", &term) // 0 if never saved
```
