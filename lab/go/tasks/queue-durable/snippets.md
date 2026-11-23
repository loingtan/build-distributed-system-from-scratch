```go
var jobs map[string]Job
if ok, _ := n.Disk().Get("jobs", &jobs); !ok { // first start: nothing yet; after a restart: the saved jobs
	jobs = map[string]Job{}
}
```
