// Timeouts and deadlines. Read the brief: npm run lab -- brief resil-timeout
//
// A service that answers "get" by asking the upstream for the value.
// It works as long as the upstream answers quickly, every time.
package main

import (
	"context"
	"log"
	"time"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// n.Call waits as long as the timeout you pass (here a generous second)
		// and returns quorumlab.ErrTimeout if no reply came.
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, time.Second)

		// TODO: the client must hear back within 300ms, whatever the upstream does.
		// If there is no answer in time, reply {"type":"error","code":"timeout"}.
		if err != nil {
			return err
		}
		var up struct {
			Value any `json:"value"`
		}
		res.Decode(&up)
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": up.Value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
