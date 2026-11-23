// Echo: reply to every echo request with the same payload.
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	n.Handle("echo", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Echo any `json:"echo"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "echo_ok", "echo": req.Echo})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
