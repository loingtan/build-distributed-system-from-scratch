// Single-node broadcast. Read the brief: npm run lab -- brief broadcast-single
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// State declared here lives for as long as the node does. Handlers run
	// concurrently, so guard it with a sync.Mutex.

	n.Handle("broadcast", func(ctx context.Context, msg quorumlab.Message) error {
		// The body's "message" field is an integer.
		// TODO: remember it, then reply {"type": "broadcast_ok"}
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: reply {"type": "read_ok", "messages": [every value seen so far]}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
