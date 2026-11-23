// Echo: this program runs once on every node in the cluster.
// Read the brief with: npm run lab -- brief echo
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	n.Handle("echo", func(ctx context.Context, msg quorumlab.Message) error {
		// msg.Body looks like {"type":"echo","echo":"hello 1","msg_id":1}
		// TODO: reply with {"type":"echo_ok","echo": <the same payload>}
		//       using n.Reply(msg, body).
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
