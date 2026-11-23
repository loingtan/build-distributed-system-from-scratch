// Unique IDs. Read the brief: npm run lab -- brief unique-ids
//
// This works on one node. Run the tests to see what goes wrong on three.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	counter := 0

	n.Handle("generate", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		counter++
		id := counter
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": id})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
