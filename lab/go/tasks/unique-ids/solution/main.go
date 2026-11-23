// Unique IDs: this node's id plus a local counter, with no coordination.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// A node's id is unique in the cluster, and its own counter never repeats,
	// so the pair is globally unique. No coordination, so no waiting on a partition.
	var mu sync.Mutex
	counter := 0

	n.Handle("generate", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		counter++
		id := fmt.Sprintf("%s-%d", n.ID(), counter)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": id})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
