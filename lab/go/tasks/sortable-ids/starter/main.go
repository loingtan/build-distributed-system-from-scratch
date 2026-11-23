// Sortable IDs. Read the brief: npm run lab -- brief sortable-ids
//
// Your unique ids from the last challenge. Unique, but they are strings and
// say nothing about when they were made.
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

	var mu sync.Mutex
	counter := 0

	n.Handle("generate", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		counter++
		// TODO: build one integer out of the time, this node's number and a
		// per-millisecond sequence. n.Elapsed().Milliseconds() is the current
		// time in ms.
		id := fmt.Sprintf("%s-%d", n.ID(), counter)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": id})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
