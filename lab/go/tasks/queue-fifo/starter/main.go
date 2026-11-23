// A FIFO queue. Read the brief: npm run lab -- brief queue-fifo
//
// A work queue on one node: this node is the broker.
// Producers send enqueue, consumers send dequeue.
package main

import (
	"context"
	"encoding/json"
	"log"

	"quorumlab"
)

// job is what consumers are handed: { id, payload }.
type job struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	n := quorumlab.NewNode()

	n.Handle("enqueue", func(ctx context.Context, msg quorumlab.Message) error {
		// msg's body is { type: "enqueue", id: "job-7", payload: { ... } };
		// msg.Decode(&j) into a job reads it.
		// TODO: remember the job as { id, payload }, behind the jobs already waiting.
		return n.Reply(msg, map[string]any{"type": "enqueue_ok"})
	})

	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: hand out the job that has waited longest and forget it.
		// If nothing is waiting, the answer is job: null.
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": nil})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
