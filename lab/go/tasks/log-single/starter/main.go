// Single-node log. Read the brief: npm run lab -- brief log-single
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// key -> list of messages. A message's offset is its index.
	// Handlers run concurrently: guard it with a mutex.
	logs := map[string][]any{}
	_ = logs

	n.Handle("send", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
			Msg any    `json:"msg"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: append req.Msg to the log for req.Key
		// reply {"type": "send_ok", "offset": offset}
		return nil
	})

	n.Handle("poll", func(ctx context.Context, msg quorumlab.Message) error {
		// msg.Body has "offsets": {"k1": 3, "k2": 0}
		// TODO: reply {"type": "poll_ok", "msgs": {"k1": [[3, msg], [4, msg]], "k2": [...]}}
		return nil
	})

	n.Handle("commit_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		// msg.Body has "offsets": {"k1": 5}
		// TODO: remember them, reply {"type": "commit_offsets_ok"}
		return nil
	})

	n.Handle("list_committed_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		// msg.Body has "keys": ["k1", "k2"]
		// TODO: reply {"type": "list_committed_offsets_ok", "offsets": {"k1": 5}}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
