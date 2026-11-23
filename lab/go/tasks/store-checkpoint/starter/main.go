// Checkpoints. Read the brief: npm run lab -- brief store-checkpoint
//
// The write-ahead log from the previous task. It recovers correctly, but
// the log on disk grows by one entry per write, forever.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

// entry is one change: {"op":"put","key":...,"value":...} or {"op":"delete","key":...}.
type entry struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func main() {
	n := quorumlab.NewNode()

	// mu makes each write one step: log, apply, in that order, with no other
	// write in between.
	var mu sync.Mutex
	wal := []entry{}
	data := map[string]string{}

	apply := func(e entry) {
		if e.Op == "put" {
			data[e.Key] = e.Value
		} else {
			delete(data, e.Key)
		}
	}

	n.OnStart(func(ctx context.Context) {
		// TODO: recovery now has two steps: load the checkpoint (if there is
		//       one), then replay the log on top of it.
		hasLog, err := n.Disk().Get("log", &wal)
		if err != nil {
			log.Fatalf("reading the log: %v", err)
		}
		for _, e := range wal {
			apply(e)
		}
		// Create the log on the first start, so the disk always has one.
		if !hasLog {
			if err := n.Disk().Set("log", wal); err != nil {
				log.Fatalf("creating the log: %v", err)
			}
		}
	})

	// TODO: write the whole map to disk as one object (for example
	//       tx.Set("snapshot", data)) and start a fresh log, in one
	//       n.Disk().Update so that both changes land together or not at all.
	// checkpoint := func() error { return nil }

	write := func(e entry) error {
		mu.Lock()
		defer mu.Unlock()
		next := append(wal, e)
		if err := n.Disk().Set("log", next); err != nil {
			return err
		}
		wal = next
		apply(e)
		// TODO: when the log is long enough, checkpoint. Not on every write:
		//       a checkpoint rewrites everything, a log append writes one entry.
		return nil
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if err := write(entry{Op: "put", Key: req.Key, Value: req.Value}); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("delete", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if err := write(entry{Op: "delete", Key: req.Key}); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "delete_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value, ok := data[req.Key]
		mu.Unlock()
		if !ok {
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": nil})
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
