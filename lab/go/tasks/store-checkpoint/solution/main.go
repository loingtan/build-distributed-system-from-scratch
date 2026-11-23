// Checkpoints. Two things on disk: a checkpoint ("snapshot", the whole map at
// some moment) and the log of changes made since that moment ("log").
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

const checkpointEvery = 32 // log entries; the tests allow up to 50

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
		// Recovery: start from the checkpoint, then replay the (short) log.
		if _, err := n.Disk().Get("snapshot", &data); err != nil {
			log.Fatalf("reading the checkpoint: %v", err)
		}
		hasLog, err := n.Disk().Get("log", &wal)
		if err != nil {
			log.Fatalf("reading the log: %v", err)
		}
		for _, e := range wal {
			apply(e)
		}
		// On the first start, create both, so the disk always has a log.
		// After a restart, leave the disk as it is.
		if hasLog {
			return
		}
		err = n.Disk().Update(func(tx *quorumlab.Tx) error {
			if err := tx.Set("snapshot", data); err != nil {
				return err
			}
			return tx.Set("log", wal)
		})
		if err != nil {
			log.Fatalf("creating the log: %v", err)
		}
	})

	// checkpoint saves data and empties the log. Everything the log says is
	// already in data, so once data is saved the log can be dropped. Both
	// changes go in one Update, so a crash cannot land between the save and
	// the truncation. The caller holds mu.
	checkpoint := func() error {
		err := n.Disk().Update(func(tx *quorumlab.Tx) error {
			if err := tx.Set("snapshot", data); err != nil {
				return err
			}
			return tx.Set("log", []entry{})
		})
		if err != nil {
			return err
		}
		wal = []entry{}
		return nil
	}

	// write logs first, then changes memory. Once Set has returned the change
	// is durable, so the caller may tell the client.
	write := func(e entry) error {
		mu.Lock()
		defer mu.Unlock()
		next := append(wal, e)
		if err := n.Disk().Set("log", next); err != nil {
			return err
		}
		wal = next
		apply(e)
		if len(wal) >= checkpointEvery {
			return checkpoint()
		}
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
