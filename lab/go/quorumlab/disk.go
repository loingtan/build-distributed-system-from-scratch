package quorumlab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Disk is a node's durable storage: a JSON object of named values kept in
// one file, disk.json, in the node's data directory.
//
// Set and Update return only after the change is written and fsynced, so
// anything they stored survives a crash. Memory does not: after a restart
// your program starts from the top and reads its state back from here.
//
// The checker can read disk.json, so in challenges that say how the disk
// must look, use the key names they give.
type Disk struct {
	mu   sync.Mutex
	path string
	data map[string]json.RawMessage
}

func openDisk(dir string) (*Disk, error) {
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &Disk{path: filepath.Join(dir, "disk.json"), data: map[string]json.RawMessage{}}
	b, err := os.ReadFile(d.path)
	if errors.Is(err, os.ErrNotExist) {
		return d, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &d.data); err != nil {
			return nil, fmt.Errorf("quorumlab: disk.json is corrupt: %w", err)
		}
	}
	return d, nil
}

// Get decodes the value stored under key into v. It reports false if
// nothing is stored there.
func (d *Disk) Get(key string, v any) (bool, error) {
	d.mu.Lock()
	raw, ok := d.data[key]
	d.mu.Unlock()
	if !ok || string(raw) == "null" {
		return false, nil
	}
	return true, json.Unmarshal(raw, v)
}

// Set stores v under key and waits until it is on disk.
func (d *Disk) Set(key string, v any) error {
	return d.Update(func(tx *Tx) error { return tx.Set(key, v) })
}

// Delete removes key and waits until the change is on disk.
func (d *Disk) Delete(key string) error {
	return d.Update(func(tx *Tx) error { tx.Delete(key); return nil })
}

// Tx is a set of changes applied to the disk in one atomic write.
type Tx struct {
	data map[string]json.RawMessage
}

// Get reads a value inside the transaction, including its own changes.
func (tx *Tx) Get(key string, v any) (bool, error) {
	raw, ok := tx.data[key]
	if !ok || string(raw) == "null" {
		return false, nil
	}
	return true, json.Unmarshal(raw, v)
}

// Set changes a value inside the transaction.
func (tx *Tx) Set(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tx.data[key] = b
	return nil
}

// Delete removes a value inside the transaction.
func (tx *Tx) Delete(key string) { delete(tx.data, key) }

// Update applies fn's changes atomically: either all of them reach the disk
// or, if fn returns an error or the node crashes first, none do.
func (d *Disk) Update(fn func(tx *Tx) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := make(map[string]json.RawMessage, len(d.data)+1)
	for k, v := range d.data {
		next[k] = v
	}
	if err := fn(&Tx{data: next}); err != nil {
		return err
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	tmp := d.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, d.path); err != nil {
		return err
	}
	d.data = next
	return nil
}
