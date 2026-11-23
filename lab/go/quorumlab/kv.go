package quorumlab

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// KV is the shared linearizable key-value store ("kv"), in challenges that
// provide one. Every node can always reach it; it behaves like one variable
// per key, and every operation takes effect at a single instant.
type KV struct {
	n *Node
}

const kvTimeout = 5 * time.Second

func (k *KV) call(ctx context.Context, body map[string]any) (Message, error) {
	if !k.n.hasKV {
		return Message{}, fmt.Errorf("quorumlab: this challenge has no kv service")
	}
	return k.n.Call(ctx, "kv", body, kvTimeout)
}

// Read decodes the value under key into v. It reports false if the key has
// never been written.
func (k *KV) Read(ctx context.Context, key string, v any) (bool, error) {
	m, err := k.call(ctx, map[string]any{"type": "read", "key": key})
	if err != nil {
		return false, err
	}
	var b struct {
		Value json.RawMessage `json:"value"`
	}
	if err := m.Decode(&b); err != nil {
		return false, err
	}
	if len(b.Value) == 0 || string(b.Value) == "null" {
		return false, nil
	}
	return true, json.Unmarshal(b.Value, v)
}

// Write stores v under key.
func (k *KV) Write(ctx context.Context, key string, v any) error {
	m, err := k.call(ctx, map[string]any{"type": "write", "key": key, "value": v})
	if err != nil {
		return err
	}
	if m.IsError() {
		return fmt.Errorf("kv write: %s", m.header().Text)
	}
	return nil
}

// CAS sets key to `to` only if it currently holds `from`, and reports whether
// it did. Pass nil as from to mean "only if the key does not exist yet".
func (k *KV) CAS(ctx context.Context, key string, from, to any) (bool, error) {
	m, err := k.call(ctx, map[string]any{"type": "cas", "key": key, "from": from, "to": to})
	if err != nil {
		return false, err
	}
	return m.Type() == "cas_ok", nil
}
