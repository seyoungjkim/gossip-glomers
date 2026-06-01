package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w g-counter --bin ~/go/bin/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

const retryBackoff = 50 * time.Millisecond

type addMessage struct {
	Delta int `json:"delta"`
}

func main() {
	var mu sync.Mutex
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	nodeDelta := 0

	// Helper function to return key value as int, 0 otherwise
	readIfExists := func(key string) (int, error) {
		value, err := kv.ReadInt(context.Background(), key)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			// Return 0 if key not set yet
			if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				value = 0
				err = nil
			}
		}
		return value, err
	}

	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body addMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := body.Delta

		// Read old value and add the delta
		value, err := readIfExists("value")
		if err != nil {
			return err
		}
		newValue := value + delta
		err = kv.CompareAndSwap(context.Background(), "value", value, newValue, true)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			// The counter was updated - schedule another attempt
			if ok && rpcErr.Code == maelstrom.PreconditionFailed {
				mu.Lock()
				nodeDelta += delta
				mu.Unlock()
			} else {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		value, err := readIfExists("value")
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	// Background goroutine to retry adding periodically
	go func() {
		ticker := time.NewTicker(retryBackoff)
		for range ticker.C {
			// Skip if nothing to add
			if nodeDelta == 0 {
				continue
			}
			// Read old value and add the delta
			value, err := readIfExists("value")
			if err != nil {
				continue
			}
			newValue := value + nodeDelta
			err = kv.CompareAndSwap(context.Background(), "value", value, newValue, true)
			if err == nil {
				mu.Lock()
				nodeDelta = 0
				mu.Unlock()
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
