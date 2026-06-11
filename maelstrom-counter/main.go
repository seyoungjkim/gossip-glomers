package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w g-counter --bin ~/go/bin/maelstrom-counter --node-count 20 --rate 100 --time-limit 20 --nemesis partition

type addMessage struct {
	Delta int `json:"delta"`
}

type readNodeMessage struct {
	Value int `json:"value"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	nodeValues := make(map[string]int)
	mu := &sync.Mutex{}

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
		value, err := readIfExists(n.ID())
		if err != nil {
			return err
		}
		newValue := value + delta
		err = kv.CompareAndSwap(context.Background(), n.ID(), value, newValue, true)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		value, err := readIfExists(n.ID())
		if err != nil {
			return err
		}
		mu.Lock()
		nodeValues[n.ID()] = value
		mu.Unlock()
		for _, node := range n.NodeIDs() {
			if node == n.ID() {
				continue
			}
			nodeMsg, err := n.SyncRPC(context.Background(), node, map[string]string{"type": "read_node"})
			if err != nil {
				// use in-memory value, which can be stale
				mu.Lock()
				value += nodeValues[node]
				mu.Unlock()
				continue
			}
			var body readNodeMessage
			if err := json.Unmarshal(nodeMsg.Body, &body); err != nil {
				return err
			}
			value += body.Value
			mu.Lock()
			nodeValues[node] = body.Value
			mu.Unlock()
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("read_node", func(msg maelstrom.Message) error {
		value, err := readIfExists(n.ID())
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "read_node_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
