package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w g-counter --bin ~/go/bin/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

type addMessage struct {
	Delta int `json:"delta"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

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

		value, err := readIfExists("value")
		if err != nil {
			return err
		}
		newValue := value + delta
		err = kv.CompareAndSwap(context.Background(), "value", value, newValue, true)
		if err != nil {
			return err
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
