package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
// Run: ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10

func main() {
	n := maelstrom.NewNode()
	var messages []any
	var topology map[string][]string

	// Requests node broadcast message
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body["message"])
		// TODO: figure out how to efficiently broadcast seen messages. Prevent loops.
		for _, neighbor := range topology[n.ID()] {
			err := n.Send(neighbor, body)
			if err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// Asks node to return seen messages
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messages})
	})

	// Informs nodes of neighbors
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topologyBytes, err := json.Marshal(topology["body"])
		if err != nil {
			return err
		}
		if err := json.Unmarshal(topologyBytes, &topology); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
