package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
// Consider other IDs, such as Snowflake ID: https://en.wikipedia.org/wiki/Snowflake_ID

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Send message back with UUID.
		newBody := map[string]any{
			"type": "generate_ok",
			"id":   uuid.New(),
		}
		return n.Reply(msg, newBody)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
