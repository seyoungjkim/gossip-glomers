package main

import (
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

func main() {
	n := maelstrom.NewNode()
	count := 0
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Send message back with UUID.
		newBody := map[string]any{
			"type": "generate_ok",
			"id":   n.ID() + "_" + strconv.Itoa(count),
		}
		count += 1
		return n.Reply(msg, newBody)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
