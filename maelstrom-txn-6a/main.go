package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

func main() {
	n := maelstrom.NewNode()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
