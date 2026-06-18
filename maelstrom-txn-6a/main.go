package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn-6a --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n  *maelstrom.Node
	kv *sync.Map
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:  n,
		kv: &sync.Map{},
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body txnMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var results [][]any
		for _, txn := range body.Txn {
			var res []any
			if txn[0] == "r" {
				res = s.handleRead(int(txn[1].(float64)))
			} else if txn[0] == "w" {
				res = s.handleWrite(int(txn[1].(float64)), int(txn[2].(float64)))
			}
			results = append(results, res)
		}
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleRead(key int) []any {
	val, ok := s.kv.Load(key)
	if !ok {
		return []any{"r", key, nil}
	}
	return []any{"r", key, val}
}

func (s *server) handleWrite(key int, val int) []any {
	s.kv.Store(key, val)
	return []any{"w", key, val}
}
