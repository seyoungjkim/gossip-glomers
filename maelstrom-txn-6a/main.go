package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-6a --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

const readOp = "r"
const writeOp = "append"

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n  *maelstrom.Node
	kv map[int][]int
	mu sync.Mutex
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:  n,
		kv: make(map[int][]int),
		mu: sync.Mutex{},
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body txnMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		var results [][]any
		for _, txn := range body.Txn {
			var res []any
			key := int(txn[1].(float64))
			if txn[0] == readOp {
				res = s.handleRead(key)
			} else if txn[0] == writeOp {
				res = s.handleWrite(key, int(txn[2].(float64)))
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
	list, ok := s.kv[key]
	if !ok {
		return []any{readOp, key, nil}
	}
	return []any{readOp, key, list}
}

func (s *server) handleWrite(key int, val int) []any {
	_, ok := s.kv[key]
	if !ok {
		s.kv[key] = []int{}
	}
	s.kv[key] = append(s.kv[key], val)
	return []any{writeOp, key, val}
}
