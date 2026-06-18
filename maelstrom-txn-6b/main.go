package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run without partition:
//   ./../maelstrom/maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
// Run with partition:
//   ./../maelstrom/maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
// Note: These tests pass without implementing retries. Even 6a passes these tests.

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n  *maelstrom.Node
	kv map[int]int
	mu sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:  n,
		kv: make(map[int]int),
		mu: sync.RWMutex{},
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		return s.handleTxn(msg, false)
	})

	n.Handle("txn_internal", func(msg maelstrom.Message) error {
		return s.handleTxn(msg, true)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleTxn(msg maelstrom.Message, isInternal bool) error {
	// Unmarshal the message body
	var body txnMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	var results [][]any
	var writeTxns [][]any

	// Maintain lock for the entire transaction
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, txn := range body.Txn {
		var res []any
		key := int(txn[1].(float64))
		if txn[0] == "r" {
			res = s.handleRead(int(txn[1].(float64)))
		} else if txn[0] == "w" {
			val := int(txn[2].(float64))
			res = s.handleWrite(key, val)
			writeTxns = append(writeTxns, txn)
		}
		results = append(results, res)
	}
	if isInternal {
		return nil
	}
	err = s.sendWrites(writeTxns)
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
}

func (s *server) handleRead(key int) []any {
	val, ok := s.kv[key]
	if !ok {
		return []any{"r", key, nil}
	}
	return []any{"r", key, val}
}

func (s *server) handleWrite(key int, val int) []any {
	s.kv[key] = val
	return []any{"w", key, val}
}

func (s *server) sendWrites(writeTxns [][]any) error {
	for _, node := range s.n.NodeIDs() {
		if s.n.ID() == node {
			continue
		}
		err := s.n.Send(node, map[string]any{"type": "txn_internal", "txn": writeTxns})
		if err != nil {
			return err
		}
	}
	return nil
}
