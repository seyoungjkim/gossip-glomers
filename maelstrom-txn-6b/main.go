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
// TODO: this workload actually passes even on 6a...probably because no detection is actually happening.
// TODO: handle retries

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n      *maelstrom.Node
	kv     *sync.Map
	toSend *sync.Map
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:      n,
		kv:     &sync.Map{},
		toSend: &sync.Map{},
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
	for _, txn := range body.Txn {
		var res []any
		key := int(txn[1].(float64))
		if txn[0] == "r" {
			res = s.handleRead(int(txn[1].(float64)))
		} else if txn[0] == "w" {
			val := int(txn[2].(float64))
			res = s.handleWrite(key, val)
			if !isInternal {
				err = s.sendWrite(key, val)
				if err != nil {
					return err
				}
			}
		}
		results = append(results, res)
	}
	if isInternal {
		return nil
	}
	return s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
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

func (s *server) sendWrite(key int, val int) error {
	for _, node := range s.n.NodeIDs() {
		if s.n.ID() == node {
			continue
		}
		err := s.n.Send(node, map[string]any{"type": "txn_internal", "txn": [][]any{{"w", key, val}}})
		if err != nil {
			return err
		}
	}
	return nil
}
