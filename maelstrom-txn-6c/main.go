package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run with partition:
//   ./../maelstrom/maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn-6c --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
// Note: again, 6b technically does pass this test due to checker issues.

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n        *maelstrom.Node
	kv       map[int]int
	lockMu   *sync.Mutex
	keyLocks map[int]*sync.Mutex
}

type txnUpdate struct {
	rw  string
	key int
	val int
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:        n,
		kv:       make(map[int]int),
		lockMu:   &sync.Mutex{},
		keyLocks: make(map[int]*sync.Mutex),
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
	// Parse the message into operations
	ops, err := parseMessage(msg)
	if err != nil {
		return err
	}

	// Grab the required key locks
	locks, err := s.getKeyLocks(ops)
	if err != nil {
		return s.n.Reply(msg, map[string]any{
			"type":        "error",
			"in_reply_to": 1,
			"code":        30,
			"text":        "The requested transaction has been aborted because of a conflict with another transaction.",
		})
	}

	// Update the node's kv store
	var results [][]any
	var writes [][]any
	for _, op := range ops {
		if op.rw == "r" {
			results = append(results, s.handleRead(op.key))
		} else if op.rw == "w" {
			res := s.handleWrite(op.key, op.val)
			results = append(results, s.handleWrite(op.key, op.val))
			writes = append(writes, res)
		}
	}

	// Release key locks
	for _, kl := range locks {
		kl.Unlock()
	}

	// Send updates to other nodes
	if isInternal {
		return nil
	}
	err = s.sendWrites(writes)
	if err != nil {
		return err
	}
	return s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
}

func parseMessage(msg maelstrom.Message) ([]txnUpdate, error) {
	var body txnMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	var ops []txnUpdate
	for _, op := range body.Txn {
		key := int(op[1].(float64))
		if op[0] == "r" {
			ops = append(ops, txnUpdate{rw: "r", key: key, val: 0})
		} else if op[0] == "w" {
			ops = append(ops, txnUpdate{rw: "w", key: key, val: int(op[2].(float64))})
		}
	}
	return ops, nil
}

func (s *server) getKeyLocks(ops []txnUpdate) ([]*sync.Mutex, error) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	var locks []*sync.Mutex
	for _, op := range append(ops) {
		kl, ok := s.keyLocks[op.key]
		if !ok {
			kl = &sync.Mutex{}
			s.keyLocks[op.key] = kl
		}
		// TODO: risk of deadlock here - address that and throw error
		kl.Lock()
		locks = append(locks, kl)
	}
	return locks, nil
}

func (s *server) handleRead(key int) []any {
	val, ok := s.kv[key]
	if !ok {
		return []any{"r", key, nil}
	}
	return []any{"r", key, val}
}

func (s *server) handleWrite(key int, val int) []any {
	// TODO: randomly simulate write failures in order to test aborted internal transactions
	s.kv[key] = val
	return []any{"w", key, val}
}

func (s *server) sendWrites(writes [][]any) error {
	if len(writes) == 0 {
		return nil
	}
	for _, node := range s.n.NodeIDs() {
		if s.n.ID() == node {
			continue
		}
		err := s.n.Send(node, map[string]any{"type": "txn_internal", "txn": writes})
		if err != nil {
			return err
		}
	}
	return nil
}
