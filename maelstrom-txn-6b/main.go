package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run single node:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-6b --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
// Run without partition:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
// Run with partition:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition

const readOp = "r"
const writeOp = "append"

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n        *maelstrom.Node
	mu       sync.Mutex
	kv       map[int][]int
	lockMu   sync.Mutex
	keyLocks map[int]*keyLock
}

type txnUpdate struct {
	rw   string
	key  int
	list []int
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:  n,
		kv: make(map[int][]int),
		mu: sync.Mutex{},
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

	// Grab the required locks
	s.mu.Lock()
	defer s.mu.Unlock()
	//locks, err := s.getLocks(ops)
	//if err != nil {
	//	return s.n.Reply(msg, map[string]any{
	//		"type": "error",
	//		"code": 30,
	//		"text": "The requested transaction has been aborted because of a conflict with another transaction.",
	//	})
	//}
	//defer s.releaseLocks(locks)

	// Update the node's kv store
	var results [][]any
	var writes [][]any
	for _, op := range ops {
		if op.rw == readOp {
			results = append(results, s.handleRead(op.key))
		} else if op.rw == writeOp {
			res := s.handleWrite(op.key, op.list)
			results = append(results, res)
			writes = append(writes, res)
		}
	}

	if isInternal {
		return nil
	}

	// Send updates to other nodes
	// TODO: retry failures
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
		if op[0] == readOp {
			ops = append(ops, txnUpdate{rw: readOp, key: key, list: nil})
		} else if op[0] == writeOp {
			ops = append(ops, txnUpdate{rw: writeOp, key: key, list: []int{int(op[2].(float64))}})
		}
	}
	return ops, nil
}

func (s *server) handleRead(key int) []any {
	val, ok := s.kv[key]
	if !ok {
		return []any{readOp, key, nil}
	}
	return []any{readOp, key, val}
}

func (s *server) handleWrite(key int, val []int) []any {
	s.kv[key] = append(s.kv[key], val[0])
	return []any{writeOp, key, val[0]}
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
