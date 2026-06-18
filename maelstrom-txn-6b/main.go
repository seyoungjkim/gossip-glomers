package main

import (
	"encoding/json"
	"log"
	"reflect"
	"slices"
	"sync"
	"time"

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
const retryBackoff = 10 * time.Millisecond

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n          *maelstrom.Node
	mu         sync.Mutex
	kv         map[int][]int
	signalSend chan struct{}
	sendMu     sync.Mutex
	txnsToSend map[string][][][]any
}

type txnUpdate struct {
	rw   string
	key  int
	list []int
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:          n,
		kv:         make(map[int][]int),
		mu:         sync.Mutex{},
		signalSend: make(chan struct{}, 1),
		sendMu:     sync.Mutex{},
		txnsToSend: make(map[string][][][]any),
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		return s.handleTxn(msg, false)
	})

	n.Handle("txn_internal", func(msg maelstrom.Message) error {
		return s.handleTxn(msg, true)
	})

	// Background goroutine to send pending elements periodically
	go func() {
		ticker := time.NewTicker(retryBackoff)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-s.signalSend:
			}
			s.sendWrites()
		}
	}()

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

	// Update the node's kv store
	s.mu.Lock()
	defer s.mu.Unlock()
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
		return s.n.Reply(msg, map[string]any{"type": "txn_internal_ok"})
	}

	// Populate queue and signal sending updates to other nodes
	s.sendMu.Lock()
	for _, neighbor := range s.n.NodeIDs() {
		if neighbor == s.n.ID() { // skip self
			continue
		}
		s.txnsToSend[neighbor] = append(s.txnsToSend[neighbor], writes)
	}
	s.sendMu.Unlock()
	select {
	case s.signalSend <- struct{}{}:
	default:
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
	// TODO: enforce ordering here
	return []any{readOp, key, val}
}

func (s *server) handleWrite(key int, val []int) []any {
	s.kv[key] = append(s.kv[key], val[0])
	return []any{writeOp, key, val[0]}
}

func (s *server) sendWrites() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for neighbor, txns := range s.txnsToSend {
		for _, txn := range txns {
			s.n.RPC(neighbor, map[string]any{"type": "txn_internal", "txn": txn}, func(msg maelstrom.Message) error {
				s.sendMu.Lock()
				defer s.sendMu.Unlock()

				if msg.Type() == "error" {
					return nil
				}
				s.txnsToSend[neighbor] = slices.DeleteFunc(s.txnsToSend[neighbor], func(t [][]any) bool {
					return reflect.DeepEqual(t, txn)
				})
				return nil
			})
		}
	}
}
