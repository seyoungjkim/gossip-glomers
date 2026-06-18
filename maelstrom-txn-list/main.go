package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/sync/semaphore"
)

// Run read uncommitted:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total –-nemesis partition
// Run read committed:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
// Note: again, 6b technically does pass this test due to checker issues.

const readOp = "r"
const writeOp = "append"
const lockTimeout = 10 * time.Millisecond

type keyLock struct {
	key  int
	lock *semaphore.Weighted
}

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type server struct {
	n        *maelstrom.Node
	kv       map[int][]int
	lockMu   sync.Mutex
	keyLocks map[int]*keyLock
}

type txnUpdate struct {
	rw  string
	key int
	val []int
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:        n,
		kv:       make(map[int][]int),
		lockMu:   sync.Mutex{},
		keyLocks: make(map[int]*keyLock),
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
	locks, err := s.getLocks(ops)
	if err != nil {
		return s.n.Reply(msg, map[string]any{
			"type": "error",
			"code": 30,
			"text": "The requested transaction has been aborted because of a conflict with another transaction.",
		})
	}
	defer s.releaseLocks(locks)

	// Update the node's kv store
	var results [][]any
	var writes [][]any
	for _, op := range ops {
		if op.rw == readOp {
			results = append(results, s.handleRead(op.key))
		} else if op.rw == writeOp {
			res := s.handleWrite(op.key, op.val)
			results = append(results, res)
			writes = append(writes, res)
		}
	}

	// Send updates to other nodes
	if isInternal {
		return nil
	}

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
			ops = append(ops, txnUpdate{rw: readOp, key: key, val: nil})
		} else if op[0] == writeOp {
			ops = append(ops, txnUpdate{rw: writeOp, key: key, val: []int{int(op[2].(float64))}})
		}
	}
	return ops, nil
}

func (s *server) getLocks(ops []txnUpdate) ([]*keyLock, error) {
	// The below code is equivalent to a global lock on all keys.
	//
	// s.lockMu.Lock()
	// return []*sync.Mutex{s.lockMu}, nil
	//
	// It works, but we want something smarter, so we acquire per-key locks instead.

	var locks []*keyLock
	seen := make(map[int]struct{})

	// Grab list of locks
	s.lockMu.Lock()
	for _, op := range ops {
		if _, ok := seen[op.key]; ok {
			continue
		}
		seen[op.key] = struct{}{}
		kl, ok := s.keyLocks[op.key]
		if !ok {
			kl = &keyLock{
				key:  op.key,
				lock: semaphore.NewWeighted(1),
			}
			s.keyLocks[op.key] = kl
		}
		locks = append(locks, kl)
	}
	s.lockMu.Unlock()
	sort.Slice(locks, func(i, j int) bool {
		return locks[i].key < locks[j].key
	})

	// Try to acquire the locks within the timeout
	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	for i, kl := range locks {
		if err := kl.lock.Acquire(ctx, 1); err != nil {
			// Release all prior locks
			for _, pl := range locks[:i] {
				pl.lock.Release(1)
			}
			return nil, errors.New("lock cannot be acquired")
		}
	}
	return locks, nil
}

func (s *server) releaseLocks(locks []*keyLock) {
	for _, kl := range locks {
		kl.lock.Release(1)
	}
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
