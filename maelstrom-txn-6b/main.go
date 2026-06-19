package main

import (
	"log"
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
const retryBackoff = 100 * time.Millisecond

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type gossipMessage struct {
	Clock  int     `json:"clock"`
	Writes [][]any `json:"writes"`
}

type gossipOkMessage struct {
	ReqClock int `json:"req_clock"`
}

type server struct {
	clock      int
	n          *maelstrom.Node
	mu         sync.Mutex
	kv         map[int][]listVal
	signalSend chan struct{}
	sendMu     sync.RWMutex
	txnsToSend map[string]map[int][]writeElement
}

type txnOp struct {
	rw   string
	key  int
	list []int
}

type listVal struct {
	clock  int
	nodeId int
	index  int
	val    int
}

type writeElement struct {
	key int
	lv  listVal
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:          n,
		kv:         make(map[int][]listVal),
		clock:      0,
		mu:         sync.Mutex{},
		signalSend: make(chan struct{}, 1),
		sendMu:     sync.RWMutex{},
		txnsToSend: make(map[string]map[int][]writeElement),
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		return s.handleTxn(msg)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		return s.handleGossip(msg)
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		return s.handleGossipOk(msg)
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

func (s *server) handleTxn(msg maelstrom.Message) error {
	// Parse the message into operations
	ops, err := parseTxnMessage(msg)
	if err != nil {
		return err
	}

	// Update the node's kv store
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock++
	var results [][]any
	var writes []writeElement
	for i, op := range ops {
		if op.rw == readOp {
			results = append(results, s.handleRead(op.key))
		} else if op.rw == writeOp {
			lv := listVal{
				clock:  s.clock,
				nodeId: getNodeId(s.n.ID()),
				index:  i,
				val:    op.list[0],
			}
			we := writeElement{key: op.key, lv: lv}
			res := s.handleWrite(we)
			results = append(results, res)
			writes = append(writes, we)
		}
	}

	// Populate queue and signal sending updates to other nodes
	if len(writes) > 0 {
		s.sendMu.Lock()
		for _, neighbor := range s.n.NodeIDs() {
			if neighbor == s.n.ID() { // skip self
				continue
			}
			if _, ok := s.txnsToSend[neighbor]; !ok {
				s.txnsToSend[neighbor] = make(map[int][]writeElement)
			}
			s.txnsToSend[neighbor][s.clock] = writes
		}
		s.sendMu.Unlock()
	}
	select {
	case s.signalSend <- struct{}{}:
	default:
	}

	return s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	// Parse the message into operations
	reqClock, writeElems, err := parseGossipMessage(msg)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock = max(s.clock, reqClock)
	s.clock++

	// Update the node's kv store
	for _, we := range writeElems {
		s.handleMerge(we)
	}
	// TODO: need to handle sending to even more nodes in case of partition.
	return s.n.Reply(msg, map[string]any{"type": "gossip_ok", "req_clock": reqClock})
}

func (s *server) handleGossipOk(msg maelstrom.Message) error {
	reqClock, err := parseGossipOkMessage(msg)
	if err != nil {
		return err
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	delete(s.txnsToSend[msg.Src], reqClock)
	return nil
}

func (s *server) sendWrites() {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()

	for neighbor, txn := range s.txnsToSend {
		for _, write := range txn {
			s.n.Send(
				neighbor,
				formatGossipMessageBody(s.clock, write),
			)
		}
	}
}
