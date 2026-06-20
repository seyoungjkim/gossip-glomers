package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run single node:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
// Run without partition:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
// Run with partition:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
// Run with partition on more nodes: (TODO: get 5 nodes passing)
//	 ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 3 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition

const readOp = "r"
const writeOp = "append"
const retryBackoff = 100 * time.Millisecond

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type gossipMessage struct {
	Id     string  `json:"id"`
	Clock  int     `json:"clock"`
	Writes [][]any `json:"writes"`
}

type gossipOkMessage struct {
	Id    string `json:"id"`
	Clock int    `json:"clock"`
}

type server struct {
	clock          int
	n              *maelstrom.Node
	mu             sync.Mutex
	kv             map[int][]listVal
	signalSend     chan struct{}
	sendMu         sync.RWMutex
	txnsToSend     map[string]map[string]writeTxn
	neighborClocks map[string]int
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

type writeTxn struct {
	clock  int
	writes []writeElement
}

type writeElement struct {
	key int
	lv  listVal
}

func main() {
	n := maelstrom.NewNode()
	s := server{
		n:              n,
		kv:             make(map[int][]listVal),
		clock:          0,
		mu:             sync.Mutex{},
		signalSend:     make(chan struct{}, 1),
		sendMu:         sync.RWMutex{},
		txnsToSend:     make(map[string]map[string]writeTxn),
		neighborClocks: make(map[string]int),
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

	s.mu.Lock()
	// Increment clock
	s.clock++
	// Execute transaction
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
	s.mu.Unlock()

	// Populate queue and signal sending updates to other nodes
	if len(writes) > 0 {
		id := fmt.Sprintf("%s-%d", s.n.ID(), s.clock)
		s.queueAndSignalSend(id, s.clock, writes)
	}

	return s.n.Reply(msg, map[string]any{"type": "txn_ok", "txn": results})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	// Parse the message into writes
	id, txn, err := parseGossipMessage(msg)
	if err != nil {
		return err
	}

	s.mu.Lock()
	// Increment the clock
	s.clock = max(s.clock, txn.clock) + 1
	// Update the node's kv store
	for _, we := range txn.writes {
		s.handleMerge(we)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "gossip_ok", "id": id, "clock": s.clock})
}

func (s *server) handleGossipOk(msg maelstrom.Message) error {
	id, clock, err := parseGossipOkMessage(msg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update neighbor timestamp
	s.neighborClocks[msg.Src] = max(s.neighborClocks[msg.Src], clock)

	// Clear sent messages
	s.clearSentMessages(msg.Src, id)
	return nil
}
