package main

import (
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run single node:
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
// Run without partition (read-uncommitted):
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total
// Run with partition (read-uncommitted):
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
// Run with partition on 5 nodes (read-uncommitted):
//	 ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 5 --concurrency 2n --time-limit 20 --rate 100 --consistency-models read-uncommitted --availability total --nemesis partition
// Run without partition (read-committed):
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total
// Run with partition (read-uncommitted);
//   ./../maelstrom/maelstrom test -w txn-list-append --bin ~/go/bin/maelstrom-txn-list-6b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition

const readOp = "r"
const writeOp = "append"
const retryBackoff = 100 * time.Millisecond

type txnMessage struct {
	Txn [][]any `json:"txn"`
}

type gossipMessage struct {
	Id     int     `json:"id"`
	Clock  int     `json:"clock"`
	Writes [][]any `json:"writes"`
}

type gossipOkMessage struct {
	Id int `json:"id"`
}

type server struct {
	clock          int
	id             int
	n              *maelstrom.Node
	mu             sync.Mutex
	kv             map[int][]listVal
	signalSend     chan struct{}
	sendMu         sync.RWMutex
	txnsToSend     map[string]map[int]writeTxn
	neighborStates map[string]neighborState
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

type neighborState struct {
	lastValidClock int
	nextId         int
	unmergedIds    map[int]int
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:              n,
		clock:          0,
		id:             0,
		kv:             make(map[int][]listVal),
		signalSend:     make(chan struct{}, 1),
		txnsToSend:     make(map[string]map[int]writeTxn),
		neighborStates: make(map[string]neighborState),
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
	id := s.id
	clock := s.clock
	if len(writes) > 0 {
		s.id++
	}
	s.mu.Unlock()

	// Populate queue and signal sending updates to other nodes
	if len(writes) > 0 {
		s.queueAndSignalSend(id, clock, writes)
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
	s.updateNeighborStates(msg.Src, id, txn.clock)
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "gossip_ok", "id": id})
}

func (s *server) handleGossipOk(msg maelstrom.Message) error {
	id, err := parseGossipOkMessage(msg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear sent messages
	s.clearSentMessages(msg.Src, id)
	return nil
}

func (s *server) updateNeighborStates(neighbor string, id int, clock int) {
	state, ok := s.neighborStates[neighbor]
	if !ok {
		state = neighborState{
			lastValidClock: 0,
			nextId:         0,
			unmergedIds:    make(map[int]int),
		}
	}
	if id == state.nextId { // success, we can update the last valid id
		state.lastValidClock = clock
		state.nextId++
		for {
			nextId := state.nextId
			if unmergedClock, ok := state.unmergedIds[nextId]; ok {
				state.lastValidClock = unmergedClock
				state.nextId++
				delete(state.unmergedIds, nextId)
			} else {
				break
			}
		}
	} else if id > state.nextId { // store the seen id and clock for later
		state.unmergedIds[id] = clock
	}
	s.neighborStates[neighbor] = state
}
