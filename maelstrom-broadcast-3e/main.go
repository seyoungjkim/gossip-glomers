package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run 3e (high efficiency): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast-3e --node-count 25 --time-limit 20 --rate 100 --latency 100
//   Goal: Messages-per-operation < 20, Median latency < 1 second, Maximum latency < 2 seconds
// With partitions: --nemesis partition

// High-level approach: queue up messages to send in bulk only to immediate neighbors
// Pick an arbitrary leader node. All messages funneled through this leader.
// Each node will send `broadcast` messages to the leader.
// Nodes record messages received from `bulk_broadcast` but do not send them on unless they are the leader.

const retryBackoff = 100 * time.Millisecond

type broadcastMessage struct {
	Message int64 `json:"message"`
}

type bulkBroadcastMessage struct {
	Messages []int64 `json:"messages"`
}

func main() {
	mainImpl()
}

func mainImpl() {
	n := maelstrom.NewNode()
	var mu sync.Mutex
	seenMessages := make(map[int64]struct{})
	pendingMessages := make(map[string]map[int64]struct{})
	topology := make(map[string][]string)

	// Handle node broadcast message
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body broadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body.Message

		mu.Lock()
		defer mu.Unlock()

		// Update message state
		if _, ok := seenMessages[message]; ok { // Don't do anything if seen already
			if isFromClient(msg.Src) { // Only respond to clients
				return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
			}
			return nil
		}

		// Add to seen messages and add new message to pending messages queue
		seenMessages[message] = struct{}{}
		for _, neighbor := range topology[n.ID()] {
			if neighbor == msg.Src {
				continue
			}
			if pendingMessages[neighbor] == nil {
				pendingMessages[neighbor] = make(map[int64]struct{})
			}
			pendingMessages[neighbor][message] = struct{}{}
		}

		// Only respond to clients
		if isFromClient(msg.Src) {
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
		return nil
	})

	// Handles returning seen messages
	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var seenMessageList []int64
		for message := range seenMessages {
			seenMessageList = append(seenMessageList, message)
		}
		body := map[string]any{"type": "read_ok", "messages": seenMessageList}
		return n.Reply(msg, body)
	})

	// Ignore given topology - select a single leader and followers
	n.Handle("topology", func(msg maelstrom.Message) error {
		var nodes []string
		for _, node := range n.NodeIDs() {
			nodes = append(nodes, node)
		}
		slices.Sort(nodes)
		leader := nodes[0]
		leaderTopology := make(map[string][]string)
		leaderTopology[leader] = []string{}
		for _, follower := range nodes[1:] {
			leaderTopology[leader] = append(leaderTopology[leader], follower)
			leaderTopology[follower] = []string{leader}
		}

		// Set the new topology
		mu.Lock()
		defer mu.Unlock()
		topology = leaderTopology
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	// Handles receiving bulk messages
	n.Handle("bulk_broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body bulkBroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		for _, message := range body.Messages {
			// Don't do anything if seen already
			if _, ok := seenMessages[message]; ok {
				continue
			}

			// Otherwise add to seen messages
			seenMessages[message] = struct{}{}

			// Add new message to pending messages queue
			for _, neighbor := range topology[n.ID()] {
				if neighbor == msg.Src {
					continue
				}
				if pendingMessages[neighbor] == nil {
					pendingMessages[neighbor] = make(map[int64]struct{})
				}
				pendingMessages[neighbor][message] = struct{}{}
			}
		}
		return nil
	})

	// Background goroutine to send pending messages periodically
	go func() {
		ticker := time.NewTicker(retryBackoff)
		for range ticker.C {
			// Lock: Gather messages to send
			mu.Lock()
			toSendRPC := make(map[string][]int64)
			for neighbor, messages := range pendingMessages {
				for m := range messages {
					toSendRPC[neighbor] = append(toSendRPC[neighbor], m)
				}
			}
			mu.Unlock()

			// Unlocked: Now send them
			for neighbor, messages := range toSendRPC {
				n.RPC(neighbor, map[string]any{"type": "bulk_broadcast", "messages": messages}, func(msg maelstrom.Message) error {
					mu.Lock()
					for _, sentMessage := range messages {
						delete(pendingMessages[neighbor], sentMessage)
					}
					if len(pendingMessages[neighbor]) == 0 {
						delete(pendingMessages, neighbor)
					}
					mu.Unlock()
					return nil
				})
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func isFromClient(src string) bool {
	return src[0] == 'c'
}
