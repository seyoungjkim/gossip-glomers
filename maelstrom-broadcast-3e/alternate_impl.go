package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run 3e (high efficiency): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast-3e --node-count 25 --time-limit 20 --rate 100 --latency 100
// With partitions: --nemesis partition

// High-level approach: queue up messages to send in bulk only to immediate neighbors
// Each node will send `broadcast` messages to its neighbors in `bulk_broadcast`
// Nodes record messages received from `bulk_broadcast` but do not send them on.

const alternateRetryBackoff = 1000 * time.Millisecond

func alternateImpl() {
	n := maelstrom.NewNode()
	var mu sync.Mutex
	seenMessages := make(map[int64]struct{})
	pendingMessages := make(map[string]map[int64]struct{})

	// Handle node broadcast message, which is only received from clients
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body broadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body.Message

		mu.Lock()
		defer mu.Unlock()

		// Update seen message state
		if _, ok := seenMessages[message]; ok { // Don't do anything if seen already
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
		seenMessages[message] = struct{}{}

		// Add new message to pending messages queue
		for _, neighbor := range n.NodeIDs() {
			if neighbor == n.ID() { // skip self
				continue
			}
			if pendingMessages[neighbor] == nil {
				pendingMessages[neighbor] = make(map[int64]struct{})
			}
			pendingMessages[neighbor][message] = struct{}{}
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
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

	// Ignore given topology - each node will be root of tree
	n.Handle("topology", func(msg maelstrom.Message) error {
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

		// Add to seen messages
		for _, message := range body.Messages {
			seenMessages[message] = struct{}{}
		}
		return n.Reply(msg, map[string]any{"type": "bulk_broadcast_ok"})
	})

	// Background goroutine to send pending messages periodically
	go func() {
		ticker := time.NewTicker(alternateRetryBackoff)
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
