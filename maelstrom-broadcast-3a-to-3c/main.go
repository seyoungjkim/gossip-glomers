package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run 3a (single-node): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast-3a-to-3c --node-count 1 --time-limit 20 --rate 10
// Run 3b (multi-node): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast-3a-to-3c --node-count 5 --time-limit 20 --rate 10
// Run 3c (network partitions): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast-3a-to-3c --node-count 5 --time-limit 20 --rate 10 --nemesis partition

const retryBackoff = 100 * time.Millisecond

type broadcastMessage struct {
	Message int64 `json:"message"`
}

type bulkBroadcastMessage struct {
	Messages []int64 `json:"messages"`
}

type topologyMessage struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	var mu sync.Mutex
	seenMessages := make(map[int64]struct{})
	pendingMessages := make(map[string]map[int64]struct{})
	topology := make(map[string][]string)

	// Requests node broadcast message
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body broadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body.Message

		// Lock: update message state
		mu.Lock()
		if _, ok := seenMessages[message]; ok { // Don't do anything if seen already
			mu.Unlock()
			if msg.Src[0] != 'c' { // Only respond to clients
				return nil
			}
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}

		// Add to seen messages and add new message to pending messages queue
		seenMessages[message] = struct{}{}
		var toSendRPC []string
		for _, neighbor := range topology[n.ID()] {
			if neighbor == msg.Src {
				continue
			}
			if pendingMessages[neighbor] == nil {
				pendingMessages[neighbor] = make(map[int64]struct{})
			}
			pendingMessages[neighbor][message] = struct{}{}
			toSendRPC = append(toSendRPC, neighbor)
		}
		mu.Unlock()

		// Unlocked: Now send all the messages
		for _, neighbor := range toSendRPC {
			n.RPC(neighbor, map[string]any{"type": "broadcast", "message": message}, func(msg maelstrom.Message) error {
				mu.Lock()
				delete(pendingMessages[neighbor], message)
				if len(pendingMessages[neighbor]) == 0 {
					delete(pendingMessages, neighbor)
				}
				mu.Unlock()
				return nil
			})
		}

		// Only respond to clients
		if msg.Src[0] != 'c' {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// Asks node to return seen messages
	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		var seenMessageList []int64
		for message := range seenMessages {
			seenMessageList = append(seenMessageList, message)
		}
		body := map[string]any{"type": "read_ok", "messages": seenMessageList}
		mu.Unlock()
		return n.Reply(msg, body)
	})

	// Informs nodes of neighbors
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		topology = body.Topology
		mu.Unlock()
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
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "bulk_broadcast_ok"})
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
