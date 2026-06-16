package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w g-set --bin ~/go/bin/maelstrom-set --node-count 20 --rate 100 --time-limit 20 --nemesis partition
// High-level approach: Similar to broadcast solution.

const retryBackoff = 50 * time.Millisecond

type addMessage struct {
	Element any `json:"element"`
}

type bulkAddMessage struct {
	Elements []any `json:"elements"`
}

func main() {
	n := maelstrom.NewNode()
	var mu sync.Mutex
	seenElements := make(map[any]struct{})
	pendingElements := make(map[string]map[any]struct{})
	bulkAdd := make(chan struct{}, 1)

	// Handle node add message, which is only received from clients
	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body addMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		elem := body.Element

		mu.Lock()
		defer mu.Unlock()

		// Update seen message state
		if _, ok := seenElements[elem]; ok { // Don't do anything if seen already
			return n.Reply(msg, map[string]any{"type": "add_ok"})
		}
		seenElements[elem] = struct{}{}

		// Add new message to pending queue
		for _, neighbor := range n.NodeIDs() {
			if neighbor == n.ID() { // skip self
				continue
			}
			if pendingElements[neighbor] == nil {
				pendingElements[neighbor] = make(map[any]struct{})
			}
			pendingElements[neighbor][elem] = struct{}{}
		}

		// Send pending element(s) to neighbors
		select {
		case bulkAdd <- struct{}{}:
		default:
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	// Handles receiving bulk adds
	n.Handle("bulk_add", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body bulkAddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		// Add to seen elements
		for _, elem := range body.Elements {
			seenElements[elem] = struct{}{}
		}
		return n.Reply(msg, map[string]any{"type": "bulk_add_ok"})
	})

	// Handles returning elements
	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var seenElementsList []any
		for elem := range seenElements {
			seenElementsList = append(seenElementsList, elem)
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": seenElementsList})
	})

	// Background goroutine to send pending elements periodically
	go func() {
		ticker := time.NewTicker(retryBackoff)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-bulkAdd:
			}

			// Gather elements to send
			mu.Lock()
			toSendRPC := make(map[string][]any)
			for neighbor, elements := range pendingElements {
				for elem := range elements {
					toSendRPC[neighbor] = append(toSendRPC[neighbor], elem)
				}
			}
			mu.Unlock()

			// Now send them
			for neighbor, elements := range toSendRPC {
				n.RPC(neighbor, map[string]any{"type": "bulk_add", "elements": elements}, func(msg maelstrom.Message) error {
					mu.Lock()
					defer mu.Unlock()

					if msg.Type() == "error" {
						return nil
					}
					for _, elem := range elements {
						delete(pendingElements[neighbor], elem)
					}
					if len(pendingElements[neighbor]) == 0 {
						delete(pendingElements, neighbor)
					}
					return nil
				})
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
