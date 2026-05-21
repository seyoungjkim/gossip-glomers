package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run 3a (single-node): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
// Run 3b (multi-node): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10
// Run 3c (network partitions): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
// Run 3d (efficiency): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
//   Goal: Messages-per-operation < 30, Median latency < 400ms, Maximum latency < 600ms
// Run 3e (high efficiency): ./../maelstrom/maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
//   Goal: Messages-per-operation < 20, Median latency < 1 second, Maximum latency < 2 seconds

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type topologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	mu := &sync.Mutex{}
	seenMessages := make(map[int64]struct{})
	var messageList []int64
	topology := make(map[string][]string)

	// Requests node broadcast message
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body broadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		_, ok := seenMessages[body.Message]
		if ok {
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
		// Only send new messages to neighbors
		mu.Lock()
		seenMessages[body.Message] = struct{}{}
		messageList = append(messageList, body.Message)
		mu.Unlock()
		for _, neighbor := range topology[n.ID()] {
			if neighbor == msg.Src {
				continue
			}
			err := n.RPC(neighbor, body, func(msg maelstrom.Message) error { return nil })
			if err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// Asks node to return seen messages
	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messageList})
	})

	// Informs nodes of neighbors
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology = body.Topology
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
