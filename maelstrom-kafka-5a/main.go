package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5a --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

type sendMessage struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type pollMessage struct {
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsMessage struct {
	Offsets map[string]int `json:"offsets"`
}

type listCommittedOffsetsMessage struct {
	Keys []string `json:"keys"`
}

func main() {
	n := maelstrom.NewNode()
	messages := make(map[string][]int)
	clientOffsets := make(map[string]map[string]int)

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if _, ok := messages[body.Key]; !ok {
			messages[body.Key] = make([]int, 0)
		}
		offset := len(messages[body.Key])
		messages[body.Key] = append(messages[body.Key], body.Msg)
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body pollMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs := make(map[string][][]int)
		for requestedKey, requestedOffset := range body.Offsets {
			logs, ok := messages[requestedKey]
			if !ok || len(logs) <= requestedOffset {
				continue
			}
			msgs[requestedKey] = make([][]int, 0)
			// TODO: return more than 1
			msgs[requestedKey] = append(msgs[requestedKey], []int{requestedOffset, logs[requestedOffset]})
		}
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body commitOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if _, ok := clientOffsets[msg.Src]; !ok {
			clientOffsets[msg.Src] = make(map[string]int)
		}
		for logKey, logOffset := range body.Offsets {
			clientOffsets[msg.Src][logKey] = logOffset
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body listCommittedOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var offsets = make(map[string]int)
		if _, ok := messages[msg.Src]; ok {
			offsets = clientOffsets[msg.Src]
		}
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
