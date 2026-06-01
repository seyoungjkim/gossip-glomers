package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

const logPrefix = "log-"
const clientPrefix = "client-"

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
	kv := maelstrom.NewLinKV(n)
	logs := make(map[string][]int)
	clientOffsets := make(map[string]map[string]int)

	// Helper function to return key value, 0 otherwise
	readIfExists := func(key string, v any) error {
		err := kv.ReadInto(context.Background(), key, v)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			// Do nothing if key not set yet
			if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				err = nil
			}
		}
		return err
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var messages []int
		err := readIfExists(logPrefix+body.Key, &messages)
		if err != nil {
			return err
		}
		offset := len(messages)
		messages = append(messages, body.Msg)
		// TODO: how to store the message and offset?
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
			requestedLogs, ok := logs[requestedKey]
			if !ok || len(requestedLogs) <= requestedOffset {
				continue
			}
			msgs[requestedKey] = make([][]int, 0)
			// Return message at offset
			msgs[requestedKey] = append(msgs[requestedKey], []int{requestedOffset, requestedLogs[requestedOffset]})
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
		if _, ok := clientOffsets[msg.Src]; ok {
			offsets = clientOffsets[msg.Src]
		}
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
