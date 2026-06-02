package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

const logPrefix = "log-"
const offsetPrefix = "offset-"
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

	// Helper function to populate key value; returns nil if value doesn't exist
	readIfExists := func(key string) (*int, error) {
		value, err := kv.ReadInt(context.Background(), key)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				return nil, nil
			}
			return nil, err
		}
		return &value, nil
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset, err := kv.ReadInt(context.Background(), offsetPrefix+body.Key)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			// Do nothing if key not set yet, otherwise return error
			if !(ok && rpcErr.Code == maelstrom.KeyDoesNotExist) {
				return err
			}
		}
		err = kv.CompareAndSwap(context.Background(), offsetPrefix+body.Key, offset, offset+1, true)
		if err != nil {
			return err
		}
		err = kv.Write(context.Background(), logPrefix+body.Key+strconv.Itoa(offset), body.Msg)
		if err != nil {
			return err
		}
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
			kvKey := logPrefix + requestedKey + strconv.Itoa(requestedOffset)
			requestedLog, err := readIfExists(kvKey)
			if err != nil {
				return err
			}
			if requestedLog != nil {
				msgs[requestedKey] = make([][]int, 0)
				// Return message at offset
				// TODO: return more messages?
				msgs[requestedKey] = append(msgs[requestedKey], []int{requestedOffset, *requestedLog})
			}
		}
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body commitOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for logKey, logOffset := range body.Offsets {
			err := kv.Write(context.Background(), clientPrefix+msg.Src+logKey, logOffset)
			if err != nil {
				return err
			}
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
		for _, logKey := range body.Keys {
			committedOffset, err := readIfExists(clientPrefix + msg.Src + logKey)
			if err != nil {
				return err
			}
			if committedOffset != nil {
				offsets[logKey] = *committedOffset
			}
		}
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
