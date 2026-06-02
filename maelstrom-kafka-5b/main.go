package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
// Result:
//       :availability {:valid? true, :ok-fraction 0.99964577},
//       :net {:all {:send-count 382318,
//             :recv-count 382318,
//             :msg-count 382318,
//             :msgs-per-op 22.570282},
//       :clients {:send-count 48170,
//                 :recv-count 48170,
//                 :msg-count 48170},
//       :servers {:send-count 334148,
//                 :recv-count 334148,
//                 :msg-count 334148,
//                 :msgs-per-op 19.72655},
//       :valid? true},

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

	// Helper function to get offset; returns -1 if value doesn't exist
	readOffset := func(key string) (int, error) {
		offset, err := kv.ReadInt(context.Background(), offsetPrefix+key)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				return -1, nil
			}
			return 0, err
		}
		return offset, nil
	}

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
		var messageOffset int
		// Keep attempting to send until success
		for {
			prevOffset, err := readOffset(body.Key)
			if err != nil {
				return err
			}
			messageOffset = prevOffset + 1
			err = kv.CompareAndSwap(context.Background(), offsetPrefix+body.Key, prevOffset, messageOffset, true)
			if err != nil {
				rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
				if ok && rpcErr.Code == maelstrom.PreconditionFailed {
					time.Sleep(10 * time.Millisecond)
					continue
				} else {
					return err
				}
			}
			// success!
			break
		}
		err := kv.Write(context.Background(), formatLogKey(body.Key, messageOffset), body.Msg)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": messageOffset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body pollMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs := make(map[string][][]int)
		for requestedKey, requestedOffset := range body.Offsets {
			requestedLog, err := readIfExists(formatLogKey(requestedKey, requestedOffset))
			if err != nil {
				return err
			}
			if requestedLog != nil {
				// Return message at offset
				msgs[requestedKey] = [][]int{{requestedOffset, *requestedLog}}
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
			err := kv.Write(context.Background(), formatClientKey(msg.Src, logKey), logOffset)
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
			committedOffset, err := readIfExists(formatClientKey(msg.Src, logKey))
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

func formatLogKey(key string, offset int) string {
	return logPrefix + key + "-" + strconv.Itoa(offset)
}

func formatClientKey(client string, key string) string {
	return clientPrefix + client + "-" + key
}
