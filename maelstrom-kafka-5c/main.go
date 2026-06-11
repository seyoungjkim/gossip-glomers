package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5c --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
// 5b Result:
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
// 5c Result:
//       :availability {:valid? true, :ok-fraction 0.99952906},
//       :net {:all {:send-count 432798,
//             :recv-count 432798,
//             :msg-count 432798,
//             :msgs-per-op 25.47669},
//       :clients {:send-count 47988,
//                 :recv-count 47988,
//                 :msg-count 47988},
//       :servers {:send-count 384810,
//                 :recv-count 384810,
//                 :msg-count 384810,
//                 :msgs-per-op 22.651873},
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

type sendInternalMessage struct {
	Offset int `json:"offset"`
}

type server struct {
	n              *maelstrom.Node
	kv             *maelstrom.KV
	hasher         hash.Hash
	messageOffsets map[string]int
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := server{n, kv, fnv.New32a(), make(map[string]int)}

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nodeToHandle, err := s.routeNode(body.Key)
		if err != nil {
			return err
		}
		if n.ID() == nodeToHandle {
			offset, err := s.handleSend(msg)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
		}
		nodeMsg, err := n.SyncRPC(context.Background(), nodeToHandle, map[string]any{
			"type": "send_internal",
			"key":  body.Key,
			"msg":  body.Msg,
		})
		if err != nil {
			return err
		}
		// Unmarshal the message body
		var nodeBody sendInternalMessage
		if err := json.Unmarshal(nodeMsg.Body, &nodeBody); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": nodeBody.Offset})
	})

	n.Handle("send_internal", func(msg maelstrom.Message) error {
		offset, err := s.handleSend(msg)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "send_internal_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body pollMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msgs := make(map[string][][]int)
		for requestedKey, requestedOffset := range body.Offsets {
			requestedLog, err := s.readIfExists(formatLogKey(requestedKey, requestedOffset))
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
			committedOffset, err := s.readIfExists(formatClientKey(msg.Src, logKey))
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

// Helper function to get key value; returns nil if value doesn't exist
func (s *server) readIfExists(key string) (*int, error) {
	value, err := s.kv.ReadInt(context.Background(), key)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return nil, nil
		}
		return nil, err
	}
	return &value, nil
}

func (s *server) routeNode(key string) (string, error) {
	sum, err := s.hasher.Write([]byte(key))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("n%d", sum%len(s.n.NodeIDs())), nil
}

func (s *server) handleSend(msg maelstrom.Message) (int, error) {
	// Unmarshal the message body
	var body sendMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return -1, err
	}
	// Read previous offset
	prevOffset, err := s.kv.ReadInt(context.Background(), offsetPrefix+body.Key)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if !ok || rpcErr.Code != maelstrom.KeyDoesNotExist {
			return -1, err
		}
		prevOffset = -1
	}
	// Write new offset - should not need retries since each node has own key
	messageOffset := prevOffset + 1
	err = s.kv.CompareAndSwap(context.Background(), offsetPrefix+body.Key, prevOffset, messageOffset, true)
	if err != nil {
		return -1, err
	}
	err = s.kv.Write(context.Background(), formatLogKey(body.Key, messageOffset), body.Msg)
	if err != nil {
		return -1, err
	}
	return messageOffset, nil
}

func formatLogKey(key string, offset int) string {
	return logPrefix + key + "-" + strconv.Itoa(offset)
}

func formatClientKey(client string, key string) string {
	return clientPrefix + client + "-" + key
}
