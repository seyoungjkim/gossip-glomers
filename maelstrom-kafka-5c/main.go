package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Run: ./../maelstrom/maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka-5c --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
// 5b result (1 msg/poll):
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
// 5b result (5 msg/poll)
//       :availability {:valid? true, :ok-fraction 0.9995857},
//       :net {:all {:send-count 259414,
//             :recv-count 259414,
//             :msg-count 259414,
//             :msgs-per-op 15.353575},
//       :clients {:send-count 43188,
//                 :recv-count 43188,
//                 :msg-count 43188},
//       :servers {:send-count 216226,
//                 :recv-count 216226,
//                 :msg-count 216226,
//                 :msgs-per-op 12.797467},
//       :valid? true},
// 5c result after optimizing client offsets (1 msg/poll):
//       :availability {:valid? true, :ok-fraction 0.99953276},
//       :net {:all {:send-count 261346,
//             :recv-count 261346,
//             :msg-count 261346,
//             :msgs-per-op 15.263754},
//       :clients {:send-count 48318,
//                 :recv-count 48318,
//                 :msg-count 48318},
//       :servers {:send-count 213028,
//                 :recv-count 213028,
//                 :msg-count 213028,
//                 :msgs-per-op 12.441771},
//       :valid? true},
// 5c result after optimizing client offsets and logs (5 msg/poll):
//       :availability {:valid? true, :ok-fraction 0.9995894},
//       :net {:all {:send-count 169266,
//             :recv-count 169266,
//             :msg-count 169266,
//             :msgs-per-op 9.929372},
//       :clients {:send-count 43658,
//                 :recv-count 43658,
//                 :msg-count 43658},
//       :servers {:send-count 125608,
//                 :recv-count 125608,
//                 :msg-count 125608,
//                 :msgs-per-op 7.368335},
//       :valid? true},
// Same with 10 msg/poll:
//       :availability {:valid? true, :ok-fraction 0.9995302},
//       :net {:all {:send-count 152760,
//             :recv-count 152760,
//             :msg-count 152760,
//             :msgs-per-op 8.970579},
//       :clients {:send-count 42460,
//                 :recv-count 42460,
//                 :msg-count 42460},
//       :servers {:send-count 110300,
//                 :recv-count 110300,
//                 :msg-count 110300,
//                 :msgs-per-op 6.477186},
//       :valid? true},

const pollMessageCount = 10

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
	Client  string         `json:"client"`
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
	keyMutex       *sync.Mutex
	logMutex       *sync.Mutex
	clientMutex    *sync.Mutex
	messageOffsets map[string]int
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := server{
		n,
		kv,
		&sync.Mutex{},
		&sync.Mutex{},
		&sync.Mutex{},
		make(map[string]int),
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// Find which node should handle send for this topic
		nodeToHandle, err := s.routeNode(body.Key)
		if err != nil {
			return err
		}
		// If current node, send
		if n.ID() == nodeToHandle {
			offset, err := s.handleSend(body.Key, body.Msg)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
		}
		// Otherwise, ask other node to send
		nodeMsg, err := n.SyncRPC(context.Background(), nodeToHandle, map[string]any{
			"type": "send_internal",
			"key":  body.Key,
			"msg":  body.Msg,
		})
		if err != nil {
			return err
		}
		var nodeBody sendInternalMessage
		if err := json.Unmarshal(nodeMsg.Body, &nodeBody); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": nodeBody.Offset})
	})

	n.Handle("send_internal", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body sendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset, err := s.handleSend(body.Key, body.Msg)
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
			requestedLogs, err := s.readLogsIfExists(logPrefix + requestedKey)
			if err != nil {
				return err
			}
			// Grab up to pollMessageCount logs
			var logs [][]int
			for i := 0; i < pollMessageCount; i++ {
				requestedMessage, ok := requestedLogs[requestedOffset+i]
				if !ok {
					break
				}
				logs = append(logs, []int{requestedOffset + i, requestedMessage})
			}
			if len(logs) > 0 {
				msgs[requestedKey] = logs
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
		// Find which node should handle commit for this client
		nodeToHandle, err := s.routeNode(msg.Src)
		if err != nil {
			return err
		}
		// If current node, handle commit
		if n.ID() == nodeToHandle {
			err = s.handleCommitOffsets(body.Offsets, msg.Src)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
		}
		// Otherwise, ask other node to commit
		_, err = n.SyncRPC(context.Background(), nodeToHandle, map[string]any{
			"type":    "commit_offsets_internal",
			"offsets": body.Offsets,
			"client":  msg.Src,
		})
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("commit_offsets_internal", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body commitOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		err := s.handleCommitOffsets(body.Offsets, body.Client)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_internal_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body
		var body listCommittedOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets, err := s.readOffsetsIfExists(clientPrefix + msg.Src)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// Helper function to get key value; returns nil if value doesn't exist
func (s *server) readIntIfExists(key string) (*int, error) {
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

// Helper function to get key value; returns empty map if value doesn't exist
func (s *server) readOffsetsIfExists(key string) (map[string]int, error) {
	m := map[string]int{}
	err := s.kv.ReadInto(context.Background(), key, &m)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return m, nil
		}
		return nil, err
	}
	return m, nil
}

// Helper function to get key value; returns empty map if value doesn't exist
func (s *server) readLogsIfExists(key string) (map[int]int, error) {
	m := map[int]int{}
	err := s.kv.ReadInto(context.Background(), key, &m)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return m, nil
		}
		return nil, err
	}
	return m, nil
}

func (s *server) routeNode(key string) (string, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return "", err
	}
	sum := int(hasher.Sum32())
	return fmt.Sprintf("n%d", sum%len(s.n.NodeIDs())), nil
}

func (s *server) handleSend(key string, message int) (int, error) {
	// Read previous offset from memory, falling back to KV if needed
	s.keyMutex.Lock()
	_, ok := s.messageOffsets[key]
	if !ok { // Grab previous offset
		prevOffset, err := s.kv.ReadInt(context.Background(), offsetPrefix+key)
		if err != nil {
			rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
			if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				prevOffset = -1
			} else {
				s.keyMutex.Unlock()
				return -1, err
			}
		}
		s.messageOffsets[key] = prevOffset
	}
	s.messageOffsets[key]++
	messageOffset := s.messageOffsets[key]
	s.keyMutex.Unlock()

	// Write new offset - should not need CaS since each node has own key
	err := s.kv.Write(context.Background(), offsetPrefix+key, messageOffset)
	if err != nil {
		return -1, err
	}

	// Update logs by reading existing and adding new
	s.logMutex.Lock()
	defer s.logMutex.Unlock()
	logs, err := s.readLogsIfExists(logPrefix + key)
	if err != nil {
		return -1, err
	}
	logs[messageOffset] = message
	err = s.kv.Write(context.Background(), logPrefix+key, logs)
	if err != nil {
		return -1, err
	}
	return messageOffset, nil
}

// Read and write offsets from "client-{CLIENT_NAME}" key
func (s *server) handleCommitOffsets(newOffsets map[string]int, client string) error {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()

	clientOffsets, err := s.readOffsetsIfExists(clientPrefix + client)
	if err != nil {
		return err
	}
	for logKey, logOffset := range newOffsets {
		clientOffsets[logKey] = logOffset
	}
	err = s.kv.Write(context.Background(), clientPrefix+client, &clientOffsets)
	if err != nil {
		return err
	}
	return nil
}
