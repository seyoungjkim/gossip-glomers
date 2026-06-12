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

const pollMessageCount = 20

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
	Client  string         `json:"client"`
}

type listCommittedOffsetsMessage struct {
	Keys []string `json:"keys"`
}

type sendInternalMessage struct {
	Offset int `json:"offset"`
}

type server struct {
	n             *maelstrom.Node
	kv            *maelstrom.KV
	logMutexes    *sync.Map
	clientMutexes *sync.Map
	logMessages   *sync.Map
	clientOffsets *sync.Map
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := server{
		n,
		kv,
		&sync.Map{},
		&sync.Map{},
		&sync.Map{},
		&sync.Map{},
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
		// If current node, handle send
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
		requestedMessages := make(map[string][][]int)
		for requestedKey, requestedOffset := range body.Offsets {
			// Get read lock since we might read from memory
			mu, _ := s.logMutexes.LoadOrStore(requestedKey, &sync.RWMutex{})
			mu.(*sync.RWMutex).RLock()
			allMessages, err := s.readLogMessages(requestedKey)
			if err != nil {
				mu.(*sync.RWMutex).RUnlock()
				return err
			}
			// Grab up to `pollMessageCount` logs
			var messages [][]int
			for i := requestedOffset; i < min(requestedOffset+pollMessageCount, len(allMessages)); i++ {
				messages = append(messages, []int{i, allMessages[i]})
			}
			mu.(*sync.RWMutex).RUnlock()
			if len(messages) > 0 {
				requestedMessages[requestedKey] = messages
			}
		}
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": requestedMessages})
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
		// Get read lock since we might read from memory
		mu, _ := s.clientMutexes.LoadOrStore(msg.Src, &sync.RWMutex{})
		mu.(*sync.RWMutex).RLock()
		allOffsets, err := s.readClientOffsets(msg.Src)
		if err != nil {
			mu.(*sync.RWMutex).RUnlock()
			return err
		}
		requestedOffsets := map[string]int{}
		for _, key := range body.Keys {
			offset, ok := allOffsets[key]
			if ok {
				requestedOffsets[key] = offset
			}
		}
		mu.(*sync.RWMutex).RUnlock()
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": requestedOffsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
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

// Increment offsets + read and write logs from "log-{KEY}"
func (s *server) handleSend(key string, message int) (int, error) {
	mu, _ := s.logMutexes.LoadOrStore(key, &sync.RWMutex{})
	mu.(*sync.RWMutex).Lock()
	defer mu.(*sync.RWMutex).Unlock()

	// Update logs by reading existing and adding new
	logs, err := s.readLogMessages(key)
	if err != nil {
		return -1, err
	}
	messageOffset := len(logs)
	logs = append(logs, message)
	if err = s.writeLogMessages(key, logs); err != nil {
		return -1, err
	}
	return messageOffset, nil
}

// Read and write offsets from "client-{CLIENT_NAME}" key
func (s *server) handleCommitOffsets(newOffsets map[string]int, client string) error {
	mu, _ := s.clientMutexes.LoadOrStore(client, &sync.RWMutex{})
	mu.(*sync.RWMutex).Lock()
	defer mu.(*sync.RWMutex).Unlock()

	// Update offsets for client
	clientOffsets, err := s.readClientOffsets(client)
	if err != nil {
		return err
	}
	for logKey, logOffset := range newOffsets {
		clientOffsets[logKey] = logOffset
	}
	if err = s.writeClientOffsets(client, clientOffsets); err != nil {
		return err
	}
	return nil
}

// Helper function to get key value; returns empty slice if value doesn't exist
// Attempts to read from in-memory store and falls back to KV store.
func (s *server) readLogMessages(key string) ([]int, error) {
	logs, ok := s.logMessages.Load(key)
	if ok {
		return logs.([]int), nil
	}
	var l []int
	err := s.kv.ReadInto(context.Background(), logPrefix+key, &l)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return l, nil
		}
		return nil, err
	}
	return l, nil
}

// Helper function to write key value to both in-memory and KV stores
func (s *server) writeLogMessages(key string, messages []int) error {
	err := s.kv.Write(context.Background(), logPrefix+key, messages)
	if err != nil {
		return err
	}
	s.logMessages.Store(key, messages)
	return nil
}

// Helper function to get key value; returns empty map if value doesn't exist
// Attempts to read from in-memory store and falls back to KV store.
func (s *server) readClientOffsets(key string) (map[string]int, error) {
	offsets, ok := s.clientOffsets.Load(key)
	if ok {
		return offsets.(map[string]int), nil
	}
	m := map[string]int{}
	err := s.kv.ReadInto(context.Background(), clientPrefix+key, &m)
	if err != nil {
		rpcErr, ok := errors.AsType[*maelstrom.RPCError](err)
		if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return m, nil
		}
		return nil, err
	}
	return m, nil
}

// Helper function to write key value to both in-memory and KV stores
func (s *server) writeClientOffsets(key string, offsets map[string]int) error {
	err := s.kv.Write(context.Background(), clientPrefix+key, offsets)
	if err != nil {
		return err
	}
	s.clientOffsets.Store(key, offsets)
	return nil
}
