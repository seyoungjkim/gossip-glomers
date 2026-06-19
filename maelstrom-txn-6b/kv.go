package main

import (
	"slices"
)

// TODO[optional]: Move complexity into handleMerge?
func (s *server) handleRead(key int) []any {
	list, ok := s.kv[key]
	if !ok {
		return []any{readOp, key, nil}
	}
	slices.SortFunc(list, compareListVals)
	return []any{readOp, key, formatList(list)}
}

func (s *server) handleWrite(we writeElement) []any {
	s.kv[we.key] = append(s.kv[we.key], we.lv)
	return []any{writeOp, we.key, we.lv.val}
}

func (s *server) handleMerge(we writeElement) {
	// Important: write with the txn's clock value, not the current node's
	for _, lv := range s.kv[we.key] {
		if compareListVals(lv, we.lv) == 0 { // already added
			break
		}
	}
	s.kv[we.key] = append(s.kv[we.key], we.lv)
}

func compareListVals(a, b listVal) int {
	if a.clock == b.clock {
		if a.nodeId == b.nodeId {
			return a.index - b.index
		}
		return a.nodeId - b.nodeId
	}
	return a.clock - b.clock
}
