package main

import (
	"math"
	"slices"
)

// TODO[optional]: improve efficiency of writes
// O(n)
func (s *server) handleRead(key int) []any {
	list, ok := s.kv[key]
	if !ok {
		return []any{readOp, key, nil}
	}
	// maelstrom will get detect a G0 cycle if we return writes that might be re-ordered later.
	maxClock := s.getMaxClock()
	return []any{readOp, key, formatList(maxClock, list)}
}

// O(nlogn)
func (s *server) handleWrite(we writeElement) []any {
	s.kv[we.key] = append(s.kv[we.key], we.lv)
	slices.SortFunc(s.kv[we.key], compareListVals)
	return []any{writeOp, we.key, we.lv.val}
}

// O(nlogn)
func (s *server) handleMerge(we writeElement) {
	// Important: write with the txn's clock value, not the current node's
	for _, lv := range s.kv[we.key] {
		if compareListVals(lv, we.lv) == 0 { // already added
			return
		}
	}
	s.kv[we.key] = append(s.kv[we.key], we.lv)
	slices.SortFunc(s.kv[we.key], compareListVals)
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

func (s *server) getMaxClock() int {
	maxClock := math.MaxInt
	for _, neighbor := range s.n.NodeIDs() {
		if s.n.ID() == neighbor {
			continue
		}
		c := 0
		if state, ok := s.neighborStates[neighbor]; ok {
			c = state.lastValidClock
		}
		maxClock = min(c, maxClock)
	}
	return maxClock
}
