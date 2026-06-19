package main

import "slices"

// TODO: improve efficiency of writes
// O(n)
func (s *server) handleRead(key int) []any {
	list, ok := s.kv[key]
	if !ok {
		return []any{readOp, key, nil}
	}
	return []any{readOp, key, formatList(list)}
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
