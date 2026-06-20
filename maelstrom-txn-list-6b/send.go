package main

type sendRequest struct {
	neighbor string
	id       string
	txn      writeTxn
}

func (s *server) queueAndSignalSend(id string, clock int, writeElems []writeElement) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for _, neighbor := range s.n.NodeIDs() {
		if neighbor == s.n.ID() { // skip self and source
			continue
		}
		if _, ok := s.txnsToSend[neighbor]; !ok {
			s.txnsToSend[neighbor] = make(map[string]writeTxn)
		}
		s.txnsToSend[neighbor][id] = writeTxn{
			clock:  clock,
			writes: writeElems,
		}
	}

	select {
	case s.signalSend <- struct{}{}:
	default:
	}
}

func (s *server) clearSentMessages(neighbor string, id string) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	delete(s.txnsToSend[neighbor], id)
}

func (s *server) sendWrites() {
	s.sendMu.RLock()
	var reqs []sendRequest
	for neighbor, sq := range s.txnsToSend {
		for id, txn := range sq {
			reqs = append(reqs, sendRequest{neighbor, id, txn})
		}
	}
	s.sendMu.RUnlock()

	for _, req := range reqs {
		s.n.Send(
			req.neighbor,
			formatGossipMessageBody(req.id, req.txn),
		)
	}
}
