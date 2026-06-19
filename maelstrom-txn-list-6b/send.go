package main

func (s *server) queueAndSignalSend(src string, id string, clock int, writeElems []writeElement) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for _, neighbor := range s.n.NodeIDs() {
		if neighbor == s.n.ID() || neighbor == src { // skip self and source
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

// TODO: optimize so we don't hold lock during send?
func (s *server) sendWrites() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for neighbor, sq := range s.txnsToSend {
		for id, txn := range sq {
			s.n.Send(
				neighbor,
				formatGossipMessageBody(id, txn),
			)
		}
	}
}
