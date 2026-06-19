package main

func initializeTxnsToSend(neighbors []string) map[string]sendQueue {
	txnsToSend := make(map[string]sendQueue)
	for _, neighbor := range neighbors {
		txnsToSend[neighbor] = sendQueue{clockMessages: make(map[int][][]writeElement)}
	}
	return txnsToSend
}

func (s *server) queueAndSignalSend(writeElems []writeElement) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for _, neighbor := range s.n.NodeIDs() {
		if neighbor == s.n.ID() { // skip self
			continue
		}
		sq := s.txnsToSend[neighbor].clockMessages[s.clock]
		sq = append(sq, writeElems)
	}

	select {
	case s.signalSend <- struct{}{}:
	default:
	}
}

func (s *server) clearSentMessages(neighbor string, clock int) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for c := range s.txnsToSend[neighbor].clockMessages {
		if c < clock {
			delete(s.txnsToSend[neighbor].clockMessages, c)
		}
	}
}

func (s *server) sendWrites() {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()

	for neighbor, sq := range s.txnsToSend {
		for clock, txns := range sq.clockMessages {
			for _, txn := range txns {
				s.n.Send(
					neighbor,
					formatGossipMessageBody(clock, txn),
				)
			}
		}
	}
}
