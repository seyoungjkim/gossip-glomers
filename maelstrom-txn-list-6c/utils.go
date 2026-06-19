package main

import (
	"context"
	"errors"
	"maelstrom-txn-6b"
	"sort"
	"time"

	"golang.org/x/sync/semaphore"
)

const lockTimeout = 10 * time.Millisecond

type keyLock struct {
	key  int
	lock *semaphore.Weighted
}

func (s *maelstrom_txn_6b.server) getLocks(ops []maelstrom_txn_6b.txnUpdate) ([]*keyLock, error) {
	// The below code is equivalent to a global lock on all keys.
	//
	// s.lockMu.Lock()
	// return []*sync.Mutex{s.lockMu}, nil
	//
	// It works, but we want something smarter, so we acquire per-key locks instead.

	var locks []*keyLock
	seen := make(map[int]struct{})

	// Grab list of locks
	s.lockMu.Lock()
	for _, op := range ops {
		if _, ok := seen[op.key]; ok {
			continue
		}
		seen[op.key] = struct{}{}
		kl, ok := s.keyLocks[op.key]
		if !ok {
			kl = &keyLock{
				key:  op.key,
				lock: semaphore.NewWeighted(1),
			}
			s.keyLocks[op.key] = kl
		}
		locks = append(locks, kl)
	}
	s.lockMu.Unlock()
	sort.Slice(locks, func(i, j int) bool {
		return locks[i].key < locks[j].key
	})

	// Try to acquire the locks within the timeout
	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	for i, kl := range locks {
		err := kl.lock.Acquire(ctx, 1)
		if err != nil {
			// Release all prior locks
			for _, pl := range locks[:i] {
				pl.lock.Release(1)
			}
			return nil, errors.New("lock cannot be acquired")
		}
	}
	return locks, nil
}

func (s *maelstrom_txn_6b.server) releaseLocks(locks []*keyLock) {
	for _, kl := range locks {
		kl.lock.Release(1)
	}
}
