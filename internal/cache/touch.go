// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// touchQueueDepth bounds the bookkeeping allowed to accumulate behind a slow
// index. A full queue drops touches rather than making cache readers wait; an
// older LRU timestamp is a miss-risk, never a correctness risk.
const touchQueueDepth = 4096

// touchBatchSize limits one Pebble batch so a shutdown drain and a hot cache do
// not turn a single LRU update into an unbounded index lock hold.
const touchBatchSize = 256

// toucher coalesces last-used timestamp refreshes away from cache readers.
type toucher struct {
	touch       func([]ids.ActionID, int64, time.Duration) (int, error)
	granularity time.Duration
	logf        Logf

	queue chan ids.ActionID
	done  chan struct{}
	wg    sync.WaitGroup

	pendingMu sync.Mutex
	pending   map[ids.ActionID]struct{}
	closeOnce sync.Once
}

// newToucher starts the single index writer that batches deferred LRU refreshes.
func newToucher(touch func([]ids.ActionID, int64, time.Duration) (int, error), granularity time.Duration, logf Logf) *toucher {
	t := &toucher{
		touch:       touch,
		granularity: granularity,
		logf:        logf,
		queue:       make(chan ids.ActionID, touchQueueDepth),
		done:        make(chan struct{}),
		pending:     make(map[ids.ActionID]struct{}),
	}
	t.wg.Add(1)
	go t.run()
	return t
}

// enqueue schedules one action's LRU refresh when the entry is already older
// than the relatime window. An eligible action is queued at most once while
// queued or running. Losing either race deliberately leaves the older
// timestamp: a cache may evict a hot body early, but must never make a reader
// wait for bookkeeping.
func (t *toucher) enqueue(a ids.ActionID, lastUsedAt int64) {
	now := time.Now().UnixNano()
	if g := t.granularity.Nanoseconds(); g > 0 && now-lastUsedAt < g {
		return
	}
	select {
	case <-t.done:
		return
	default:
	}

	t.pendingMu.Lock()
	if _, ok := t.pending[a]; ok {
		t.pendingMu.Unlock()
		return
	}
	t.pending[a] = struct{}{}
	t.pendingMu.Unlock()

	select {
	case t.queue <- a:
	case <-t.done:
		t.forget(a)
	default:
		t.forget(a)
	}
}

// close stops new refreshes and drains the bounded queue before the index closes.
func (t *toucher) close() {
	t.closeOnce.Do(func() {
		close(t.done)
		t.wg.Wait()
	})
}

// run owns the one Pebble writer so readers never contend on index mutations.
func (t *toucher) run() {
	defer t.wg.Done()
	for {
		select {
		case <-t.done:
			t.drain()
			return
		case a := <-t.queue:
			t.apply(t.batch(a))
		}
	}
}

// drain commits work accepted before close, preserving a graceful daemon's LRU
// state without allowing new reads to extend shutdown.
func (t *toucher) drain() {
	for {
		select {
		case a := <-t.queue:
			t.apply(t.batch(a))
		default:
			return
		}
	}
}

// batch takes the immediately available work so one index commit refreshes
// several actions without waiting to fill a batch under light load.
func (t *toucher) batch(first ids.ActionID) []ids.ActionID {
	actions := make([]ids.ActionID, 0, touchBatchSize)
	actions = append(actions, first)
	for len(actions) < touchBatchSize {
		select {
		case a := <-t.queue:
			actions = append(actions, a)
		default:
			return actions
		}
	}
	return actions
}

// apply persists a batch and makes every action eligible for a later refresh.
func (t *toucher) apply(actions []ids.ActionID) {
	_, err := t.touch(actions, time.Now().UnixNano(), t.granularity)
	for _, a := range actions {
		t.forget(a)
	}
	if err != nil {
		t.logf("index touch: %v", err)
	}
}

// forget releases an action after its queued refresh was committed or dropped.
func (t *toucher) forget(a ids.ActionID) {
	t.pendingMu.Lock()
	delete(t.pending, a)
	t.pendingMu.Unlock()
}
