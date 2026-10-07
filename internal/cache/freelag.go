// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"time"
)

// freedLedger remembers what recent eviction passes freed, so the free-space
// floor can credit space the filesystem has not yet reported as free.
//
// Deleting a body does not raise statfs's free figure at once on every
// filesystem: ZFS returns the blocks a transaction group later and frees large
// objects asynchronously after that. A second floor-driven pass soon after the
// first reads the old figure, sees the same shortfall, and evicts it again — and
// passes come that close together when a full disk asks for one every 10
// seconds. Adding back what was freed inside the filesystem's lag window is what
// makes the second pass see the space the first one already released.
//
// Eviction runs from the ticker, from a full disk's early request and from gc,
// so the ledger is shared across goroutines and guarded by its own mutex.
type freedLedger struct {
	mu      sync.Mutex
	entries []freedEntry
}

// freedEntry is one pass's release: when it finished and how many recorded
// bytes it removed.
type freedEntry struct {
	at    time.Time
	bytes int64
}

// record notes that a pass freed bytes at the given instant.
func (l *freedLedger) record(at time.Time, bytes int64) {
	if bytes <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, freedEntry{at: at, bytes: bytes})
}

// recent reports the bytes freed within window of now, and drops what has aged
// out of it. A zero window credits nothing, which is right for a filesystem that
// returns space at once.
//
// Entries are appended in time order, so the expired ones are a prefix. Two
// concurrent passes can record slightly out of order; that leaves an expired
// entry behind for one more read at worst, never drops a live one.
func (l *freedLedger) recent(now time.Time, window time.Duration) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	cut := 0
	for cut < len(l.entries) && now.Sub(l.entries[cut].at) >= window {
		cut++
	}
	l.entries = l.entries[cut:]
	var sum int64
	for _, e := range l.entries {
		if now.Sub(e.at) < window {
			sum += e.bytes
		}
	}
	return sum
}
