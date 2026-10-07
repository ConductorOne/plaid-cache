// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"testing"
	"time"
)

// TestFreedLedgerWindow pins what the ledger credits: entries inside the window
// count, entries at or past it do not and are dropped, and a zero window — a
// filesystem that frees at once — credits nothing.
func TestFreedLedgerWindow(t *testing.T) {
	var l freedLedger
	now := time.Now()
	l.record(now.Add(-40*time.Second), 1000)
	l.record(now.Add(-10*time.Second), 200)
	l.record(now, 30)
	l.record(now, 0) // nothing freed is not an entry

	if got := l.recent(now, 30*time.Second); got != 230 {
		t.Fatalf("30s window credited %d, want 230", got)
	}
	if n := len(l.entries); n != 2 {
		t.Fatalf("ledger holds %d entries after expiry, want 2", n)
	}
	if got := l.recent(now, 0); got != 0 {
		t.Fatalf("zero window credited %d, want 0", got)
	}
}

// TestFreedLedgerConcurrent pins that the ledger is safe to record into and read
// from at once, as eviction does from the ticker, a full disk's request and gc.
// It is meaningful under -race.
func TestFreedLedgerConcurrent(t *testing.T) {
	var l freedLedger
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				l.record(time.Now(), 1)
				_ = l.recent(time.Now(), time.Minute)
			}
		})
	}
	wg.Wait()
	if got := l.recent(time.Now(), time.Minute); got != 800 {
		t.Fatalf("credited %d after 800 records, want 800", got)
	}
}
