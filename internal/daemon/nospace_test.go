// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"syscall"
	"testing"
	"time"
)

// TestFullDiskRunsAnEvictionPassBeforeTheTick pins that a write which finds the
// disk full gets an eviction pass now rather than at the next tick.
//
// The tick is set to an hour so that only the disk-full request can explain a
// pass inside the test; on the default one-minute tick, every write in between
// would fail the same way.
func TestFullDiskRunsAnEvictionPassBeforeTheTick(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.DisableEviction = false
	cfg.EvictInterval = time.Hour
	cfg.MaxBytes = 1
	ts := startServer(t, cfg)
	putCached(t, ts.Server, testActionID(3), testOutputID(3), fill(4096))

	ts.cache.NoteWriteError(syscall.ENOSPC)

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := ts.idx.Stats()
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if st.Actions == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d actions still held 5s after a disk-full write, want an eviction pass to have run", st.Actions)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
