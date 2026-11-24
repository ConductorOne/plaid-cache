// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
)

// withRefreshAfter sets how old a remote entry in local use may get.
func withRefreshAfter(d time.Duration) option {
	return func(c *config.Config) { c.RemoteRefreshAfter = d }
}

// uploaded puts an entry and waits for its upload, so the remote holds it
// before the test ages it.
func (tc *testCache) uploaded(t *testing.T, a ids.ActionID, o ids.OutputID, body []byte) {
	t.Helper()
	tc.put(t, a, o, body)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := tc.rem.storedAction(a); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("upload of %x never reached the remote", a[:4])
		}
		time.Sleep(time.Millisecond)
	}
}

// settleRefresh waits until the refresher has checked n actions, then closes
// the cache so any check still in flight finishes before the test asserts.
func (tc *testCache) settleRefresh(t *testing.T, n int64) RefreshSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for tc.cache.Refresh().Checked < n {
		if time.Now().After(deadline) {
			t.Fatalf("refresher checked %d actions, want %d", tc.cache.Refresh().Checked, n)
		}
		time.Sleep(time.Millisecond)
	}
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return tc.cache.Refresh()
}

// TestRefreshRewritesAStaleEntryOnALocalHit pins the fix for busy entries
// expiring from the shared tier: an entry hit locally whose remote record and
// body are older than the refresh window is written to the bucket again, body
// first, unconditionally, so an age-based lifecycle rule's clock restarts.
func TestRefreshRewritesAStaleEntryOnALocalHit(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(41), mkOutput(41)
	body := []byte("a body every build hits")
	tc.uploaded(t, a, o, body)
	old := time.Now().Add(-2 * time.Hour)
	tc.rem.age(a, o, old)

	if res := tc.get(t, a); res.Miss {
		t.Fatalf("local hit missed")
	}
	snap := tc.settleRefresh(t, 1)

	if snap.Rewritten != 1 || snap.Failed != 0 {
		t.Fatalf("refresh = %+v, want one rewrite and no failures", snap)
	}
	actionAt, objectAt := tc.rem.written(a, o)
	if !actionAt.After(old) || !objectAt.After(old) {
		t.Fatalf("written at action=%v object=%v, want both after %v", actionAt, objectAt, old)
	}
	if got, _ := tc.rem.storedObject(o); !bytes.Equal(got, body) {
		t.Fatalf("rewritten body = %q, want %q", got, body)
	}
	calls := tc.rem.callOrder()
	rw := slices.Index(calls, "TouchObject")
	if rw < 0 || slices.Contains(calls[rw:], "PutObject") || !slices.Contains(calls[rw:], "PutActionIf") {
		t.Fatalf("calls = %v, want TouchObject then PutActionIf", calls)
	}
}

// TestRefreshRepairsAnExpiredBody pins that a body the bucket has already lost
// is put back from the local copy.
func TestRefreshRepairsAnExpiredBody(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(42), mkOutput(42)
	body := []byte("a body the lifecycle rule took")
	tc.uploaded(t, a, o, body)
	tc.rem.mu.Lock()
	delete(tc.rem.objects, o)
	delete(tc.rem.objectWritten, o)
	tc.rem.mu.Unlock()

	tc.get(t, a)
	if snap := tc.settleRefresh(t, 1); snap.Rewritten != 1 {
		t.Fatalf("refresh = %+v, want one rewrite", snap)
	}
	if got, ok := tc.rem.storedObject(o); !ok || !bytes.Equal(got, body) {
		t.Fatalf("body after refresh = %q (present %v), want %q", got, ok, body)
	}
}

// TestRefreshRestoresABodyDeletedDuringTouch keeps the action record behind a
// body that actually exists, even if the object expires after the age check.
func TestRefreshRestoresABodyDeletedDuringTouch(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(49), mkOutput(49)
	body := []byte("expires during touch")
	tc.rem.onTouch = func() {
		tc.rem.mu.Lock()
		defer tc.rem.mu.Unlock()
		delete(tc.rem.objects, o)
		delete(tc.rem.objectWritten, o)
	}
	tc.uploaded(t, a, o, body)
	tc.rem.age(a, o, time.Now().Add(-2*time.Hour))

	tc.get(t, a)
	if snap := tc.settleRefresh(t, 1); snap.Rewritten != 1 || snap.Failed != 0 {
		t.Fatalf("refresh = %+v, want one rewrite and no failures", snap)
	}
	if got, ok := tc.rem.storedObject(o); !ok || !bytes.Equal(got, body) {
		t.Fatalf("restored body = %q (present %v), want %q", got, ok, body)
	}
	calls := tc.rem.callOrder()
	touch := slices.Index(calls, "TouchObject")
	if touch < 0 || !slices.Equal(calls[touch:], []string{"TouchObject", "PutObject", "PutActionIf"}) {
		t.Fatalf("calls = %v, want copy attempt followed by body restore and conditional record write", calls)
	}
}

// TestRefreshLeavesAFreshEntryAlone pins that a recently written entry costs
// a check and nothing more.
func TestRefreshLeavesAFreshEntryAlone(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(43), mkOutput(43)
	tc.uploaded(t, a, o, []byte("just uploaded"))

	tc.get(t, a)
	if snap := tc.settleRefresh(t, 1); snap.Rewritten != 0 {
		t.Fatalf("refresh = %+v, want no rewrite", snap)
	}
	if slices.Contains(tc.rem.callOrder(), "TouchObject") {
		t.Fatalf("calls = %v, want no TouchObject", tc.rem.callOrder())
	}
}

// TestRefreshDoesNotOverwriteAnotherMachinesFreshRecord pins that a fresh
// record naming a different output is left as it is.
func TestRefreshDoesNotOverwriteAnotherMachinesFreshRecord(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, mine, theirs := mkAction(44), mkOutput(44), mkOutput(45)
	tc.uploaded(t, a, mine, []byte("our output"))
	tc.rem.age(a, mine, time.Now().Add(-2*time.Hour))
	tc.rem.programAction(a, theirs, time.Now())

	tc.get(t, a)
	if snap := tc.settleRefresh(t, 1); snap.Rewritten != 0 {
		t.Fatalf("refresh = %+v, want no rewrite", snap)
	}
	if rec, _ := tc.rem.storedAction(a); rec.output != theirs {
		t.Fatalf("record names %v, want the other machine's %v", rec.output, theirs)
	}
}

// TestRefreshDoesNotRevertARecordWrittenDuringTheRefresh pins the race where
// the refresher reads action -> ours, another machine publishes action ->
// theirs while the body is being rewritten, and the refresher's record write
// would otherwise put ours back. The write is conditional on the ETag read, so
// it loses and the newer record stands.
func TestRefreshDoesNotRevertARecordWrittenDuringTheRefresh(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, mine, theirs := mkAction(49), mkOutput(49), mkOutput(50)
	tc.uploaded(t, a, mine, []byte("our stale output"))
	tc.rem.age(a, mine, time.Now().Add(-2*time.Hour))
	tc.rem.mu.Lock()
	tc.rem.onTouch = func() { tc.rem.programAction(a, theirs, time.Now()) }
	tc.rem.mu.Unlock()

	tc.get(t, a)
	snap := tc.settleRefresh(t, 1)
	if snap.Superseded != 1 || snap.Rewritten != 0 || snap.Failed != 0 {
		t.Fatalf("refresh = %+v, want one superseded record write", snap)
	}
	if rec, _ := tc.rem.storedAction(a); rec.output != theirs {
		t.Fatalf("record names %v, want the newer %v", rec.output, theirs)
	}
	if _, objectAt := tc.rem.written(a, mine); time.Since(objectAt) > time.Minute {
		t.Fatalf("body written at %v, want it refreshed even though the record was not", objectAt)
	}
}

// TestRefreshDoesNotOverwriteARecordCreatedDuringTheRefresh pins the same race
// when the record was absent at the read: the write is If-None-Match.
func TestRefreshDoesNotOverwriteARecordCreatedDuringTheRefresh(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, mine, theirs := mkAction(51), mkOutput(51), mkOutput(52)
	tc.uploaded(t, a, mine, []byte("ours, record lost"))
	tc.rem.mu.Lock()
	delete(tc.rem.actions, a)
	delete(tc.rem.actionWritten, a)
	delete(tc.rem.actionETag, a)
	tc.rem.objectWritten[mine] = time.Now().Add(-2 * time.Hour)
	tc.rem.onTouch = func() { tc.rem.programAction(a, theirs, time.Now()) }
	tc.rem.mu.Unlock()

	tc.get(t, a)
	if snap := tc.settleRefresh(t, 1); snap.Superseded != 1 {
		t.Fatalf("refresh = %+v, want one superseded record write", snap)
	}
	if rec, _ := tc.rem.storedAction(a); rec.output != theirs {
		t.Fatalf("record names %v, want the newer %v", rec.output, theirs)
	}
}

// TestRefreshFollowsPresenceProbes pins that FindMissingBlobs-style probes,
// which is how Bazel learns it need not re-upload a blob, count as use.
func TestRefreshFollowsPresenceProbes(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(46), mkOutput(46)
	tc.uploaded(t, a, o, []byte("probed, never read"))
	tc.rem.age(a, o, time.Now().Add(-2*time.Hour))

	if !tc.cache.Has(t.Context(), a) {
		t.Fatalf("Has = false, want true")
	}
	if snap := tc.settleRefresh(t, 1); snap.Rewritten != 1 {
		t.Fatalf("refresh = %+v, want one rewrite", snap)
	}
}

// TestRefreshChecksAnEntryOncePerWindow pins that repeated hits do not turn
// into repeated round trips.
func TestRefreshChecksAnEntryOncePerWindow(t *testing.T) {
	tc := newTestCache(t, withRemote(), withRefreshAfter(time.Hour))
	a, o := mkAction(47), mkOutput(47)
	tc.uploaded(t, a, o, []byte("hit many times"))
	for range 50 {
		tc.get(t, a)
	}
	if snap := tc.settleRefresh(t, 1); snap.Checked != 1 {
		t.Fatalf("refresh = %+v, want exactly one check", snap)
	}
}

// TestRefreshIsOffWithoutAWindow pins that a zero window disables refreshing.
func TestRefreshIsOffWithoutAWindow(t *testing.T) {
	tc := newTestCache(t, withRemote())
	a, o := mkAction(48), mkOutput(48)
	tc.uploaded(t, a, o, []byte("never refreshed"))
	tc.rem.age(a, o, time.Now().Add(-1000*time.Hour))
	tc.get(t, a)
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if slices.Contains(tc.rem.callOrder(), "Freshness") {
		t.Fatalf("calls = %v, want no Freshness probe", tc.rem.callOrder())
	}
}
