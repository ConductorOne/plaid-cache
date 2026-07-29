// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

// TestActivitySurvivesReopening is the whole point: a counter that dies with the
// process answers "what has this process seen", which for a daemon that exits on
// a 30-minute idle timeout is not what anyone asking about the cache means.
func TestActivitySurvivesReopening(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 7, 28, 10, 30, 0, 0, time.UTC)

	ix := openIndex(t, dir)
	if err := ix.RecordActivity(Activity{GetLocalHit: 7, GetMiss: 3, Put: 3}, now); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ix = openIndex(t, dir)
	got, since, err := ix.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.GetLocalHit != 7 || got.GetMiss != 3 || got.Put != 3 {
		t.Fatalf("after reopening: %+v, want the counts recorded before", got)
	}
	if since != now.UnixNano() {
		t.Fatalf("since = %d, want %d", since, now.UnixNano())
	}
}

// TestLifetimeStartTimeIsTheFirstRecord pins that the start of the history stays
// where it was.
//
// It is the denominator of "since when": a start time that advanced with every
// flush would report a fortnight of counters as though they happened in the last
// minute, which is a worse answer than none.
func TestLifetimeStartTimeIsTheFirstRecord(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	first := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	later := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, first); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, later); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	_, since, err := ix.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if since != first.UnixNano() {
		t.Fatalf("since = %s, want the first record at %s",
			time.Unix(0, since).UTC(), first)
	}
}

// TestActivityAccumulates pins that each record adds to the total rather than
// replacing it, which is what lets several processes count into one history.
func TestActivityAccumulates(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	at := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		if err := ix.RecordActivity(Activity{GetLocalHit: 5}, at); err != nil {
			t.Fatalf("RecordActivity %d: %v", i, err)
		}
	}
	got, _, err := ix.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.GetLocalHit != 15 {
		t.Fatalf("GetLocalHit = %d, want 15", got.GetLocalHit)
	}
}

// TestActivityBucketsByHour pins the per-hour history, which is what makes a
// rate answerable. One lifetime total cannot say whether the cache is working
// now or worked well a week ago.
func TestActivityBucketsByHour(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	base := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, base.Add(5*time.Minute)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.RecordActivity(Activity{GetLocalHit: 2}, base.Add(55*time.Minute)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.RecordActivity(Activity{GetMiss: 4}, base.Add(90*time.Minute)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}

	buckets, err := ix.ActivitySince(base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ActivitySince: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("got %d buckets, want 2 (two distinct hours)", len(buckets))
	}
	if buckets[0].Hour != base.Unix() || buckets[0].Activity.GetLocalHit != 3 {
		t.Fatalf("first bucket = %+v, want hour %d with 3 local hits", buckets[0], base.Unix())
	}
	if buckets[1].Hour != base.Add(time.Hour).Unix() || buckets[1].Activity.GetMiss != 4 {
		t.Fatalf("second bucket = %+v, want the next hour with 4 misses", buckets[1])
	}
}

// TestActivitySinceExcludesOlderHours pins the window, so a rate asked for one
// day is not silently computed over a fortnight.
func TestActivitySinceExcludesOlderHours(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, now.Add(-48*time.Hour)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.RecordActivity(Activity{GetLocalHit: 2}, now); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}

	buckets, err := ix.ActivitySince(now.Add(-24 * time.Hour))
	if err != nil {
		t.Fatalf("ActivitySince: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Activity.GetLocalHit != 2 {
		t.Fatalf("got %+v, want only the recent hour", buckets)
	}
	// The lifetime total still counts the old hour, so a total never shrinks
	// when the window rolls past something.
	total, _, err := ix.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if total.GetLocalHit != 3 {
		t.Fatalf("lifetime GetLocalHit = %d, want 3", total.GetLocalHit)
	}
}

// TestActivityHistoryIsBounded pins the retention. A cache whose whole purpose is
// bounded growth must not accumulate its own telemetry forever.
func TestActivityHistoryIsBounded(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, now.Add(-ActivityRetention-2*time.Hour)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, now); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}

	// Reach back further than retention: the old bucket must be gone, dropped by
	// the write that came after it rather than by any sweep.
	buckets, err := ix.ActivitySince(now.Add(-10 * ActivityRetention))
	if err != nil {
		t.Fatalf("ActivitySince: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("got %d buckets, want the expired one dropped", len(buckets))
	}
	if buckets[0].Hour != now.Unix() {
		t.Fatalf("kept the wrong bucket: %+v", buckets[0])
	}
}

// TestRecordingNothingWritesNothing pins that an idle daemon's periodic flush is
// free, rather than rewriting the same record every minute for as long as the
// process lives.
func TestRecordingNothingWritesNothing(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	at := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	if err := ix.RecordActivity(Activity{}, at); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if _, since, err := ix.TotalActivity(); err != nil {
		t.Fatalf("TotalActivity: %v", err)
	} else if since != 0 {
		t.Fatalf("an empty delta started the history at %d", since)
	}
	buckets, err := ix.ActivitySince(at.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ActivitySince: %v", err)
	}
	if len(buckets) != 0 {
		t.Fatalf("an empty delta wrote %d buckets", len(buckets))
	}
}

// TestActivityOnAClosedIndexIsAnErrorNotAPanic pins the guard. Pebble panics on a
// closed database rather than returning an error, and the cache flushes its
// counters as it closes — so without this, an index closed first would take the
// process down over telemetry.
func TestActivityOnAClosedIndexIsAnErrorNotAPanic(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := ix.RecordActivity(Activity{GetLocalHit: 1}, time.Now()); err == nil {
		t.Fatal("RecordActivity on a closed index returned no error")
	}
	if _, _, err := ix.TotalActivity(); err == nil {
		t.Fatal("TotalActivity on a closed index returned no error")
	}
	if _, err := ix.ActivitySince(time.Now().Add(-time.Hour)); err == nil {
		t.Fatal("ActivitySince on a closed index returned no error")
	}
}

// TestActivityIsSeparateFromTheEntryCounters pins that the two live in different
// records. An unclean shutdown rebuilds the entry counters by rescanning, and
// activity cannot be rebuilt from anything — so a rebuild must not touch it.
func TestActivityIsSeparateFromTheEntryCounters(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	ix := openIndex(t, dir)
	if err := ix.RecordActivity(Activity{GetLocalHit: 9}, at); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	// Die without a clean shutdown, exactly as a killed process would.
	if err := ix.closeAbruptly(); err != nil {
		t.Fatalf("closeAbruptly: %v", err)
	}

	ix = openIndex(t, dir)
	got, _, err := ix.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.GetLocalHit != 9 {
		t.Fatalf("GetLocalHit = %d after an unclean shutdown, want 9 — the rebuild ate the history", got.GetLocalHit)
	}
}

// TestHitRateIsUnknownWithoutLookups pins that an idle cache does not report a
// 0.0%% hit rate, which reads as a broken one.
func TestHitRateIsUnknownWithoutLookups(t *testing.T) {
	if _, ok := (Activity{}).HitRate(); ok {
		t.Fatal("HitRate over no lookups claims to be known")
	}
	rate, ok := Activity{GetLocalHit: 3, GetMiss: 1}.HitRate()
	if !ok || rate != 0.75 {
		t.Fatalf("HitRate = %v, %v; want 0.75, true", rate, ok)
	}
}

// openIndex opens an index and closes it at test end unless it is closed sooner.
func openIndex(t *testing.T, dir string) *Index {
	t.Helper()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix
}
