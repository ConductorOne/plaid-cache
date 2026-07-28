// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"errors"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// putAged stores one entry with a chosen last-used timestamp.
func putAged(t *testing.T, ix *Index, seed byte, lastUsed int64) {
	t.Helper()
	var a ids.ActionID
	var o ids.OutputID
	a[0], o[0] = seed, seed
	if err := ix.Put(a, Entry{
		OutputID:   o,
		Size:       1,
		CreatedAt:  lastUsed,
		LastUsedAt: lastUsed,
	}, 512); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// TestAgeSpanEmptyIndex pins that an empty index reports no span rather than a
// zero one, since a zero timestamp would read as an entry from 1970.
func TestAgeSpanEmptyIndex(t *testing.T) {
	ix := openTemp(t)
	oldest, newest, ok, err := ix.AgeSpan()
	if err != nil {
		t.Fatalf("AgeSpan: %v", err)
	}
	if ok {
		t.Fatalf("ok = true on an empty index (oldest=%d newest=%d)", oldest, newest)
	}
}

// TestAgeSpanReportsExtremes pins that the span is the minimum and maximum
// last-used time, which is what makes it answerable in two seeks: the LRU
// keyspace is ordered by that timestamp.
func TestAgeSpanReportsExtremes(t *testing.T) {
	ix := openTemp(t)
	// Inserted out of order on purpose: the answer must come from key order,
	// not insertion order.
	putAged(t, ix, 2, 5_000)
	putAged(t, ix, 1, 1_000)
	putAged(t, ix, 3, 9_000)
	putAged(t, ix, 4, 7_000)

	oldest, newest, ok, err := ix.AgeSpan()
	if err != nil {
		t.Fatalf("AgeSpan: %v", err)
	}
	if !ok {
		t.Fatal("ok = false with four entries")
	}
	if oldest != 1_000 {
		t.Fatalf("oldest = %d, want 1000", oldest)
	}
	if newest != 9_000 {
		t.Fatalf("newest = %d, want 9000", newest)
	}
}

// TestAgeSpanSingleEntry pins that one entry is its own oldest and newest,
// rather than leaving newest unset.
func TestAgeSpanSingleEntry(t *testing.T) {
	ix := openTemp(t)
	putAged(t, ix, 7, 4_242)

	oldest, newest, ok, err := ix.AgeSpan()
	if err != nil {
		t.Fatalf("AgeSpan: %v", err)
	}
	if !ok || oldest != 4_242 || newest != 4_242 {
		t.Fatalf("AgeSpan = (%d, %d, %v), want (4242, 4242, true)", oldest, newest, ok)
	}
}

// TestAgeSpanFollowsTouch pins that the span tracks last-used, not creation:
// touching the oldest entry must move the lower bound forward.
func TestAgeSpanFollowsTouch(t *testing.T) {
	ix := openTemp(t)
	putAged(t, ix, 1, 1_000)
	putAged(t, ix, 2, 5_000)

	var a ids.ActionID
	a[0] = 1
	// Zero granularity forces the write regardless of how recent the stored
	// timestamp is.
	if wrote, err := ix.Touch(a, 8_000, 0); err != nil {
		t.Fatalf("Touch: %v", err)
	} else if !wrote {
		t.Fatal("Touch did not write")
	}

	oldest, newest, ok, err := ix.AgeSpan()
	if err != nil {
		t.Fatalf("AgeSpan: %v", err)
	}
	if !ok {
		t.Fatal("ok = false")
	}
	if oldest != 5_000 {
		t.Fatalf("oldest = %d, want 5000; the touched entry should no longer be the oldest", oldest)
	}
	if newest != 8_000 {
		t.Fatalf("newest = %d, want 8000", newest)
	}
}

// TestAgeSpanAfterCloseErrors pins that a closed index reports an error rather
// than a plausible-looking empty span.
func TestAgeSpanAfterCloseErrors(t *testing.T) {
	ix := openTemp(t)
	putAged(t, ix, 1, 1_000)
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, ok, err := ix.AgeSpan(); err == nil {
		t.Fatalf("AgeSpan after Close returned ok=%v, want an error", ok)
	} else if !errors.Is(err, ErrClosed) {
		t.Fatalf("AgeSpan error = %v, want ErrClosed", err)
	}
}
