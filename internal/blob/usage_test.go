// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package blob

import (
	"io/fs"
	"syscall"
	"testing"
	"time"
)

// fakeInfo is an fs.FileInfo with a chosen length, modification time, and
// allocated block count.
//
// The decision under test turns on a filesystem behaviour — compression — that a
// test cannot rely on the host providing: zeros compress to nothing on ZFS and
// allocate in full on ext4, so a test that wrote a file and asserted on the
// result would pass or fail according to where it ran. Constructing the stat
// directly tests the rule instead of the host.
type fakeInfo struct {
	size    int64
	modTime time.Time
	blocks  int64
}

func (f fakeInfo) Name() string       { return "fake" }
func (f fakeInfo) Size() int64        { return f.size }
func (f fakeInfo) Mode() fs.FileMode  { return 0o644 }
func (f fakeInfo) ModTime() time.Time { return f.modTime }
func (f fakeInfo) IsDir() bool        { return false }
func (f fakeInfo) Sys() any           { return &syscall.Stat_t{Blocks: f.blocks} }

// TestFreshWritesAreNotTrusted pins the deferred-allocation guard.
//
// ZFS defers allocation to the next transaction group, so a file written moments
// ago reports one block however large it is. Believing that would let the budget
// record a 33 MB write as 96 KB, which is what it did before the maximum below
// was introduced.
func TestFreshWritesAreNotTrusted(t *testing.T) {
	fi := fakeInfo{size: 8 << 20, modTime: time.Now(), blocks: 1}
	got, settled := settledBytes(fi, time.Now())
	if settled {
		t.Fatal("a file written just now was reported as measurable")
	}
	if got != 8<<20 {
		t.Fatalf("provisional cost = %d, want the logical %d", got, int64(8<<20))
	}
}

// TestSettledCompressionIsBelieved is the fix for issue #5.
//
// Once the write has settled, an allocation smaller than the length is the truth
// rather than an artefact — the data compressed. Taking the maximum here, as the
// code used to, makes the compression case unreachable and overcounts by whatever
// the compression ratio happens to be: measured at 3x on a real cache, 84x for a
// body of zeros.
func TestSettledCompressionIsBelieved(t *testing.T) {
	const size = 8 << 20
	fi := fakeInfo{size: size, modTime: time.Now().Add(-time.Hour), blocks: 1} // 512 bytes allocated
	got, settled := settledBytes(fi, time.Now())
	if !settled {
		t.Fatal("an hour-old file was still reported as unmeasurable")
	}
	if got != 512 {
		t.Fatalf("settled cost = %d, want the allocated 512 — the maximum is back", got)
	}
}

// TestSettledSmallFilesCountTheirBlock pins the other direction: a small file
// costs a whole block, so the allocated figure exceeds the length and is still
// the one to use. This is the case the maximum was originally protecting.
func TestSettledSmallFilesCountTheirBlock(t *testing.T) {
	fi := fakeInfo{size: 10, modTime: time.Now().Add(-time.Hour), blocks: 8} // 4 KiB
	got, settled := settledBytes(fi, time.Now())
	if !settled || got != 4096 {
		t.Fatalf("settled cost = %d (settled=%v), want 4096", got, settled)
	}
}

// TestNoBlocksAtAllIsNotTrusted pins that a file with length and no allocation is
// refused rather than treated as free. Either the filesystem does not report
// allocation or the file is entirely a hole; calling it free would let the budget
// ignore it completely.
func TestNoBlocksAtAllIsNotTrusted(t *testing.T) {
	fi := fakeInfo{size: 1 << 20, modTime: time.Now().Add(-time.Hour), blocks: 0}
	got, settled := settledBytes(fi, time.Now())
	if settled {
		t.Fatal("a file reporting no allocation at all was believed")
	}
	if got != 1<<20 {
		t.Fatalf("cost = %d, want the logical %d", got, int64(1<<20))
	}
}

// TestSettleWindowBoundary pins that the window is what decides, not the values.
func TestSettleWindowBoundary(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		age  time.Duration
		want bool
	}{
		{"just written", 0, false},
		{"inside the window", allocationSettleWindow - time.Second, false},
		{"past the window", allocationSettleWindow + time.Second, true},
	} {
		fi := fakeInfo{size: 1 << 20, modTime: now.Add(-tc.age), blocks: 8}
		if _, settled := settledBytes(fi, now); settled != tc.want {
			t.Errorf("%s: settled = %v, want %v", tc.name, settled, tc.want)
		}
	}
}
