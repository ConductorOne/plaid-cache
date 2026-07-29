// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package blob

import (
	"io/fs"
	"syscall"
	"time"
)

// blockSize is the unit st_blocks counts in. It is fixed at 512 by POSIX and
// is unrelated to the filesystem's actual block size.
const blockSize = 512

// allocationSettleWindow is how long after a write the allocated-blocks figure
// must be left alone.
//
// ZFS defers allocation to the next transaction group, so a file written moments
// ago reports one block however large it is. Five seconds is the usual txg
// interval; doubling it costs nothing, because the only consequence of waiting
// longer is that a body keeps its provisional figure for one more eviction pass.
const allocationSettleWindow = 10 * time.Second

// diskBytes reports a provisional cost for a body that was just written.
//
// It is the larger of the allocated blocks and the logical length, because
// neither is safe alone at this moment. A cache is mostly small files, each
// rounded up to a full filesystem block, so the logical length understates real
// consumption. But the allocated figure is not populated yet: on ZFS a file
// written moments ago reports one block no matter how large it is, because
// allocation is deferred to the next transaction group. Measured here, 188
// freshly written objects totalling 33 MB reported 96 KB of blocks, an
// undercount of roughly 344x.
//
// So this figure is deliberately an overestimate, and it is only ever an
// overestimate: settledBytes replaces it once the allocation is real. That
// division matters, and getting it wrong was a 3x error — see the comment there.
func diskBytes(fi fs.FileInfo) int64 {
	size := fi.Size()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return size
	}
	if allocated := int64(st.Blocks) * blockSize; allocated > size {
		return allocated
	}
	return size
}

// settledBytes reports what a body actually costs, and whether that can be
// believed yet.
//
// The two figures diskBytes chooses between fail in opposite directions and on
// opposite timescales, which is why one max() cannot serve both:
//
//   - Deferred allocation makes the allocated figure far too small, and lasts
//     one transaction group.
//   - Compression makes the logical length far too large, and lasts forever.
//
// Taking the maximum handles the first and is defeated by the second, because
// the maximum of a real allocation and an inflated length is the inflated
// length. On a dataset compressing 3x that is not "evicting somewhat early", as
// this file used to claim: it is a permanent 3x overcount, and it was measured
// evicting 34748 entries while two thirds of the budget sat unused. Reported as
// issue #5.
//
// Once the write has settled the allocated figure is simply the truth — smaller
// than the length when the data compressed, larger when a small file was rounded
// up to a block — so it is used as-is.
func settledBytes(fi fs.FileInfo, now time.Time) (int64, bool) {
	size := fi.Size()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return size, false
	}
	if now.Sub(fi.ModTime()) < allocationSettleWindow {
		return diskBytes(fi), false
	}
	allocated := int64(st.Blocks) * blockSize
	if allocated <= 0 && size > 0 {
		// A file with length and no blocks at all is not something to believe:
		// either the filesystem does not report allocation, or the whole file is
		// a hole. Claiming it is free would let the budget ignore it entirely.
		return size, false
	}
	return allocated, true
}
