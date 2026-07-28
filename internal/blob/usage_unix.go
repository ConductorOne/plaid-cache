// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package blob

import (
	"io/fs"
	"syscall"
)

// blockSize is the unit st_blocks counts in. It is fixed at 512 by POSIX and
// is unrelated to the filesystem's actual block size.
const blockSize = 512

// diskBytes reports what a file costs the filesystem, for the byte budget
// that drives eviction.
//
// It is the larger of the allocated blocks and the logical length, and it is
// deliberately the larger of the two rather than either one alone.
//
// Allocated blocks alone are not safe. A cache is mostly small files, each
// rounded up to a full filesystem block, so logical length understates real
// consumption — which argues for st_blocks. But st_blocks is not always
// populated when we ask: on ZFS, a file written moments ago reports one block
// no matter how large it is, because allocation is deferred to the next
// transaction group. Measured here, 188 freshly written objects totalling
// 33 MB reported 96 KB of blocks, an undercount of roughly 344x. A budget
// built on that number does not bound anything.
//
// Logical length alone is not safe either, for the small-file reason above.
//
// Taking the maximum is wrong only in the direction that is safe: on a
// compressing filesystem the true cost can be below both figures, so the
// budget evicts somewhat earlier than strictly necessary. Overshooting a disk
// ceiling is a bug; undershooting it is a tuning question.
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
