// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package blob

import "io/fs"

// diskBytes reports what a file actually costs the filesystem.
//
// Platforms without stat(2)'s st_blocks expose no portable allocated-size
// field through io/fs, so the logical size is the best available estimate.
// It undercounts a cache of small files, which makes the byte budget slightly
// generous here — acceptable, since the budget is an operational ceiling
// rather than a correctness invariant.
func diskBytes(fi fs.FileInfo) int64 { return fi.Size() }
