// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package blob

import (
	"io/fs"
	"time"
)

// diskBytes reports what a file actually costs the filesystem.
//
// Platforms without stat(2)'s st_blocks expose no portable allocated-size
// field through io/fs, so the logical size is the best available estimate.
// It undercounts a cache of small files, which makes the byte budget slightly
// generous here — acceptable, since the budget is an operational ceiling
// rather than a correctness invariant.
func diskBytes(fi fs.FileInfo) int64 { return fi.Size() }

// settledBytes reports the logical size and that it is not an allocation figure.
//
// Reporting it as untrusted keeps the re-measuring pass from replacing a
// provisional figure with the same number it already has, so on these platforms
// the budget stays logical — which is all it can be without st_blocks.
func settledBytes(fi fs.FileInfo, _ time.Time) (int64, bool) { return fi.Size(), false }
