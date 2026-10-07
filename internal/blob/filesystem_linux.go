// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package blob

import (
	"fmt"
	"syscall"
)

// DetectFilesystem reports the profile of the filesystem holding dir.
//
// On error the cautious profile comes back with it, so a caller that only logs
// the error can use the profile as it is.
func DetectFilesystem(dir string) (Filesystem, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return UnknownFilesystem(), fmt.Errorf("DetectFilesystem: %w", err)
	}
	// Truncating is the point: see filesystemForMagic.
	return filesystemForMagic(uint32(st.Type)), nil //nolint:gosec // f_type magics are 32-bit
}
