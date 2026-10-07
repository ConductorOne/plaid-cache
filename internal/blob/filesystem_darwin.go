// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package blob

import (
	"fmt"
	"syscall"
)

// DetectFilesystem reports the profile of the filesystem holding dir.
//
// Darwin names its filesystems rather than numbering them, so this reads
// f_fstypename where Linux reads f_type. On error the cautious profile comes
// back with it.
func DetectFilesystem(dir string) (Filesystem, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return UnknownFilesystem(), fmt.Errorf("DetectFilesystem: %w", err)
	}
	name := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	return filesystemForName(string(name)), nil
}
