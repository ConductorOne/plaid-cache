// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package blob

import (
	"fmt"
	"syscall"
)

// VolumeUsage reports the total and available bytes of the filesystem holding
// dir.
//
// The byte budget counts what the cache stores; this counts what the disk has.
// The two can disagree by a lot — compression, other tenants of the volume,
// snapshots — and an operator deciding whether a ceiling is set sensibly is
// reading the second one whether or not it is shown.
func VolumeUsage(dir string) (total, avail uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, fmt.Errorf("VolumeUsage: %w", err)
	}
	// Bsize is signed on Linux and unsigned on Darwin; the conversion keeps this
	// valid on both.
	bs := uint64(st.Bsize) //nolint:unconvert // signedness differs by platform
	return bs * st.Blocks, bs * st.Bavail, nil
}
