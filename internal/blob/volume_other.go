// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package blob

import "errors"

// VolumeUsage is unavailable without statfs. Callers report the budget alone.
func VolumeUsage(string) (total, avail uint64, err error) {
	return 0, 0, errors.New("VolumeUsage: unsupported on this platform")
}
