// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package blob

// DetectFilesystem reports the cautious profile: there is no portable way to
// name a filesystem here. It is not an error, because nothing failed — the
// cache simply keeps the conservative accounting it has always used.
func DetectFilesystem(string) (Filesystem, error) {
	return UnknownFilesystem(), nil
}
