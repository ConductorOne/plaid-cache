// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package daemon

import "os/exec"

// detach is a no-op on platforms with no session concept. The daemon still
// outlives its parent because the parent never waits on it.
func detach(cmd *exec.Cmd) {}
