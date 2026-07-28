// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package daemon

import (
	"os/exec"
	"syscall"
)

// detach puts the daemon in its own session.
//
// Without this the daemon inherits the caller's process group, so a Ctrl-C
// aimed at a build would also kill the cache that other builds are using.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
