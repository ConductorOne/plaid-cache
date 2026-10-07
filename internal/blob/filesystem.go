// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"fmt"
	"time"
)

// Filesystem describes how the filesystem holding the cache reports space, as
// far as the cache's accounting depends on it.
//
// Two behaviours differ by filesystem and both bite the byte budget: when a
// freshly written file's allocated size becomes believable, and how long freed
// blocks take to show up as free space. Getting either wrong in the generous
// direction is the failure that matters, so anything not positively recognised
// gets the cautious values.
type Filesystem struct {
	// Name is a short lower-case label for logs, status and metrics: "zfs",
	// "xfs", "ext4", or "unknown".
	Name string

	// AllocationSettle is how long after a write the allocated-blocks figure
	// must be left alone before it is believed. Zero means at once.
	AllocationSettle time.Duration

	// FreeLag is how long space freed by deleting a body may take to appear in
	// statfs. A free-space check made inside it credits what was just freed
	// rather than evicting for the same shortfall twice.
	FreeLag time.Duration
}

// String renders the profile the way the startup log line wants it.
func (f Filesystem) String() string {
	return fmt.Sprintf("%s (allocation settle %v, free lag %v)", f.Name, f.AllocationSettle, f.FreeLag)
}

// allocationSettleWindow is how long after a write the allocated-blocks figure
// must be left alone on a filesystem that defers allocation, or one not
// recognised.
//
// ZFS defers allocation to the next transaction group, so a file written moments
// ago reports one block however large it is. Five seconds is the usual txg
// interval; doubling it costs nothing, because the only consequence of waiting
// longer is that a body keeps its provisional figure for one more eviction pass.
const allocationSettleWindow = 10 * time.Second

// Free-lag windows, per filesystem.
//
// Deleting a file does not return its blocks to statfs at once everywhere. ZFS
// frees in the transaction group after the unlink and then frees large objects
// asynchronously, so a 5s txg plus the async free queue is comfortably inside
// 30s. XFS on recent kernels inactivates unlinked inodes in a background worker
// that runs within a few seconds. ext4 returns blocks when the journal commits,
// every 5s by default, so twice that. A window too long only delays a needed
// second eviction; a window too short evicts the same shortfall twice, which is
// the bug this exists to stop — so unknown filesystems get the longest.
const (
	freeLagZFS     = 30 * time.Second
	freeLagXFS     = 5 * time.Second
	freeLagExt4    = 10 * time.Second
	freeLagUnknown = 30 * time.Second
)

// Statfs magic numbers, from linux/magic.h. Kept as plain numbers rather than
// taken from x/sys so this file builds and is testable on every platform.
const (
	magicZFS     = 0x2FC12FC1
	magicXFS     = 0x58465342
	magicExt     = 0xEF53 // ext2, ext3 and ext4 share it
	magicBtrfs   = 0x9123683E
	magicTmpfs   = 0x01021994
	magicOverlay = 0x794C7630
	magicNFS     = 0x6969
)

// UnknownFilesystem is the cautious profile, for a filesystem not recognised
// or not detectable. It is also what Open assumes.
func UnknownFilesystem() Filesystem {
	return cautious("unknown")
}

// cautious is the profile for a filesystem whose behaviour is not pinned down:
// the ZFS settle window, and the longest free lag.
//
// btrfs, overlayfs and NFS get it under their own names. btrfs counts delayed
// allocation in st_blocks but compresses at writeback and frees at transaction
// commit (30s by default); overlayfs behaves like whatever its upper layer is;
// NFS reports what the server says, whenever it says it. None of them is safe to
// believe early.
func cautious(name string) Filesystem {
	return Filesystem{Name: name, AllocationSettle: allocationSettleWindow, FreeLag: freeLagUnknown}
}

// filesystemForMagic maps a Linux statfs f_type to a profile.
//
// It takes a uint32 because every magic number fits in one and f_type's Go type
// varies by architecture — int64 on amd64, int32 on 386, where btrfs's magic
// would otherwise be negative.
//
// XFS and ext4 count delayed-allocation reservations in st_blocks the moment
// the data is written (xfs_vn_getattr adds i_delayed_blks, ext4_getattr adds the
// reserved delalloc blocks), so a fresh body's allocation is believable at once
// and needs no settle window. tmpfs allocates pages as they are written and
// frees them on the last unlink, so it has neither delay.
func filesystemForMagic(magic uint32) Filesystem {
	switch magic {
	case magicZFS:
		return Filesystem{Name: "zfs", AllocationSettle: allocationSettleWindow, FreeLag: freeLagZFS}
	case magicXFS:
		return Filesystem{Name: "xfs", AllocationSettle: 0, FreeLag: freeLagXFS}
	case magicExt:
		return Filesystem{Name: "ext4", AllocationSettle: 0, FreeLag: freeLagExt4}
	case magicTmpfs:
		return Filesystem{Name: "tmpfs", AllocationSettle: 0, FreeLag: 0}
	case magicBtrfs:
		return cautious("btrfs")
	case magicOverlay:
		return cautious("overlay")
	case magicNFS:
		return cautious("nfs")
	default:
		return UnknownFilesystem()
	}
}

// filesystemForName maps a Darwin statfs f_fstypename to a profile.
//
// APFS and HFS+ both allocate lazily for some writes and are not where a large
// cache is run, so they keep the cautious values under their own names. OpenZFS
// on macOS is ZFS.
func filesystemForName(name string) Filesystem {
	switch name {
	case "zfs":
		return filesystemForMagic(magicZFS)
	case "apfs":
		return cautious("apfs")
	case "hfs":
		return cautious("hfs")
	default:
		return UnknownFilesystem()
	}
}
