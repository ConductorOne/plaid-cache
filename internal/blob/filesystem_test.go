// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"testing"
	"time"
)

// TestFilesystemForMagic pins the statfs f_type mapping, and that anything not
// positively recognised gets the cautious timings rather than the fast ones.
func TestFilesystemForMagic(t *testing.T) {
	for _, tc := range []struct {
		magic  uint32
		name   string
		settle time.Duration
		lag    time.Duration
	}{
		{0x2FC12FC1, "zfs", 10 * time.Second, 30 * time.Second},
		{0x58465342, "xfs", 0, 5 * time.Second},
		{0xEF53, "ext4", 0, 10 * time.Second},
		{0x01021994, "tmpfs", 0, 0},
		{0x9123683E, "btrfs", 10 * time.Second, 30 * time.Second},
		{0x794C7630, "overlay", 10 * time.Second, 30 * time.Second},
		{0x6969, "nfs", 10 * time.Second, 30 * time.Second},
		{0x12345678, "unknown", 10 * time.Second, 30 * time.Second},
		{0, "unknown", 10 * time.Second, 30 * time.Second},
	} {
		got := filesystemForMagic(tc.magic)
		want := Filesystem{Name: tc.name, AllocationSettle: tc.settle, FreeLag: tc.lag}
		if got != want {
			t.Errorf("magic %#x = %+v, want %+v", tc.magic, got, want)
		}
	}
}

// TestFilesystemForName pins the Darwin f_fstypename mapping: ZFS by name gets
// the ZFS profile, and nothing else is trusted to allocate at once.
func TestFilesystemForName(t *testing.T) {
	for name, want := range map[string]string{
		"zfs": "zfs", "apfs": "apfs", "hfs": "hfs", "msdos": "unknown", "": "unknown",
	} {
		got := filesystemForName(name)
		if got.Name != want {
			t.Errorf("%q = %q, want %q", name, got.Name, want)
		}
		if got.Name != "zfs" && (got.AllocationSettle != allocationSettleWindow || got.FreeLag != freeLagUnknown) {
			t.Errorf("%q got non-cautious timings %+v", name, got)
		}
	}
	if got, want := filesystemForName("zfs"), filesystemForMagic(magicZFS); got != want {
		t.Errorf("zfs by name = %+v, want %+v", got, want)
	}
}

// TestDetectFilesystemOnTheTestMachine pins that detection works wherever the
// tests run: no error, and a profile with a name. Which filesystem it is depends
// on the host, so that is not asserted.
func TestDetectFilesystemOnTheTestMachine(t *testing.T) {
	got, err := DetectFilesystem(t.TempDir())
	if err != nil {
		t.Fatalf("DetectFilesystem: %v", err)
	}
	if got.Name == "" {
		t.Fatalf("detected a profile with no name: %+v", got)
	}
	t.Logf("test machine: %v", got)
}

// TestOpenAssumesTheCautiousProfile pins that a store opened without a detected
// profile measures the way the store always has, and that OpenAs keeps the one
// it is given.
func TestOpenAssumesTheCautiousProfile(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := s.Filesystem(); got != UnknownFilesystem() {
		t.Fatalf("Open profile = %+v, want the cautious one", got)
	}
	xfs := filesystemForMagic(magicXFS)
	s, err = OpenAs(t.TempDir(), xfs)
	if err != nil {
		t.Fatalf("OpenAs: %v", err)
	}
	if got := s.Filesystem(); got != xfs {
		t.Fatalf("OpenAs profile = %+v, want %+v", got, xfs)
	}
}
