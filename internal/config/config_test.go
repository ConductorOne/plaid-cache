// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestParseBytesUnits pins that SI and IEC suffixes are distinguished rather
// than conflated, since treating GB as 2^30 misstates a disk budget by 7%.
func TestParseBytesUnits(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"1B", 1},
		{"1K", 1 << 10},
		{"1KB", 1000},
		{"1KiB", 1 << 10},
		{"1MB", 1e6},
		{"1MiB", 1 << 20},
		{"20GB", 20e9},
		{"20GiB", 20 << 30},
		{"1TiB", 1 << 40},
		{"0", 0},
		{"  512MiB  ", 512 << 20},
		{"1.5GiB", 1610612736},
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		if err != nil {
			t.Fatalf("ParseBytes(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("ParseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestParseBytesRejectsGarbage pins that an unparseable size is an error, not
// a silent fallback to a default that would disable the disk ceiling.
func TestParseBytesRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "   ", "abc", "12XB", "-5", "-1GiB", "1.2.3GB", "GB"} {
		if got, err := ParseBytes(in); err == nil {
			t.Fatalf("ParseBytes(%q) = %d, want error", in, got)
		}
	}
}

// TestFormatBytes pins the human-facing rendering used by the status command.
func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1 << 10, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{20 << 30, "20.0 GiB"},
	}
	for _, c := range cases {
		if got := FormatBytes(c.in); got != c.want {
			t.Fatalf("FormatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestLoadDefaults pins that a bare environment yields the documented
// defaults and, critically, no remote tier: there is no bucket worth guessing.
func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	t.Setenv("PLAID_GOCACHE_DIR", dir)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.MaxBytes != defaultMaxBytes {
		t.Fatalf("MaxBytes = %d, want %d", c.MaxBytes, int64(defaultMaxBytes))
	}
	if c.TTL != defaultTTL {
		t.Fatalf("TTL = %v, want %v", c.TTL, defaultTTL)
	}
	if c.TouchGranularity != defaultTouchGranularity {
		t.Fatalf("TouchGranularity = %v, want %v", c.TouchGranularity, defaultTouchGranularity)
	}
	if c.RemoteEnabled() {
		t.Fatalf("RemoteEnabled = true with no bucket set, want false")
	}
	if c.S3Bucket != "" || c.S3Region != "" || c.S3Prefix != "" {
		t.Fatalf("remote settings are non-empty by default: %+v", c)
	}
	if c.Log != LogError {
		t.Fatalf("Log = %v, want error", c.Log)
	}
	// The Bazel listener is opt-in: a second listener is something to ask for
	// rather than something to find running.
	if c.BazelAddr != "" {
		t.Fatalf("BazelAddr = %q by default, want it empty", c.BazelAddr)
	}
	if c.DisableBazelVerify {
		t.Fatalf("DisableBazelVerify = true by default, want uploads verified")
	}
	if c.UploadQueueDepth != defaultUploadQueueDepth {
		t.Fatalf("UploadQueueDepth = %d, want %d", c.UploadQueueDepth, defaultUploadQueueDepth)
	}
	// Waiting for room in the upload queue is opt-in. A default that blocked
	// would turn a full queue from a lost cache entry into a stalled build,
	// which is the one thing this tool promises not to do.
	if c.UploadBlockTimeout != 0 {
		t.Fatalf("UploadBlockTimeout = %v by default, want 0 (drop rather than wait)", c.UploadBlockTimeout)
	}
}

// TestLoadDirChain pins the PLAID_GOCACHE_DIR > XDG_CACHE_HOME precedence.
func TestLoadDirChain(t *testing.T) {
	clearEnv(t)
	xdg := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", xdg)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := filepath.Join(xdg, "plaid-cache"); c.Dir != want {
		t.Fatalf("Dir = %q, want %q", c.Dir, want)
	}

	explicit := t.TempDir()
	t.Setenv("PLAID_GOCACHE_DIR", explicit)
	c, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Dir != explicit {
		t.Fatalf("Dir = %q, want %q (PLAID_GOCACHE_DIR must win)", c.Dir, explicit)
	}
}

// TestLoadRejectsBadValues pins that every typo'd setting fails loudly. A
// silently-ignored PLAID_GOCACHE_MAX_BYTES would let the cache fill the disk.
func TestLoadRejectsBadValues(t *testing.T) {
	cases := []struct{ name, value string }{
		{"PLAID_GOCACHE_MAX_BYTES", "twenty gigs"},
		{"PLAID_GOCACHE_TTL", "7 days"},
		{"PLAID_GOCACHE_TTL", "-1h"},
		{"PLAID_GOCACHE_TOUCH_GRANULARITY", "soon"},
		{"PLAID_GOCACHE_MIN_UPLOAD_SIZE", "small"},
		{"PLAID_GOCACHE_UPLOAD_CONCURRENCY", "many"},
		{"PLAID_GOCACHE_UPLOAD_CONCURRENCY", "0"},
		{"PLAID_GOCACHE_UPLOAD_QUEUE_DEPTH", "deep"},
		{"PLAID_GOCACHE_UPLOAD_QUEUE_DEPTH", "0"},
		{"PLAID_GOCACHE_UPLOAD_BLOCK_TIMEOUT", "a while"},
		{"PLAID_GOCACHE_UPLOAD_BLOCK_TIMEOUT", "-1s"},
		{"PLAID_GOCACHE_IDLE_TIMEOUT", "forever"},
		{"PLAID_GOCACHE_EVICT_INTERVAL", "often"},
		{"PLAID_GOCACHE_DISABLE_EVICTION", "true"},
		{"PLAID_GOCACHE_DISABLE_EVICTION", "yes"},
		{"PLAID_GOCACHE_DISABLE_DAEMON", "on"},
		{"PLAID_GOCACHE_LOG", "verbose"},
	}
	for _, c := range cases {
		t.Run(c.name+"="+c.value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
			t.Setenv(c.name, c.value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load with %s=%q succeeded, want error", c.name, c.value)
			} else if !strings.Contains(err.Error(), c.name) {
				t.Fatalf("Load error %q does not name the offending variable %s", err, c.name)
			}
		})
	}
}

// TestLoadZeroDisablesConstraint pins that zero is a legal, meaningful value:
// it turns off one eviction constraint without turning off the other.
func TestLoadZeroDisablesConstraint(t *testing.T) {
	clearEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "0")
	t.Setenv("PLAID_GOCACHE_TTL", "0s")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.MaxBytes != 0 {
		t.Fatalf("MaxBytes = %d, want 0", c.MaxBytes)
	}
	if c.TTL != 0 {
		t.Fatalf("TTL = %v, want 0", c.TTL)
	}
}

// TestLoadBoolAndLog pins the accepted spellings of the escape hatches.
func TestLoadBoolAndLog(t *testing.T) {
	clearEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_DISABLE_EVICTION", "1")
	t.Setenv("PLAID_GOCACHE_DISABLE_DAEMON", "0")
	t.Setenv("PLAID_GOCACHE_LOG", "debug")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.DisableEviction {
		t.Fatalf("DisableEviction = false, want true")
	}
	if c.DisableDaemon {
		t.Fatalf("DisableDaemon = true, want false")
	}
	if c.Log != LogDebug {
		t.Fatalf("Log = %v, want debug", c.Log)
	}
}

// TestPathsAreUnderDir pins that every derived path stays inside the cache
// root, so that `clean` removing Dir removes everything the tool created.
func TestPathsAreUnderDir(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	t.Setenv("PLAID_GOCACHE_DIR", dir)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for name, p := range map[string]string{
		"BlobDir":    c.BlobDir(),
		"IndexDir":   c.IndexDir(),
		"SocketPath": c.SocketPath(),
		"LogPath":    c.LogPath(),
	} {
		rel, err := filepath.Rel(dir, p)
		if err != nil || strings.HasPrefix(rel, "..") {
			t.Fatalf("%s = %q is not under Dir %q", name, p, dir)
		}
	}
}

// TestParseDurationAcceptsGoSyntax pins that TTL uses Go duration syntax.
func TestParseDurationAcceptsGoSyntax(t *testing.T) {
	clearEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_TTL", "36h30m")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := 36*time.Hour + 30*time.Minute; c.TTL != want {
		t.Fatalf("TTL = %v, want %v", c.TTL, want)
	}
}

// clearEnv unsets every variable plaid-cache reads, so a test observes only
// what it sets. The ambient environment of a developer machine or CI runner
// would otherwise leak into the result.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, n := range []string{
		"PLAID_GOCACHE_DIR",
		"PLAID_GOCACHE_MAX_BYTES",
		"PLAID_GOCACHE_TTL",
		"PLAID_GOCACHE_TOUCH_GRANULARITY",
		"PLAID_GOCACHE_S3_BUCKET",
		"PLAID_GOCACHE_S3_REGION",
		"PLAID_GOCACHE_S3_PREFIX",
		"PLAID_GOCACHE_S3_ENDPOINT_URL",
		"PLAID_GOCACHE_MIN_UPLOAD_SIZE",
		"PLAID_GOCACHE_UPLOAD_CONCURRENCY",
		"PLAID_GOCACHE_UPLOAD_QUEUE_DEPTH",
		"PLAID_GOCACHE_UPLOAD_BLOCK_TIMEOUT",
		"PLAID_GOCACHE_IDLE_TIMEOUT",
		"PLAID_GOCACHE_EVICT_INTERVAL",
		"PLAID_GOCACHE_DISABLE_EVICTION",
		"PLAID_GOCACHE_DISABLE_DAEMON",
		"PLAID_GOCACHE_LOG",
		"PLAID_GOCACHE_COMPACT_AFTER",
		"PLAID_GOCACHE_BAZEL_ADDR",
		"PLAID_GOCACHE_BAZEL_GRPC_ADDR",
		"PLAID_GOCACHE_DISABLE_BAZEL_VERIFY",
		"XDG_CACHE_HOME",
	} {
		// Setting to empty is equivalent to unset for every reader here,
		// each of which treats "" as absent, and it keeps t.Setenv's
		// automatic restore.
		t.Setenv(n, "")
	}
	// Point the configuration-file lookup at an empty directory. Clearing
	// XDG_CONFIG_HOME would fall through to os.UserConfigDir and pick up the
	// file on a developer's own machine, which is exactly the leak this
	// clearing exists to prevent.
	t.Setenv("PLAID_GOCACHE_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
}

// TestLoadBazelSettings pins that the Bazel listener is configurable from the
// environment as well as from the serve flag, since a supervised daemon is
// started from a unit file rather than by hand.
func TestLoadBazelSettings(t *testing.T) {
	clearEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_BAZEL_ADDR", "localhost:9095")
	t.Setenv("PLAID_GOCACHE_BAZEL_GRPC_ADDR", "localhost:9096")
	t.Setenv("PLAID_GOCACHE_BAZEL_MONITORING", "1")
	t.Setenv("PLAID_GOCACHE_DISABLE_BAZEL_VERIFY", "1")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.BazelAddr != "localhost:9095" {
		t.Fatalf("BazelAddr = %q, want %q", c.BazelAddr, "localhost:9095")
	}
	if c.BazelGRPCAddr != "localhost:9096" {
		t.Fatalf("BazelGRPCAddr = %q, want %q", c.BazelGRPCAddr, "localhost:9096")
	}
	if !c.BazelMonitoring {
		t.Fatalf("BazelMonitoring = false, want true")
	}
	if !c.DisableBazelVerify {
		t.Fatalf("DisableBazelVerify = false, want true")
	}
}

// TestMonitoringIsOffUnlessAsked pins the conservative default. The monitoring
// routes describe the host rather than the cache's contents, so a daemon that
// was only told to serve Bazel serves them not at all.
func TestMonitoringIsOffUnlessAsked(t *testing.T) {
	clearEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_BAZEL_ADDR", "localhost:9095")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.BazelMonitoring {
		t.Fatalf("BazelMonitoring = true for a daemon that was only asked to serve Bazel")
	}
}

// TestBazelSettingsAreValidFileKeys pins that every Bazel setting can be written to
// the configuration file. An unknown key there is a hard error, so a setting
// missing from the accepted set is one a user cannot persist.
func TestBazelSettingsAreValidFileKeys(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	t.Setenv("PLAID_GOCACHE_DIR", dir)
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("bazel-addr = localhost:9096\nbazel-grpc-addr = localhost:9097\nbazel-monitoring = 1\ndisable-bazel-verify = 1\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(configFileEnvVar, path)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.BazelAddr != "localhost:9096" {
		t.Fatalf("BazelAddr = %q, want %q", c.BazelAddr, "localhost:9096")
	}
	if c.BazelGRPCAddr != "localhost:9097" {
		t.Fatalf("BazelGRPCAddr = %q, want %q", c.BazelGRPCAddr, "localhost:9097")
	}
	if !c.BazelMonitoring {
		t.Fatalf("BazelMonitoring = false, want true")
	}
	if !c.DisableBazelVerify {
		t.Fatalf("DisableBazelVerify = false, want true")
	}
}
