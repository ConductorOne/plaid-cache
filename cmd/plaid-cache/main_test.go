// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// newApp returns an app wired to buffers, with the cache pointed at a temp dir
// so a test never touches a real cache.
func newApp(t *testing.T, args ...string) (*app, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	clearCacheEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	var out, errb bytes.Buffer
	return &app{args: args, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}, &out, &errb
}

// clearCacheEnv removes every variable the tool reads, so the ambient
// environment of a developer machine or CI runner cannot leak into a result.
func clearCacheEnv(t *testing.T) {
	t.Helper()
	for _, n := range []string{
		"PLAID_GOCACHE_DIR", "PLAID_GOCACHE_MAX_BYTES", "PLAID_GOCACHE_TTL",
		"PLAID_GOCACHE_TOUCH_GRANULARITY", "PLAID_GOCACHE_S3_BUCKET",
		"PLAID_GOCACHE_S3_REGION", "PLAID_GOCACHE_S3_PREFIX",
		"PLAID_GOCACHE_S3_ENDPOINT_URL", "PLAID_GOCACHE_MIN_UPLOAD_SIZE",
		"PLAID_GOCACHE_UPLOAD_CONCURRENCY", "PLAID_GOCACHE_IDLE_TIMEOUT",
		"PLAID_GOCACHE_EVICT_INTERVAL", "PLAID_GOCACHE_DISABLE_EVICTION",
		"PLAID_GOCACHE_DISABLE_DAEMON", "PLAID_GOCACHE_LOG", "PLAID_GOCACHE_COMPACT_AFTER", "XDG_CACHE_HOME",
	} {
		t.Setenv(n, "")
	}
	// Point the configuration-file lookup at an empty directory. Clearing
	// XDG_CONFIG_HOME would fall through to os.UserConfigDir and pick up the
	// file on a developer's own machine, which is exactly the leak this
	// clearing exists to prevent.
	t.Setenv("PLAID_GOCACHE_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
}

// TestVersionFlagWorks pins that `-v` prints build info and exits zero.
//
// The release pipeline pushes a Homebrew formula whose test runs `plaid-cache -v`,
// so this flag failing would break packaging rather than anything visible here.
func TestVersionFlagWorks(t *testing.T) {
	for _, arg := range []string{"-v", "--version", "version"} {
		a, out, errb := newApp(t, arg)
		if code := a.run(); code != exitOK {
			t.Fatalf("run(%q) = %d, want %d (stderr: %s)", arg, code, exitOK, errb)
		}
		if !strings.HasPrefix(out.String(), "plaid-cache ") {
			t.Fatalf("run(%q) printed %q, want a plaid-cache version line", arg, out)
		}
	}
}

// TestVersionStringNeverEmpty pins that a build with no ldflags still reports
// something, since `go install ...@latest` passes none.
func TestVersionStringNeverEmpty(t *testing.T) {
	got := versionString()
	if got == "" || got == "plaid-cache " {
		t.Fatalf("versionString() = %q, want a non-empty version", got)
	}
	if !strings.HasPrefix(got, "plaid-cache ") {
		t.Fatalf("versionString() = %q, want it to start with the program name", got)
	}
}

// TestBuildVersionIsStable pins that the daemon-identity string does not change
// between calls. Clients and the daemon compare it to decide whether to replace
// each other, so a value that varied would make them fight.
func TestBuildVersionIsStable(t *testing.T) {
	if a, b := buildVersion(), buildVersion(); a != b {
		t.Fatalf("buildVersion() varies between calls: %q vs %q", a, b)
	}
	if buildVersion() == "" {
		t.Fatal("buildVersion() is empty; a daemon could not be version-checked")
	}
}

// TestHelpExitsZero pins that help is not an error.
func TestHelpExitsZero(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		a, out, _ := newApp(t, arg)
		if code := a.run(); code != exitOK {
			t.Fatalf("run(%q) = %d, want %d", arg, code, exitOK)
		}
		if !strings.Contains(out.String(), "GOCACHEPROG") {
			t.Fatalf("run(%q) help text does not mention GOCACHEPROG:\n%s", arg, out)
		}
	}
}

// TestUnknownSubcommandIsUsageError pins that a typo is a usage error, not a
// silent success, and that it names the offending word.
func TestUnknownSubcommandIsUsageError(t *testing.T) {
	a, _, errb := newApp(t, "stats")
	if code := a.run(); code != exitUsage {
		t.Fatalf("run(\"stats\") = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), "stats") {
		t.Fatalf("stderr does not name the unknown subcommand: %s", errb)
	}
}

// TestBadConfigIsAnError pins that an unparseable setting fails loudly instead
// of falling back to a default. A silently-ignored size ceiling would let the
// cache fill the disk.
func TestBadConfigIsAnError(t *testing.T) {
	a, _, errb := newApp(t, "status")
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "twenty gigs")
	if code := a.run(); code != exitError {
		t.Fatalf("run(status) with a bad size = %d, want %d", code, exitError)
	}
	if !strings.Contains(errb.String(), "PLAID_GOCACHE_MAX_BYTES") {
		t.Fatalf("stderr does not name the offending variable: %s", errb)
	}
}

// TestStatusWithoutDaemonReadsIndexDirectly pins that status works with no
// daemon running, and reports the local-only state rather than erroring.
func TestStatusWithoutDaemonReadsIndexDirectly(t *testing.T) {
	a, out, errb := newApp(t, "status")
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status) = %d, want %d (stderr: %s)", code, exitOK, errb)
	}
	for _, want := range []string{"directory", "actions", "objects", "size", "remote      disabled", "daemon      not running"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
}

// TestStatusDoesNotStartADaemon pins that asking a question does not create the
// thing being asked about: no socket may appear as a side effect of status.
func TestStatusDoesNotStartADaemon(t *testing.T) {
	dir := t.TempDir()
	clearCacheEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", dir)
	var out, errb bytes.Buffer
	a := &app{args: []string{"status"}, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status) = %d (stderr: %s)", code, &errb)
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if _, err := os.Stat(cfg.SocketPath()); err == nil {
		t.Fatalf("status created a daemon socket at %s", cfg.SocketPath())
	}
}

// TestGCWithoutDaemonRuns pins that gc works standalone, taking the index lock
// itself when no daemon holds it.
func TestGCWithoutDaemonRuns(t *testing.T) {
	a, out, errb := newApp(t, "gc")
	if code := a.run(); code != exitOK {
		t.Fatalf("run(gc) = %d, want %d (stderr: %s)", code, exitOK, errb)
	}
	if !strings.Contains(out.String(), "pruned") {
		t.Fatalf("gc output missing a result line:\n%s", out)
	}
}

// TestCleanRemovesTheCacheDirectory pins that clean removes everything the tool
// created, which is why every derived path lives under Dir.
func TestCleanRemovesTheCacheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	clearCacheEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", dir)

	var out, errb bytes.Buffer
	if code := (&app{args: []string{"gc"}, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}).run(); code != exitOK {
		t.Fatalf("seeding run(gc) = %d (stderr: %s)", code, &errb)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("expected the cache dir to exist after gc: %v", err)
	}

	out.Reset()
	errb.Reset()
	if code := (&app{args: []string{"clean"}, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}).run(); code != exitOK {
		t.Fatalf("run(clean) = %d (stderr: %s)", code, &errb)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("clean left %s behind (err=%v)", dir, err)
	}
	if !strings.Contains(out.String(), "removed") {
		t.Fatalf("clean output missing confirmation:\n%s", &out)
	}
}

// TestEvictOnExitPrunesInDirectMode pins the direct-mode prune.
//
// Direct mode has no eviction ticker — that belongs to the daemon — so without
// this pass a cache used with PLAID_GOCACHE_DISABLE_DAEMON=1, or via the
// daemon-unreachable fallback, never prunes at all and grows past both its TTL
// and its byte ceiling.
func TestEvictOnExitPrunesInDirectMode(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir:               dir,
		MaxBytes:          1, // anything at all is over budget
		TTL:               time.Hour,
		TouchGranularity:  time.Hour,
		UploadConcurrency: 1,
	}
	c, idx := newDirectCache(t, cfg)

	body := bytes.Repeat([]byte("x"), 4096)
	putOne(t, c, body)

	before, err := idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if before.Actions == 0 {
		t.Fatal("expected the put to be recorded before eviction")
	}

	(&app{}).evictOnExit(context.Background(), cfg, c, func(string, ...any) {})

	after, err := idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.Actions != 0 {
		t.Fatalf("actions after evict = %d, want 0 (MaxBytes=1 leaves nothing)", after.Actions)
	}
	if after.DiskBytes != 0 {
		t.Fatalf("disk bytes after evict = %d, want 0", after.DiskBytes)
	}
}

// TestEvictOnExitRunsWithACancelledContext pins that a cancelled build still
// prunes.
//
// The toolchain closing stdin, or a signal, cancels the process context — and
// those are exactly the cases where skipping the prune means never pruning. The
// pass therefore detaches from the caller's context.
func TestEvictOnExitRunsWithACancelledContext(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir: dir, MaxBytes: 1, TTL: time.Hour,
		TouchGranularity: time.Hour, UploadConcurrency: 1,
	}
	c, idx := newDirectCache(t, cfg)
	putOne(t, c, bytes.Repeat([]byte("y"), 4096))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already dead before the prune starts

	(&app{}).evictOnExit(ctx, cfg, c, func(string, ...any) {})

	after, err := idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.Actions != 0 {
		t.Fatalf("actions after evict = %d, want 0; a cancelled context must not skip the prune", after.Actions)
	}
}

// TestEvictOnExitHonorsDisableEviction pins that the escape hatch reaches this
// path too, so the flag means what it says.
func TestEvictOnExitHonorsDisableEviction(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir: dir, MaxBytes: 1, TTL: time.Hour, DisableEviction: true,
		TouchGranularity: time.Hour, UploadConcurrency: 1,
	}
	c, idx := newDirectCache(t, cfg)
	putOne(t, c, bytes.Repeat([]byte("z"), 4096))

	(&app{}).evictOnExit(context.Background(), cfg, c, func(string, ...any) {})

	after, err := idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.Actions == 0 {
		t.Fatal("eviction ran despite DisableEviction")
	}
}

// newDirectCache builds a cache over real local tiers and no remote, mirroring
// what runDirect assembles.
func newDirectCache(t *testing.T, cfg *config.Config) (*cache.Cache, *index.Index) {
	t.Helper()
	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}
	c := cache.New(cache.Params{
		Config: cfg, Index: idx, Blobs: blobs, Remote: remote.Noop{},
		Logf: func(string, ...any) {},
	})
	t.Cleanup(func() { _ = c.Close() })
	return c, idx
}

// putOne stores one body under a derived action id.
func putOne(t *testing.T, c *cache.Cache, body []byte) {
	t.Helper()
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256(append([]byte("action:"), body...)))
	if _, err := c.Put(context.Background(), a, o, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// TestRunDirectPrunesOnExit pins that runDirect actually invokes the closing
// prune, not merely that evictOnExit works when called.
//
// The other eviction tests call evictOnExit directly, so removing the call from
// runDirect would leave them all passing while direct mode silently stopped
// pruning — the exact regression this guards.
func TestRunDirectPrunesOnExit(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir: dir, MaxBytes: 1, TTL: time.Hour,
		TouchGranularity: time.Hour, UploadConcurrency: 1,
	}

	// Seed the cache and fully release the index before runDirect opens it:
	// Pebble's lock is exclusive, so the seed cannot outlive this block.
	seedCache(t, cfg, bytes.Repeat([]byte("q"), 4096))

	seeded := statsAt(t, cfg)
	if seeded.Actions == 0 {
		t.Fatal("expected the seed put to be recorded")
	}

	// A session that immediately closes is enough to reach the exit path. The
	// blank line after the request mirrors what cmd/go emits.
	var out, errb bytes.Buffer
	a := &app{
		args:   nil,
		stdin:  strings.NewReader("{\"ID\":1,\"Command\":\"close\"}\n\n"),
		stdout: &out,
		stderr: &errb,
	}
	if code := a.runDirect(context.Background(), cfg, func(string, ...any) {}); code != exitOK {
		t.Fatalf("runDirect = %d, want %d (stderr: %s)", code, exitOK, &errb)
	}
	if !strings.Contains(out.String(), "KnownCommands") {
		t.Fatalf("runDirect did not emit the protocol handshake:\n%s", &out)
	}

	after := statsAt(t, cfg)
	if after.Actions != 0 {
		t.Fatalf("actions after runDirect = %d, want 0; the exit prune did not run", after.Actions)
	}
}

// seedCache stores one body and closes every handle before returning.
//
// It deliberately does not use newDirectCache: that registers a t.Cleanup, so
// the index lock would stay held until the test ended and any later open in the
// same test would fail with ErrLocked.
func seedCache(t *testing.T, cfg *config.Config, body []byte) {
	t.Helper()
	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		_ = idx.Close()
		t.Fatalf("blob.Open: %v", err)
	}
	c := cache.New(cache.Params{
		Config: cfg, Index: idx, Blobs: blobs, Remote: remote.Noop{},
		Logf: func(string, ...any) {},
	})
	putOne(t, c, body)
	if err := c.Close(); err != nil {
		t.Fatalf("cache.Close: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("index.Close: %v", err)
	}
}

// statsAt opens the index just long enough to read its counters.
func statsAt(t *testing.T, cfg *config.Config) index.Stats {
	t.Helper()
	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	defer func() { _ = idx.Close() }()
	s, err := idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	return s
}

// TestStatusReportsDerivedFigures pins that the summary does the arithmetic for
// the reader rather than printing raw counters.
//
// Dedup ratio is the property output refcounting exists to produce, and the
// share of the budget in use is what says whether eviction is about to bite;
// both are useless if the caller has to compute them from other lines.
func TestStatusReportsDerivedFigures(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir: dir, MaxBytes: 64 << 20, TTL: time.Hour,
		TouchGranularity: time.Hour, UploadConcurrency: 1,
	}
	// Two actions sharing one output, so the dedup ratio is not 1.00x.
	body := bytes.Repeat([]byte("d"), 4096)
	func() {
		idx, err := index.Open(cfg.IndexDir())
		if err != nil {
			t.Fatalf("index.Open: %v", err)
		}
		defer func() { _ = idx.Close() }()
		blobs, err := blob.Open(cfg.BlobDir())
		if err != nil {
			t.Fatalf("blob.Open: %v", err)
		}
		c := cache.New(cache.Params{
			Config: cfg, Index: idx, Blobs: blobs, Remote: remote.Noop{},
			Logf: func(string, ...any) {},
		})
		o := ids.OutputID(sha256.Sum256(body))
		for _, tag := range []string{"one", "two"} {
			a := ids.ActionID(sha256.Sum256([]byte(tag)))
			if _, err := c.Put(context.Background(), a, o, bytes.NewReader(body), int64(len(body))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatalf("cache.Close: %v", err)
		}
	}()

	clearCacheEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", dir)
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "64MiB")
	var out, errb bytes.Buffer
	if code := (&app{args: []string{"status"}, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}).run(); code != exitOK {
		t.Fatalf("run(status) = %d (stderr: %s)", code, &errb)
	}

	got := out.String()
	for _, want := range []string{
		"2 actions, 1 objects", // the counts
		"2.00x dedup",          // derived: two actions per stored body
		"avg",                  // derived: average object size
		"of 64.0 MiB",          // the ceiling
		"free",                 // derived: headroom
		"%",                    // derived: share of budget
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("status output missing %q:\n%s", want, got)
		}
	}
}
