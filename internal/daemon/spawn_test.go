// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// TestMain lets this test binary impersonate the daemon.
//
// spawn execs os.Executable() with "serve", and under `go test` that executable
// is this binary. Rather than leave the spawn path untested, the binary answers
// to "serve" by running a real daemon, which is what makes the tests below able
// to exercise the actual spawn, the lock-arbitrated race, and the version
// replacement handshake instead of a stub.
//
// The switch is on argv[1] rather than an environment variable, because spawn
// gives the child this process's environment: a marker variable set here would
// also be set in the test process itself, which would then never run any tests.
// The go test harness only ever passes -test.* flags, so "serve" is unambiguous.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		os.Exit(testDaemonMain())
	}
	os.Exit(m.Run())
}

// testDaemonVersionEnv lets a test choose the version the spawned daemon
// reports, so a mismatch can be staged deliberately.
const testDaemonVersionEnv = "PLAID_CACHE_TEST_DAEMON_VERSION"

// testDaemonMain mirrors what cmd/plaid-cache's serve subcommand does, reduced
// to what these tests need. It is deliberately a separate implementation: the
// daemon's own entry point lives in package main and cannot be imported here.
func testDaemonMain() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "test daemon: config: %v\n", err)
		return 1
	}
	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		// Losing the race for the index lock is the expected outcome when
		// several clients spawn at once. Exit quietly, as the real daemon does;
		// the winner serves everyone.
		if errors.Is(err, index.ErrLocked) {
			return 0
		}
		fmt.Fprintf(os.Stderr, "test daemon: index: %v\n", err)
		return 1
	}
	defer func() { _ = idx.Close() }()

	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "test daemon: blobs: %v\n", err)
		return 1
	}
	c := cache.New(cache.Params{Config: cfg, Index: idx, Blobs: blobs, Remote: remote.Noop{}})
	defer func() { _ = c.Close() }()

	ln, err := Listen(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "test daemon: listen: %v\n", err)
		return 1
	}
	version := os.Getenv(testDaemonVersionEnv)
	if version == "" {
		version = "test-daemon"
	}
	srv := NewServer(ServerParams{Config: cfg, Cache: c, Index: idx, Version: version})
	if err := srv.Serve(context.Background(), ln); err != nil {
		fmt.Fprintf(os.Stderr, "test daemon: serve: %v\n", err)
		return 1
	}
	return 0
}

// newSpawnConfig prepares an environment in which spawn can run.
//
// The cache directory goes in a short path because the socket lives beside it
// and sun_path is ~108 bytes; t.TempDir() embeds the test name and would push
// SocketDir onto its hashed TMPDIR fallback, testing a different path than
// production uses. A small idle timeout means any daemon a failing test leaks
// exits on its own rather than outliving the run.
func newSpawnConfig(t *testing.T) *config.Config {
	t.Helper()
	dir, err := os.MkdirTemp("", "pcs")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	for k, v := range map[string]string{
		"PLAID_GOCACHE_DIR":               dir,
		"PLAID_GOCACHE_IDLE_TIMEOUT":      "10s",
		"PLAID_GOCACHE_DISABLE_EVICTION":  "1",
		"PLAID_GOCACHE_S3_BUCKET":         "",
		"PLAID_GOCACHE_LOG":               "info",
		"PLAID_GOCACHE_MAX_BYTES":         "",
		"PLAID_GOCACHE_TTL":               "",
		"PLAID_GOCACHE_TOUCH_GRANULARITY": "",
		"PLAID_GOCACHE_EVICT_INTERVAL":    "",
		"PLAID_GOCACHE_DISABLE_DAEMON":    "",
		"XDG_CACHE_HOME":                  "",
	} {
		t.Setenv(k, v)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	t.Cleanup(func() { stopSpawnedDaemon(t, cfg) })
	return cfg
}

// stopSpawnedDaemon shuts down whatever daemon a test started, so detached
// processes do not outlive the run. Setsid puts them in their own session, so
// nothing else reaps them.
func stopSpawnedDaemon(t *testing.T, cfg *config.Config) {
	t.Helper()
	conn, err := Dial(cfg)
	if err != nil {
		return // nothing listening
	}
	defer func() { _ = conn.Close() }()
	if _, err := handshake(conn, daemonVersion(t, cfg), OpShutdown); err != nil {
		t.Logf("cleanup shutdown: %v (daemon will exit on its idle timeout)", err)
		return
	}
	_ = WaitSocketGone(context.Background(), cfg.SocketPath())
}

// daemonVersion asks the running daemon what version it reports, so a shutdown
// is not refused for a mismatch during cleanup.
func daemonVersion(t *testing.T, cfg *config.Config) string {
	t.Helper()
	conn, err := Dial(cfg)
	if err != nil {
		return ""
	}
	defer func() { _ = conn.Close() }()
	// A deliberately wrong version elicits a refusal that names the daemon's.
	resp, err := handshake(conn, "probe-mismatch", OpStatus)
	if err != nil {
		return ""
	}
	return resp.Version
}

// spawnLog returns the daemon log, for failure messages: a spawned daemon's
// diagnostics go to a file because it has no terminal.
func spawnLog(cfg *config.Config) string {
	b, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		return fmt.Sprintf("(no daemon log: %v)", err)
	}
	return string(b)
}

// TestConnectSpawnsADaemonWhenNoneIsRunning pins the auto-spawn itself: nobody
// starts the daemon, the first client that needs it does.
func TestConnectSpawnsADaemonWhenNoneIsRunning(t *testing.T) {
	cfg := newSpawnConfig(t)
	t.Setenv(testDaemonVersionEnv, "v-spawn")

	if _, err := os.Stat(cfg.SocketPath()); err == nil {
		t.Fatal("a socket exists before any spawn")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := Connect(ctx, cfg, "v-spawn", OpSession, nil)
	if err != nil {
		t.Fatalf("Connect: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	defer func() { _ = conn.Close() }()

	// The session must be live, which means the spawned process really is a
	// daemon and not just a socket that exists.
	buf := make([]byte, 256)
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading the protocol handshake: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	if !strings.Contains(string(buf[:n]), "KnownCommands") {
		t.Fatalf("spawned daemon did not send the handshake: %q", buf[:n])
	}
	if _, err := os.Stat(cfg.LogPath()); err != nil {
		t.Fatalf("spawn did not create a log file for the detached daemon: %v", err)
	}
}

// TestConcurrentClientsConvergeOnOneDaemon pins that the spawn race is settled
// by Pebble's exclusive index lock rather than by any coordination.
//
// Every client spawns, because none can dial. Exactly one of those processes
// opens the index; the rest exit quietly and their clients find the winner on a
// later dial attempt. All clients must therefore end up talking to the same
// process, which is what the shared PID checks.
func TestConcurrentClientsConvergeOnOneDaemon(t *testing.T) {
	cfg := newSpawnConfig(t)
	t.Setenv(testDaemonVersionEnv, "v-race")

	const clients = 6
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	pids := make([]int, clients)
	errs := make([]error, clients)
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := Connect(ctx, cfg, "v-race", OpStatus, nil)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = conn.Close() }()
			var st StatusResponse
			if err := conn.ReadJSONLine(&st); err != nil {
				errs[i] = err
				return
			}
			pids[i] = st.PID
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("client %d: %v\ndaemon log:\n%s", i, err, spawnLog(cfg))
		}
	}
	for i, pid := range pids {
		if pid == 0 {
			t.Fatalf("client %d got no PID", i)
		}
		if pid != pids[0] {
			t.Fatalf("clients reached different daemons: pids=%v; the index lock did not arbitrate the race", pids)
		}
	}
	t.Logf("%d concurrent clients all reached daemon pid %d", clients, pids[0])
}

// TestConnectReplacesAMismatchedDaemon pins the version-replacement path: a
// daemon built from different code must be shut down and replaced, not shared,
// because both sides interpret the same on-disk index.
func TestConnectReplacesAMismatchedDaemon(t *testing.T) {
	cfg := newSpawnConfig(t)

	// Bring up a daemon reporting the old version.
	t.Setenv(testDaemonVersionEnv, "v-old")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first, err := Connect(ctx, cfg, "v-old", OpStatus, nil)
	if err != nil {
		t.Fatalf("initial Connect: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	var oldStatus StatusResponse
	if err := first.ReadJSONLine(&oldStatus); err != nil {
		t.Fatalf("status from the old daemon: %v", err)
	}
	_ = first.Close()
	if oldStatus.PID == 0 {
		t.Fatal("no PID from the old daemon")
	}

	// Now a newer client arrives. Any daemon it spawns will report the new
	// version, so the replacement can actually succeed.
	t.Setenv(testDaemonVersionEnv, "v-new")
	second, err := Connect(ctx, cfg, "v-new", OpStatus, nil)
	if err != nil {
		t.Fatalf("Connect after a version bump: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	defer func() { _ = second.Close() }()
	var newStatus StatusResponse
	if err := second.ReadJSONLine(&newStatus); err != nil {
		t.Fatalf("status from the new daemon: %v", err)
	}
	if newStatus.PID == 0 {
		t.Fatal("no PID from the new daemon")
	}
	if newStatus.PID == oldStatus.PID {
		t.Fatalf("the mismatched daemon (pid %d) was reused rather than replaced", oldStatus.PID)
	}
	t.Logf("daemon pid %d replaced by pid %d on a version change", oldStatus.PID, newStatus.PID)
}

// TestConnectGivesUpWhenReplacementNeverMatches pins that the replacement loop
// is bounded, so two client versions cannot evict each other forever.
//
// The spawned daemon always reports v-stuck here, so a client asking for
// something else can never be satisfied; it must give up rather than spin.
func TestConnectGivesUpWhenReplacementNeverMatches(t *testing.T) {
	cfg := newSpawnConfig(t)
	t.Setenv(testDaemonVersionEnv, "v-stuck")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Seed a daemon at the version the spawn will always produce.
	seed, err := Connect(ctx, cfg, "v-stuck", OpStatus, nil)
	if err != nil {
		t.Fatalf("seeding Connect: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	_ = seed.Close()

	start := time.Now()
	conn, err := Connect(ctx, cfg, "v-never", OpSession, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("Connect succeeded against a permanently mismatched daemon")
	}
	if ctx.Err() != nil {
		t.Fatalf("Connect ran until the test context expired (%v); the replacement loop is unbounded", time.Since(start))
	}
	if !errors.Is(err, ErrNoDaemon) && !strings.Contains(err.Error(), "mismatched daemon") {
		t.Fatalf("Connect error = %v, want it to report giving up on replacement", err)
	}
	t.Logf("gave up after %v: %v", time.Since(start).Round(time.Millisecond), err)
}

// TestSpawnCreatesTheCacheDirectory pins that spawn works before anything else
// has created the cache root, which is the state of a first-ever build.
func TestSpawnCreatesTheCacheDirectory(t *testing.T) {
	cfg := newSpawnConfig(t)
	t.Setenv(testDaemonVersionEnv, "v-mkdir")

	// Remove the root entirely; only spawn should recreate it.
	if err := os.RemoveAll(cfg.Dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	if err := spawn(cfg, func(string, ...any) {}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(cfg.SocketPath()); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(cfg.SocketPath()); err != nil {
		t.Fatalf("no socket after spawn: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	for _, p := range []string{cfg.Dir, cfg.IndexDir(), cfg.BlobDir()} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s missing after spawn: %v", filepath.Base(p), err)
		}
	}
}

// lockHeldFor is how long TestConnectRetriesWhenTheFirstSpawnLosesTheIndexLock
// keeps the index, chosen to outlast the first retry and not the ladder.
const lockHeldFor = 1200 * time.Millisecond

// TestConnectRetriesWhenTheFirstSpawnLosesTheIndexLock pins that a client whose
// daemon exits on a lock it could not take starts another one.
//
// This is the shape of every daemon handover rather than a corner case. A daemon
// shutting down closes its listener first, which unlinks the socket, and releases
// the index lock last, so a client that sees the socket vanish and immediately
// spawns lands in a window where the new daemon cannot open the index and exits
// quietly — as the real one does, since losing that race is normally how a spawn
// storm resolves. Nothing else was going to start a daemon, so spawning once and
// then only dialing spends the whole backoff ladder on a socket that will never
// appear and hands the build to direct mode.
//
// Holding the index open here reproduces that window deterministically instead of
// waiting for it to be lost under load, which is how it first showed up: the
// version-replacement test failed only on a loaded two-core runner.
func TestConnectRetriesWhenTheFirstSpawnLosesTheIndexLock(t *testing.T) {
	cfg := newSpawnConfig(t)
	t.Setenv(testDaemonVersionEnv, "v-test")

	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("holding the index: %v", err)
	}
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(lockHeldFor)
		_ = ix.Close()
	}()
	t.Cleanup(func() { <-released })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := Connect(ctx, cfg, "v-test", OpStatus, nil)
	if err != nil {
		t.Fatalf("Connect while the index lock was briefly held: %v\ndaemon log:\n%s", err, spawnLog(cfg))
	}
	defer func() { _ = conn.Close() }()

	var st StatusResponse
	if err := conn.ReadJSONLine(&st); err != nil {
		t.Fatalf("status from the daemon that eventually started: %v", err)
	}
	if st.PID == 0 {
		t.Fatal("no PID: no daemon took the index after it was released")
	}
}
