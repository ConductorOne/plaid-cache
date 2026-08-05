// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestListenSocketDirIsOwnerOnly pins that the socket's directory denies other
// users.
//
// bind creates the socket with umask-derived permissions and only a following
// chmod narrows it, so for a window the socket itself may be connectable by
// anyone who can reach it — and whoever reaches it can serve build outputs into
// someone else's compile. The window cannot be closed on the socket, so it is
// closed on the directory instead, which is what this pins.
func TestListenSocketDirIsOwnerOnly(t *testing.T) {
	cfg := newTestConfig(t)
	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	fi, err := os.Stat(cfg.SocketDir())
	if err != nil {
		t.Fatalf("stat socket dir: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("socket dir mode = %#o, want 0700; another local user could reach the socket", perm)
	}
	if !fi.IsDir() {
		t.Fatalf("socket dir %s is not a directory", cfg.SocketDir())
	}
	if got := filepath.Dir(cfg.SocketPath()); got != cfg.SocketDir() {
		t.Fatalf("SocketPath is not inside SocketDir: %s vs %s", got, cfg.SocketDir())
	}
}

// TestListenTightensAPreexistingLooseSocketDir pins that a directory left
// world-readable by an earlier run is narrowed rather than accepted, since the
// guarantee has to hold on the second start too.
func TestListenTightensAPreexistingLooseSocketDir(t *testing.T) {
	cfg := newTestConfig(t)
	if err := os.MkdirAll(cfg.SocketDir(), 0o755); err != nil {
		t.Fatalf("pre-create socket dir: %v", err)
	}
	if err := os.Chmod(cfg.SocketDir(), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	fi, err := os.Stat(cfg.SocketDir())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("socket dir mode = %#o after Listen, want 0700", perm)
	}
}

// blockingWriter accepts a fixed number of writes and then blocks until
// released, standing in for a peer that has stopped reading its responses.
type blockingWriter struct {
	allowed int
	release chan struct{}
	blocked chan struct{}
	once    sync.Once
	mu      sync.Mutex
	n       int
}

func newBlockingWriter(allowed int) *blockingWriter {
	return &blockingWriter{
		allowed: allowed,
		release: make(chan struct{}),
		blocked: make(chan struct{}),
	}
}

// Write lets the first `allowed` calls through, then parks.
func (w *blockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.n++
	n := w.n
	w.mu.Unlock()
	if n > w.allowed {
		w.once.Do(func() { close(w.blocked) })
		<-w.release
	}
	return len(p), nil
}

// TestOneWedgedSessionDoesNotStarveAnother pins that the concurrent-get bound
// is per session, not per daemon.
//
// A peer that stops reading leaves its get handler parked in a write. With a
// bound shared across the daemon, that handler holds a slot the decode loop of
// every other session must acquire, so a single stuck build stalls all of them.
//
// The bound is lowered to 1 so the wedge is deterministic: one parked handler
// holds every slot there is. Pipelining maxConcurrentGets requests instead is
// not reliable, because the decoder buffers many request lines in one read and
// dispatches them progressively, so the wedged session may hold only some slots
// when the second session starts.
func TestOneWedgedSessionDoesNotStarveAnother(t *testing.T) {
	cfg := newTestConfig(t)
	srv := newTestServerWithGetLimit(t, cfg, 1)

	body := []byte("shared body")
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("wedge-action")))
	putCached(t, srv, a, o, body)

	getLine := func(id int) string {
		return fmt.Sprintf("{\"ID\":%d,\"Command\":\"get\",\"ActionID\":%q}\n\n",
			id, base64.StdEncoding.EncodeToString(a[:]))
	}

	// Session A: one get at a peer that reads nothing after the handshake.
	wedged := newBlockingWriter(1) // allow only the protocol handshake
	sessionA := make(chan struct{})
	go func() {
		defer close(sessionA)
		srv.RunSession(context.Background(), strings.NewReader(getLine(1)), wedged)
	}()
	select {
	case <-wedged.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("wedged session never blocked on a write; the test cannot prove isolation")
	}

	// Session B must still be served while A holds its slot.
	done := make(chan string, 1)
	go func() {
		var out bytes.Buffer
		srv.RunSession(context.Background(),
			strings.NewReader(getLine(1)+"{\"ID\":2,\"Command\":\"close\"}\n\n"), &out)
		done <- out.String()
	}()

	select {
	case got := <-done:
		if !strings.Contains(got, "DiskPath") {
			t.Fatalf("second session did not get a hit while the first was wedged:\n%s", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second session was starved by a wedged one; the get bound is shared, not per session")
	}

	close(wedged.release)
	<-sessionA
}

// newTestServerWithGetLimit builds a server whose per-session get bound is
// lowered, so a test can hold every slot with a single parked handler.
func newTestServerWithGetLimit(t *testing.T, cfg *config.Config, limit int) *Server {
	t.Helper()
	s := newTestServer(t, cfg)
	s.maxGets = limit
	return s
}

// TestSilentSessionDoesNotKeepTheDaemonAlive pins that a peer which completes
// the handshake and then says nothing is eventually dropped.
//
// The handshake deadline used to be cleared outright once the hello was read.
// A connection that then stalled stayed counted as active forever, so the
// daemon's idle timer could never fire and the process never exited.
func TestSilentSessionDoesNotKeepTheDaemonAlive(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.IdleTimeout = 50 * time.Millisecond
	srv := newTestServer(t, cfg)
	srv.sessionIdle = 50 * time.Millisecond

	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(context.Background(), ln) }()

	// Complete the handshake, then go silent without ever closing.
	conn, err := net.Dial("unix", cfg.SocketPath())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := writeJSONLine(conn, Hello{Version: srv.version, Op: OpSession}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil after an idle exit", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve never returned: a silent session pinned the daemon awake")
	}
}

// failingListener returns errors for the first `fails` accepts, then blocks
// until closed. It stands in for a listener hitting a transient descriptor
// exhaustion.
type failingListener struct {
	fails  int
	closed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	n      int
}

func newFailingListener(fails int) *failingListener {
	return &failingListener{fails: fails, closed: make(chan struct{})}
}

func (l *failingListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.n++
	n := l.n
	l.mu.Unlock()
	if n <= l.fails {
		return nil, syscall.EMFILE
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *failingListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *failingListener) Addr() net.Addr { return &net.UnixAddr{Name: "test", Net: "unix"} }

// attempts reports how many times Accept was called.
func (l *failingListener) attempts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

// TestServeRetriesTransientAcceptErrors pins that a temporary accept failure
// does not take the daemon down.
//
// Every build on this machine depends on this one process, so treating a transient
// EMFILE as fatal would turn a brief descriptor shortage into a dead cache.
func TestServeRetriesTransientAcceptErrors(t *testing.T) {
	cfg := newTestConfig(t)
	srv := newTestServer(t, cfg)
	ln := newFailingListener(3)

	served := make(chan error, 1)
	go func() { served <- srv.Serve(context.Background(), ln) }()

	// Once it stops erroring, Serve should still be running; ask it to stop.
	deadline := time.Now().Add(10 * time.Second)
	for ln.attempts() <= 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := ln.attempts(); got <= 3 {
		t.Fatalf("Accept called %d times, want >3; Serve gave up on a transient error", got)
	}
	srv.Stop()

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil after Stop", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
}

// TestServeGivesUpOnAPermanentlyFailingListener pins the other side: retries are
// bounded, so a listener that never recovers surfaces as an error instead of
// spinning forever.
func TestServeGivesUpOnAPermanentlyFailingListener(t *testing.T) {
	cfg := newTestConfig(t)
	srv := newTestServer(t, cfg)
	ln := newFailingListener(maxAcceptFailures + 5)

	served := make(chan error, 1)
	go func() { served <- srv.Serve(context.Background(), ln) }()

	select {
	case err := <-served:
		if err == nil {
			t.Fatal("Serve returned nil for a listener that never recovered")
		}
		if !errors.Is(err, syscall.EMFILE) {
			t.Fatalf("Serve error = %v, want it to wrap the accept failure", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve never gave up on a permanently failing listener")
	}
}

// TestGCOverrideReachesTheDaemon pins the whole point of GCParams: a limit sent
// with the request must apply, even though the daemon read its own
// configuration at startup.
//
// Without this, a tighter ceiling asked for on the command line was silently
// ignored by a running daemon, which reads as eviction being broken.
func TestGCOverrideReachesTheDaemon(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.MaxBytes = 1 << 30 // the daemon's own ceiling: nothing should be pruned
	cfg.TTL = time.Hour
	srv := newTestServer(t, cfg)

	body := bytes.Repeat([]byte("o"), 4096)
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("override-action")))
	putCached(t, srv, a, o, body)

	// Without an override the daemon's generous ceiling prunes nothing.
	if res := srv.gc(context.Background(), nil); res.ActionsPruned != 0 {
		t.Fatalf("pruned %d with the daemon's own limits, want 0", res.ActionsPruned)
	}

	// With one, the pass uses the tighter ceiling.
	tight := int64(1)
	res := srv.gc(context.Background(), &GCParams{MaxBytes: &tight})
	if res.Err != "" {
		t.Fatalf("gc reported %q", res.Err)
	}
	if res.ActionsPruned != 1 {
		t.Fatalf("pruned %d with max_bytes=1, want 1", res.ActionsPruned)
	}
	if res.AppliedMaxBytes != 1 {
		t.Fatalf("AppliedMaxBytes = %d, want 1; the response must show what was applied", res.AppliedMaxBytes)
	}

	// The daemon's own policy is unchanged: the override was per pass.
	if cfg.MaxBytes != 1<<30 {
		t.Fatalf("the daemon's configured MaxBytes changed to %d; an override must not become policy", cfg.MaxBytes)
	}
}

// TestGCOverrideRejectsABadTTL pins that a malformed duration is reported rather
// than silently falling back to the daemon's TTL, which would prune differently
// than the caller asked for without saying so.
func TestGCOverrideRejectsABadTTL(t *testing.T) {
	cfg := newTestConfig(t)
	srv := newTestServer(t, cfg)

	bad := "not-a-duration"
	res := srv.gc(context.Background(), &GCParams{TTL: &bad})
	if res.Err == "" {
		t.Fatal("gc accepted a malformed ttl")
	}
	if !strings.Contains(res.Err, "not-a-duration") {
		t.Fatalf("error %q does not name the offending value", res.Err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d entries despite a rejected request", res.ActionsPruned)
	}
}

// TestGCOverrideZeroDisablesAConstraint pins that an explicit zero is honoured
// as "no constraint" rather than treated as unset.
func TestGCOverrideZeroDisablesAConstraint(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.MaxBytes = 1 // the daemon would prune everything
	cfg.TTL = time.Hour
	srv := newTestServer(t, cfg)

	body := bytes.Repeat([]byte("z"), 4096)
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("zero-action")))
	putCached(t, srv, a, o, body)

	none := int64(0)
	res := srv.gc(context.Background(), &GCParams{MaxBytes: &none})
	if res.Err != "" {
		t.Fatalf("gc reported %q", res.Err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d with max_bytes=0, want 0: zero must disable the ceiling", res.ActionsPruned)
	}
	if res.AppliedMaxBytes != 0 {
		t.Fatalf("AppliedMaxBytes = %d, want 0", res.AppliedMaxBytes)
	}
}
