// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/wire"
)

// TestConnReadJSONLineKeepsBufferedSessionBytes pins the invariant that a hang
// was traced to: reading the control line must not consume the bytes that
// followed it.
//
// The fake server writes the HelloResponse line AND the first session bytes in
// a single Write, which is what a warm daemon does — it answers the hello and
// emits the GOCACHEPROG handshake back to back, and the two arrive in one read
// on the client. Because Conn owns its bufio.Reader and Conn.Read goes through
// it, the session bytes are still there for the next reader.
//
// What breaks this test: decoding the control line with a throwaway
// bufio.NewReader(conn) — the original bug — or letting Conn.Read bypass c.br
// and read from the embedded net.Conn. Either way the buffered session bytes
// are discarded, the read below returns EOF instead of the handshake, and the
// `go` tool waits forever for a handshake that was already sent. It only
// reproduces against a warm daemon, where both writes land in one read, which
// is why a cold-start test never caught it.
func TestConnReadJSONLineKeepsBufferedSessionBytes(t *testing.T) {
	server, client := net.Pipe()
	conn := newConn(client)
	t.Cleanup(func() { _ = conn.Close() })

	// One write, two protocol messages: the control reply and the session bytes
	// the caller must still be able to read.
	hello := `{"version":"` + testVersion + `","ok":true}` + "\n"
	session := "{\"KnownCommands\":[\"get\",\"put\",\"close\"]}\n"
	werr := make(chan error, 1)
	go func() {
		_, err := server.Write([]byte(hello + session))
		// net.Pipe delivers a Write to a single Read, so by the time Write
		// returns the client holds every byte and closing cannot lose any.
		_ = server.Close()
		werr <- err
	}()

	var resp HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		t.Fatalf("ReadJSONLine: %v", err)
	}
	if err := <-werr; err != nil {
		t.Fatalf("server write: %v", err)
	}
	if !resp.OK || resp.Version != testVersion {
		t.Fatalf("HelloResponse = %+v, want {Version:%q OK:true}", resp, testVersion)
	}

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read after ReadJSONLine: %v", err)
	}
	if string(got) != session {
		t.Fatalf("bytes after the control line = %q, want %q", got, session)
	}
}

// TestConnectDeliversSessionHandshake pins the same invariant end to end
// against a live daemon: the connection Connect returns still carries the
// GOCACHEPROG handshake the daemon wrote immediately after the control reply.
func TestConnectDeliversSessionHandshake(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	conn, err := Connect(context.Background(), cfg, testVersion, OpSession, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	wantHandshake(t, wire.NewResponseDecoder(conn))
}

// TestConnectRefusedByMatchingVersionDoesNotReplaceDaemon pins that a refusal
// from a daemon running the client's own version is reported as an error rather
// than treated as a version conflict: replacing a daemon that agrees with us
// would shut down a healthy index owner.
func TestConnectRefusedByMatchingVersionDoesNotReplaceDaemon(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	_, err := Connect(context.Background(), cfg, testVersion, Op("frobnicate"), nil)
	if err == nil {
		t.Fatal("Connect = nil error for an unknown op, want a refusal")
	}
	if !strings.Contains(err.Error(), "daemon refused") {
		t.Fatalf("error = %q, want it to report a refusal", err)
	}
	select {
	case <-ts.done:
		t.Fatal("Serve returned, want the daemon left running after a refusal")
	default:
	}
	if _, err := os.Stat(cfg.SocketPath()); err != nil {
		t.Fatalf("Stat socket after refusal: %v", err)
	}
}

// TestDialNonexistentSocketDoesNotSpawn pins that Dial reports an error instead
// of bringing a daemon into existence: asking whether a daemon is running must
// not start one.
func TestDialNonexistentSocketDoesNotSpawn(t *testing.T) {
	cfg := newTestConfig(t)

	conn, err := Dial(cfg)
	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial = nil error with no daemon running, want an error")
	}
	if !strings.Contains(err.Error(), "Dial:") {
		t.Fatalf("error = %q, want a Dial prefix", err)
	}
	// A spawn would have created the daemon log and, shortly after, the socket.
	if _, err := os.Stat(cfg.LogPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat daemon log = %v, want not-exist: Dial must not spawn", err)
	}
	if _, err := os.Stat(cfg.SocketPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat socket = %v, want not-exist", err)
	}
}

// TestWaitSocketGoneReturnsWhenSocketDisappears pins that a caller waiting on a
// daemon it asked to shut down is released as soon as the socket is unlinked.
func TestWaitSocketGoneReturnsWhenSocketDisappears(t *testing.T) {
	t.Run("already gone", func(t *testing.T) {
		path := filepath.Join(shortTempDir(t), "absent.sock")
		if err := WaitSocketGone(context.Background(), path); err != nil {
			t.Fatalf("WaitSocketGone = %v, want nil for an absent path", err)
		}
	})

	t.Run("removed while waiting", func(t *testing.T) {
		cfg := newTestConfig(t)
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		ln, err := Listen(cfg)
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
		// Closing the listener unlinks the socket, exactly as a shutting-down
		// daemon does.
		go func() { _ = ln.Close() }()

		done := make(chan error, 1)
		go func() { done <- WaitSocketGone(context.Background(), cfg.SocketPath()) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("WaitSocketGone = %v, want nil", err)
			}
		case <-time.After(serveStopTimeout):
			t.Fatalf("WaitSocketGone did not return within %v of the socket being removed", serveStopTimeout)
		}
	})
}

// TestWaitSocketGoneStopsOnContextCancellation pins that the wait is bounded by
// the caller's context and does not sit on a socket that never goes away.
//
// The 10-second internal deadline is deliberately not exercised: reaching it
// would add ten seconds of wall clock to the suite for one error string.
func TestWaitSocketGoneStopsOnContextCancellation(t *testing.T) {
	cfg := newTestConfig(t)
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitSocketGone(ctx, cfg.SocketPath()); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitSocketGone = %v, want context.Canceled", err)
	}
}

// TestRelayCopiesBothDirectionsAndHalfCloses pins the plugin process's whole
// job: toolchain bytes reach the daemon, daemon bytes reach the toolchain, and
// end of input on stdin half-closes the connection so the daemon's decoder sees
// EOF instead of blocking forever.
func TestRelayCopiesBothDirectionsAndHalfCloses(t *testing.T) {
	conn, peer := unixPair(t)

	const upstream = "{\"ID\":1,\"Command\":\"close\"}\n"
	const downstream = "{\"KnownCommands\":[\"get\",\"put\",\"close\"]}\n"

	type peerResult struct {
		got []byte
		err error
	}
	peerDone := make(chan peerResult, 1)
	go func() {
		// ReadAll returns only once the relay has half-closed its write side.
		got, err := io.ReadAll(peer)
		if err != nil {
			peerDone <- peerResult{err: err}
			return
		}
		if _, err := peer.Write([]byte(downstream)); err != nil {
			peerDone <- peerResult{err: err}
			return
		}
		_ = peer.Close()
		peerDone <- peerResult{got: got}
	}()

	var stdout strings.Builder
	if err := relay(conn, strings.NewReader(upstream), &stdout); err != nil {
		t.Fatalf("relay: %v", err)
	}
	res := <-peerDone
	if res.err != nil {
		t.Fatalf("peer: %v", res.err)
	}
	if string(res.got) != upstream {
		t.Fatalf("daemon received %q, want %q", res.got, upstream)
	}
	if stdout.String() != downstream {
		t.Fatalf("stdout = %q, want %q", stdout.String(), downstream)
	}
}
