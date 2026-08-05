// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// testVersion is the version both halves of a test agree on. It is deliberately
// not a plausible real build version, so a mismatch test cannot pass by
// accidentally agreeing with the code under test.
const testVersion = "test-1.0.0"

// serveStopTimeout bounds how long a test waits for Serve to return. It is only
// ever reached when the daemon has failed to shut down, so it is generous
// rather than tuned.
const serveStopTimeout = 5 * time.Second

// shortTempDir returns a fresh directory with a short path.
//
// t.TempDir() embeds the test name and can exceed the kernel's ~108-byte
// sockaddr_un.sun_path limit, at which point bind fails with EINVAL.
// config.SocketPath already falls back to a hashed name in TMPDIR when the
// path is too long, but a short directory keeps the tests exercising the
// primary path (the socket beside the cache) rather than the fallback.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pc")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// newTestConfig builds a Config as a struct literal, bypassing config.Load so
// the process environment cannot influence a test.
func newTestConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Dir:               shortTempDir(t),
		MaxBytes:          1 << 30,
		TTL:               time.Hour,
		TouchGranularity:  time.Hour,
		UploadConcurrency: 1,
		DisableEviction:   true,
	}
}

// newTestServer builds a Server over a real index and body store with no remote
// tier.
func newTestServer(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}
	c := cache.New(cache.Params{Config: cfg, Index: ix, Blobs: blobs, Remote: remote.Noop{}})
	t.Cleanup(func() { _ = c.Close() })

	return NewServer(ServerParams{Config: cfg, Cache: c, Index: ix, Blobs: blobs, Version: testVersion})
}

// testServer is a Server running Serve on a real unix socket.
type testServer struct {
	*Server
	cfg *config.Config

	// err carries Serve's return value; done closes once Serve has returned.
	err  chan error
	done chan struct{}
}

// startServer runs a Server on a real socket and stops it at test end.
func startServer(t *testing.T, cfg *config.Config) *testServer {
	t.Helper()
	s := newTestServer(t, cfg)

	// Sweep here rather than leaving it to Serve, which runs in the goroutine
	// below. Serve sweeps the store's abandoned temporaries before it accepts
	// anything, so a client cannot have a write in flight when it happens — but
	// a test that writes through the cache directly is not a client and is not
	// ordered behind it. Doing it now leaves Serve's sync.Once with the work
	// already done, rather than racing a Put the test has started meanwhile and
	// deleting the temporary out from under it.
	s.cleanTemp()

	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ts := &testServer{Server: s, cfg: cfg, err: make(chan error, 1), done: make(chan struct{})}
	go func() {
		defer close(ts.done)
		ts.err <- s.Serve(context.Background(), ln)
	}()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-ts.done:
		case <-time.After(serveStopTimeout):
			t.Errorf("Serve did not return within %v of Stop", serveStopTimeout)
		}
	})
	return ts
}

// waitServe blocks until Serve returns and reports its error.
func (ts *testServer) waitServe(t *testing.T) error {
	t.Helper()
	select {
	case <-ts.done:
		return <-ts.err
	case <-time.After(serveStopTimeout):
		t.Fatalf("Serve did not return within %v", serveStopTimeout)
		return nil
	}
}

// dialServer connects to a running test daemon without spawning one.
func dialServer(t *testing.T, cfg *config.Config) *Conn {
	t.Helper()
	conn, err := Dial(cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// sayHello performs the control exchange and returns the daemon's reply.
func sayHello(t *testing.T, conn *Conn, version string, op Op) HelloResponse {
	t.Helper()
	if err := writeJSONLine(conn, Hello{Version: version, Op: op}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var resp HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		t.Fatalf("read hello response: %v", err)
	}
	return resp
}

// putCached stores one object through the Server's own cache, so a test can set
// up a hit without going through the protocol.
func putCached(t *testing.T, s *Server, a ids.ActionID, o ids.OutputID, body []byte) {
	t.Helper()
	if _, err := s.cache.Put(context.Background(), a, o, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("cache.Put: %v", err)
	}
}

// unixPair returns a connected pair of unix sockets. A real socket pair rather
// than net.Pipe is required wherever half-close matters, because only the
// former implements CloseWrite.
func unixPair(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	path := filepath.Join(shortTempDir(t), "pair.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer ln.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, aerr := ln.Accept()
		ch <- accepted{c, aerr}
	}()

	dialed, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatalf("Accept: %v", a.err)
	}
	t.Cleanup(func() {
		_ = dialed.Close()
		_ = a.conn.Close()
	})
	return newConn(dialed), a.conn
}

// testActionID returns a distinguishable 32-byte action ID.
func testActionID(seed byte) ids.ActionID {
	var a ids.ActionID
	for i := range a {
		a[i] = seed + byte(i)
	}
	return a
}

// testOutputID returns a distinguishable 32-byte output ID.
func testOutputID(seed byte) ids.OutputID {
	var o ids.OutputID
	for i := range o {
		o[i] = seed*2 + byte(i)
	}
	return o
}

// fill returns n deterministic bytes, so a mismatch reports a reproducible
// failure rather than a random one.
func fill(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}
