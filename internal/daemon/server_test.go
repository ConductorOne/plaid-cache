// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/wire"
)

// nextResponse reads one protocol response, failing the test on error.
func nextResponse(t *testing.T, d *wire.ResponseDecoder) *wire.Response {
	t.Helper()
	resp, err := d.Next()
	if err != nil {
		t.Fatalf("response decode: %v", err)
	}
	return resp
}

// wantHandshake asserts the unsolicited capability line every session opens
// with. The go tool only sends verbs it saw advertised here, so an absent or
// wrong line makes the toolchain send nothing at all.
func wantHandshake(t *testing.T, d *wire.ResponseDecoder) {
	t.Helper()
	resp := nextResponse(t, d)
	if resp.ID != 0 {
		t.Fatalf("handshake ID = %d, want 0", resp.ID)
	}
	want := []wire.Cmd{wire.CmdGet, wire.CmdPut, wire.CmdClose}
	if len(resp.KnownCommands) != len(want) {
		t.Fatalf("KnownCommands = %v, want %v", resp.KnownCommands, want)
	}
	for i, w := range want {
		if got := resp.KnownCommands[i]; got != w {
			t.Fatalf("KnownCommands[%d] = %q, want %q", i, got, w)
		}
	}
}

// runSessionOverPipes runs s.RunSession over a pair of in-memory pipes,
// bypassing the socket, and returns the client ends of the protocol.
func runSessionOverPipes(t *testing.T, s *Server) (*wire.RequestEncoder, *wire.ResponseDecoder) {
	t.Helper()
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer respW.Close()
		s.RunSession(context.Background(), reqR, respW)
	}()
	t.Cleanup(func() {
		// Close the response side first: a get goroutine parked on a write
		// nobody is reading would otherwise keep RunSession from returning.
		_ = respR.Close()
		_ = reqW.Close()
		select {
		case <-done:
		case <-time.After(serveStopTimeout):
			t.Errorf("RunSession did not return within %v", serveStopTimeout)
		}
	})
	return wire.NewRequestEncoder(reqW), wire.NewResponseDecoder(respR)
}

// TestListenCreatesOwnerOnlySocket pins that the socket is created mode 0600:
// the cache holds build outputs, so only its owner may read or poison them.
func TestListenCreatesOwnerOnlySocket(t *testing.T) {
	cfg := newTestConfig(t)
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	fi, err := os.Stat(cfg.SocketPath())
	if err != nil {
		t.Fatalf("Stat socket: %v", err)
	}
	if got, want := fi.Mode().Perm(), os.FileMode(0o600); got != want {
		t.Fatalf("socket mode = %o, want %o", got, want)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket mode = %v, want a socket", fi.Mode())
	}
}

// TestListenReplacesStaleSocket pins that a socket file left behind by a daemon
// that died without cleaning up is removed and replaced, rather than failing
// every future client forever.
func TestListenReplacesStaleSocket(t *testing.T) {
	cfg := newTestConfig(t)
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	stale, err := net.Listen("unix", cfg.SocketPath())
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	ul, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener type = %T, want *net.UnixListener", stale)
	}
	// Leave the file behind on close, which is what a killed daemon does.
	ul.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	if _, err := os.Stat(cfg.SocketPath()); err != nil {
		t.Fatalf("stale socket did not survive close: %v", err)
	}

	ln, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen over stale socket: %v", err)
	}
	defer ln.Close()

	// The replacement must actually be usable, not merely created.
	c, err := net.Dial("unix", cfg.SocketPath())
	if err != nil {
		t.Fatalf("Dial replaced socket: %v", err)
	}
	_ = c.Close()
}

// TestListenRefusesLiveSocket pins that a socket with something listening on it
// is never stolen: the second daemon is refused by name.
func TestListenRefusesLiveSocket(t *testing.T) {
	cfg := newTestConfig(t)
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	first, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer first.Close()

	second, err := Listen(cfg)
	if err == nil {
		second.Close()
		t.Fatal("Listen = nil error on a live socket, want a refusal")
	}
	if !strings.Contains(err.Error(), "another daemon is already listening") {
		t.Fatalf("error = %q, want it to report another daemon listening", err)
	}
	// The refusal must not have unlinked the live daemon's socket.
	c, err := net.Dial("unix", cfg.SocketPath())
	if err != nil {
		t.Fatalf("Dial after refused Listen: %v", err)
	}
	_ = c.Close()
}

// TestHandleSessionHandshakeAdvertisesKnownCommands pins the accepted control
// exchange: a matching version plus OpSession yields HelloResponse{OK:true} and
// then the GOCACHEPROG handshake on the same connection.
func TestHandleSessionHandshakeAdvertisesKnownCommands(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	conn := dialServer(t, cfg)
	resp := sayHello(t, conn, testVersion, OpSession)
	if !resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK", resp)
	}
	if resp.Version != testVersion {
		t.Fatalf("Version = %q, want %q", resp.Version, testVersion)
	}
	if resp.Err != "" {
		t.Fatalf("Err = %q, want empty", resp.Err)
	}
	wantHandshake(t, wire.NewResponseDecoder(conn))
}

// TestHandleVersionMismatchRefusesWithoutSession pins that a client built from
// different code is refused, that the refusal names both versions so the client
// can decide to replace the daemon, and that no session follows.
func TestHandleVersionMismatchRefusesWithoutSession(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	const clientVersion = "client-9.9.9"
	conn := dialServer(t, cfg)
	resp := sayHello(t, conn, clientVersion, OpSession)
	if resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK false on version mismatch", resp)
	}
	if resp.Version != testVersion {
		t.Fatalf("Version = %q, want the daemon's %q", resp.Version, testVersion)
	}
	if !strings.Contains(resp.Err, testVersion) || !strings.Contains(resp.Err, clientVersion) {
		t.Fatalf("Err = %q, want it to name both %q and %q", resp.Err, testVersion, clientVersion)
	}
	// No session bytes may follow: the daemon closes instead of handing the
	// connection to the protocol.
	rest, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read after refusal: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("read %q after refusal, want the connection closed", rest)
	}
}

// TestHandleMalformedHelloReportsError pins that a corrupt first line produces
// an error response instead of a panic or a silently zero-valued Hello (whose
// empty Op would otherwise fall through the switch).
func TestHandleMalformedHelloReportsError(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	conn := dialServer(t, cfg)
	if _, err := conn.Write([]byte("{this is not json\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	var resp HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		t.Fatalf("ReadJSONLine: %v", err)
	}
	if resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK false", resp)
	}
	if resp.Err != "malformed hello" {
		t.Fatalf("Err = %q, want %q", resp.Err, "malformed hello")
	}
}

// TestHandleUnknownOpListsValidOps pins that an unrecognized op is refused with
// a message naming the ops that do exist.
func TestHandleUnknownOpListsValidOps(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	conn := dialServer(t, cfg)
	resp := sayHello(t, conn, testVersion, Op("frobnicate"))
	if resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK false", resp)
	}
	if !strings.Contains(resp.Err, "frobnicate") {
		t.Fatalf("Err = %q, want it to quote the unknown op", resp.Err)
	}
	for _, op := range []Op{OpSession, OpStatus, OpGC, OpShutdown} {
		if !strings.Contains(resp.Err, string(op)) {
			t.Fatalf("Err = %q, want it to list %q", resp.Err, op)
		}
	}
}

// TestHandleStatusReportsIndexCountsAndLimits pins that OpStatus reports the
// index's own counters and the configured ceilings, without disturbing the
// cache.
func TestHandleStatusReportsIndexCountsAndLimits(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)
	putCached(t, ts.Server, testActionID(1), testOutputID(1), fill(2048))

	conn := dialServer(t, cfg)
	if resp := sayHello(t, conn, testVersion, OpStatus); !resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK", resp)
	}
	var st StatusResponse
	if err := conn.ReadJSONLine(&st); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if st.Err != "" {
		t.Fatalf("Err = %q, want empty", st.Err)
	}
	if st.Actions != 1 {
		t.Fatalf("Actions = %d, want 1", st.Actions)
	}
	if st.Objects != 1 {
		t.Fatalf("Objects = %d, want 1", st.Objects)
	}
	if st.DiskBytes <= 0 {
		t.Fatalf("DiskBytes = %d, want > 0", st.DiskBytes)
	}
	if st.MaxBytes != cfg.MaxBytes {
		t.Fatalf("MaxBytes = %d, want %d", st.MaxBytes, cfg.MaxBytes)
	}
	if st.TTL != cfg.TTL.String() {
		t.Fatalf("TTL = %q, want %q", st.TTL, cfg.TTL.String())
	}
	if st.PID != os.Getpid() {
		t.Fatalf("PID = %d, want %d", st.PID, os.Getpid())
	}
	if st.Metrics.Put != 1 {
		t.Fatalf("Metrics.Put = %d, want 1", st.Metrics.Put)
	}
}

// TestHandleGCReturnsEvictionOutcome pins that OpGC runs a real eviction pass
// and reports what it did. MaxBytes is 1 so the single stored entry violates the
// size ceiling and must be pruned.
func TestHandleGCReturnsEvictionOutcome(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.MaxBytes = 1
	ts := startServer(t, cfg)
	putCached(t, ts.Server, testActionID(2), testOutputID(2), fill(4096))

	conn := dialServer(t, cfg)
	if resp := sayHello(t, conn, testVersion, OpGC); !resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK", resp)
	}
	var gc GCResponse
	if err := conn.ReadJSONLine(&gc); err != nil {
		t.Fatalf("read gc: %v", err)
	}
	if gc.Err != "" {
		t.Fatalf("Err = %q, want empty", gc.Err)
	}
	if gc.ActionsPruned != 1 {
		t.Fatalf("ActionsPruned = %d, want 1", gc.ActionsPruned)
	}
	if gc.ObjectsPruned != 1 {
		t.Fatalf("ObjectsPruned = %d, want 1", gc.ObjectsPruned)
	}
	if gc.BytesFreed <= 0 {
		t.Fatalf("BytesFreed = %d, want > 0", gc.BytesFreed)
	}
	if _, err := time.ParseDuration(gc.Elapsed); err != nil {
		t.Fatalf("Elapsed = %q, want a parseable duration: %v", gc.Elapsed, err)
	}
}

// TestHandleShutdownStopsServeAndRemovesSocket pins that OpShutdown ends Serve
// cleanly and unlinks the socket, so the next client dials nothing rather than a
// dead address.
func TestHandleShutdownStopsServeAndRemovesSocket(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	conn := dialServer(t, cfg)
	if resp := sayHello(t, conn, testVersion, OpShutdown); !resp.OK {
		t.Fatalf("HelloResponse = %+v, want OK", resp)
	}
	if err := ts.waitServe(t); err != nil {
		t.Fatalf("Serve = %v, want nil after shutdown", err)
	}
	if _, err := os.Stat(cfg.SocketPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat socket = %v, want not-exist", err)
	}
}

// TestRunSessionPutThenGetRoundTrip pins the whole protocol path over plain
// streams: a put is readable back as a hit whose DiskPath exists and holds the
// bytes that were stored, an unknown action misses, and close ends the session.
func TestRunSessionPutThenGetRoundTrip(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	enc, dec := runSessionOverPipes(t, s)
	wantHandshake(t, dec)

	action, output := testActionID(3), testOutputID(3)
	body := fill(9000)
	put := &wire.Request{
		ID: 1, Command: wire.CmdPut,
		ActionID: action[:], OutputID: output[:], BodySize: int64(len(body)),
	}
	if err := enc.Encode(put, bytes.NewReader(body)); err != nil {
		t.Fatalf("encode put: %v", err)
	}
	putResp := nextResponse(t, dec)
	if putResp.ID != 1 {
		t.Fatalf("put response ID = %d, want 1", putResp.ID)
	}
	if putResp.Err != "" {
		t.Fatalf("put Err = %q, want empty", putResp.Err)
	}
	if putResp.DiskPath == "" {
		t.Fatal("put DiskPath = empty, want the stored body's path")
	}

	if err := enc.Encode(&wire.Request{ID: 2, Command: wire.CmdGet, ActionID: action[:]}, nil); err != nil {
		t.Fatalf("encode get: %v", err)
	}
	hit := nextResponse(t, dec)
	if hit.ID != 2 {
		t.Fatalf("get response ID = %d, want 2", hit.ID)
	}
	if hit.Miss {
		t.Fatal("Miss = true, want a hit for the action just put")
	}
	if hit.Size != int64(len(body)) {
		t.Fatalf("Size = %d, want %d", hit.Size, len(body))
	}
	if !bytes.Equal(hit.OutputID, output[:]) {
		t.Fatalf("OutputID = %x, want %x", hit.OutputID, output[:])
	}
	if hit.Time == nil {
		t.Fatal("Time = nil, want the entry's creation time")
	}
	// The go tool reads the body from disk itself, so a hit without a readable
	// DiskPath fails the build.
	got, err := os.ReadFile(hit.DiskPath)
	if err != nil {
		t.Fatalf("read DiskPath %q: %v", hit.DiskPath, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("DiskPath holds %d bytes, want the %d stored bytes", len(got), len(body))
	}

	unknown := testActionID(200)
	if err := enc.Encode(&wire.Request{ID: 3, Command: wire.CmdGet, ActionID: unknown[:]}, nil); err != nil {
		t.Fatalf("encode unknown get: %v", err)
	}
	miss := nextResponse(t, dec)
	if miss.ID != 3 {
		t.Fatalf("miss response ID = %d, want 3", miss.ID)
	}
	if !miss.Miss {
		t.Fatalf("Miss = false for an action never stored, response = %+v", miss)
	}
	if miss.DiskPath != "" {
		t.Fatalf("DiskPath = %q, want empty on a miss", miss.DiskPath)
	}

	if err := enc.Encode(&wire.Request{ID: 4, Command: wire.CmdClose}, nil); err != nil {
		t.Fatalf("encode close: %v", err)
	}
	closeResp := nextResponse(t, dec)
	if closeResp.ID != 4 {
		t.Fatalf("close response ID = %d, want 4", closeResp.ID)
	}
	if _, err := dec.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after close = %v, want io.EOF", err)
	}
}

// TestRunSessionAnswersPipelinedGetsExactlyOnce pins that a session pipelined
// with more gets than it can hold answers every request ID exactly once, in any
// order, with the right verdict. Gets run on goroutines bounded by
// maxConcurrentGets and share one Encoder, so run this under -race.
func TestRunSessionAnswersPipelinedGetsExactlyOnce(t *testing.T) {
	const hits, misses = 40, 40

	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)

	body := fill(512)
	for i := range hits {
		putCached(t, s, testActionID(byte(i)), testOutputID(byte(i)), body)
	}

	enc, dec := runSessionOverPipes(t, s)
	wantHandshake(t, dec)

	// Responses must be drained concurrently with sending, exactly as the go
	// tool does. maxConcurrentGets bounds in-flight gets, so a client that
	// pipelined past the bound without reading would stall the daemon's read
	// loop behind its own unread responses.
	type read struct {
		resp *wire.Response
		err  error
	}
	reads := make(chan read, hits+misses+1)
	go func() {
		for {
			resp, err := dec.Next()
			reads <- read{resp, err}
			if err != nil {
				return
			}
		}
	}()

	// Request IDs 1..hits are stored actions; the rest were never stored.
	want := make(map[int64]bool, hits+misses)
	for i := range hits + misses {
		id := int64(i + 1)
		want[id] = i < hits
		var a ids.ActionID
		if i < hits {
			a = testActionID(byte(i))
		} else {
			a = testActionID(byte(128 + i - hits))
		}
		if err := enc.Encode(&wire.Request{ID: id, Command: wire.CmdGet, ActionID: a[:]}, nil); err != nil {
			t.Fatalf("encode get %d: %v", id, err)
		}
	}

	// next returns one decoded response, failing rather than hanging if the
	// daemon stops answering.
	next := func() *wire.Response {
		t.Helper()
		select {
		case r := <-reads:
			if r.err != nil {
				t.Fatalf("response decode: %v", r.err)
			}
			return r.resp
		case <-time.After(serveStopTimeout):
			t.Fatalf("no response within %v", serveStopTimeout)
			return nil
		}
	}

	seen := make(map[int64]bool, len(want))
	for range want {
		resp := next()
		wantHit, ok := want[resp.ID]
		if !ok {
			t.Fatalf("response for unknown ID %d", resp.ID)
		}
		if seen[resp.ID] {
			t.Fatalf("ID %d answered twice", resp.ID)
		}
		seen[resp.ID] = true
		if resp.Err != "" {
			t.Fatalf("ID %d: Err = %q, want empty", resp.ID, resp.Err)
		}
		if wantHit && resp.Miss {
			t.Fatalf("ID %d: Miss = true, want a hit", resp.ID)
		}
		if !wantHit && !resp.Miss {
			t.Fatalf("ID %d: Miss = false, want a miss", resp.ID)
		}
		if wantHit && resp.DiskPath == "" {
			t.Fatalf("ID %d: DiskPath = empty on a hit", resp.ID)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("answered %d requests, want %d", len(seen), len(want))
	}

	if err := enc.Encode(&wire.Request{ID: 9999, Command: wire.CmdClose}, nil); err != nil {
		t.Fatalf("encode close: %v", err)
	}
	if resp := next(); resp.ID != 9999 {
		t.Fatalf("close response ID = %d, want 9999", resp.ID)
	}
}

// TestServeMissesDrainsPutBodyAndMissesEveryGet pins the last-resort path, used
// when the cache cannot be opened at all: every get misses, a put's body is
// consumed in full so the request after it still parses, and close ends the
// stream. A build must not fail because its cache is broken.
func TestServeMissesDrainsPutBodyAndMissesEveryGet(t *testing.T) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		defer respW.Close()
		errc <- ServeMisses(reqR, respW)
	}()
	t.Cleanup(func() {
		_ = respR.Close()
		_ = reqW.Close()
	})

	enc := wire.NewRequestEncoder(reqW)
	dec := wire.NewResponseDecoder(respR)
	wantHandshake(t, dec)

	action, output := testActionID(4), testOutputID(4)
	body := fill(20000)
	put := &wire.Request{
		ID: 1, Command: wire.CmdPut,
		ActionID: action[:], OutputID: output[:], BodySize: int64(len(body)),
	}
	if err := enc.Encode(put, bytes.NewReader(body)); err != nil {
		t.Fatalf("encode put: %v", err)
	}
	putResp := nextResponse(t, dec)
	if putResp.ID != 1 {
		t.Fatalf("put response ID = %d, want 1", putResp.ID)
	}
	if putResp.Err != "" {
		t.Fatalf("put Err = %q, want empty", putResp.Err)
	}
	// Nothing was stored, and an absent DiskPath is how that is reported.
	if putResp.DiskPath != "" {
		t.Fatalf("DiskPath = %q, want empty", putResp.DiskPath)
	}

	// This get only parses if the put's body was fully drained.
	if err := enc.Encode(&wire.Request{ID: 2, Command: wire.CmdGet, ActionID: action[:]}, nil); err != nil {
		t.Fatalf("encode get: %v", err)
	}
	get := nextResponse(t, dec)
	if get.ID != 2 {
		t.Fatalf("get response ID = %d, want 2", get.ID)
	}
	if !get.Miss {
		t.Fatalf("Miss = false, want every get to miss: %+v", get)
	}

	if err := enc.Encode(&wire.Request{ID: 3, Command: wire.CmdClose}, nil); err != nil {
		t.Fatalf("encode close: %v", err)
	}
	if resp := nextResponse(t, dec); resp.ID != 3 {
		t.Fatalf("close response ID = %d, want 3", resp.ID)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("ServeMisses = %v, want nil after close", err)
		}
	case <-time.After(serveStopTimeout):
		t.Fatalf("ServeMisses did not return within %v of close", serveStopTimeout)
	}
}

// TestServeExitsWhenIdle pins that a daemon nobody is using exits on its own.
//
// The timeout is tens of milliseconds because the countdown is armed as Serve
// starts and no connection ever suspends it; the serveStopTimeout ceiling is
// only reached when the exit never happens.
func TestServeExitsWhenIdle(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.IdleTimeout = 50 * time.Millisecond
	ts := startServer(t, cfg)

	if err := ts.waitServe(t); err != nil {
		t.Fatalf("Serve = %v, want nil after the idle timeout", err)
	}
	select {
	case <-ts.Stopped():
	default:
		t.Fatal("Stopped() is open, want it closed by the idle exit")
	}
}
