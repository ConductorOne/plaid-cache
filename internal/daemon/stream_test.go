// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/conductorone/plaid-cache/internal/wire"
)

// The two payload sizes the transfer below is measured at, and how much the
// larger may allocate over the smaller. See the same constants in
// internal/bazel for the reasoning; they are repeated rather than shared
// because a test helper exported from one package to another would be the only
// thing either package exported for a test.
const (
	smallPayload = 1 << 20
	largePayload = 32 << 20
	allocSlack   = (largePayload - smallPayload) / 16
)

// zeroReader yields n zero bytes without ever holding them.
type zeroReader struct{ n int64 }

// Read implements io.Reader.
func (z *zeroReader) Read(p []byte) (int, error) {
	if z.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > z.n {
		p = p[:z.n]
	}
	clear(p)
	z.n -= int64(len(p))
	return len(p), nil
}

// putStream renders a GOCACHEPROG put of size zero bytes, followed by a close,
// as a stream.
//
// The request carries its body as a base64 line, and the point of the exercise
// is that the daemon never holds it, so the fixture must not hold it either: it
// is encoded on the fly by a goroutine feeding a pipe.
type putStream struct {
	head []byte
	b64  io.Reader
	tail []byte
}

// newPutStream builds the request stream for a put of size bytes.
func newPutStream(t *testing.T, size int64) *putStream {
	t.Helper()
	a := sha256.Sum256([]byte("action"))
	o := sha256.Sum256([]byte("output"))
	req, err := json.Marshal(wire.Request{
		ID: 1, Command: wire.CmdPut, ActionID: a[:], OutputID: o[:], BodySize: size,
	})
	if err != nil {
		t.Fatalf("marshal put: %v", err)
	}
	closeReq, err := json.Marshal(wire.Request{ID: 2, Command: wire.CmdClose})
	if err != nil {
		t.Fatalf("marshal close: %v", err)
	}

	pr, pw := io.Pipe()
	go func() {
		enc := base64.NewEncoder(base64.StdEncoding, pw)
		_, cerr := io.Copy(enc, &zeroReader{n: size})
		if cerr == nil {
			cerr = enc.Close()
		}
		_ = pw.CloseWithError(cerr)
	}()

	return &putStream{
		head: append(append(req, '\n'), '"'),
		b64:  pr,
		tail: append(append([]byte("\"\n"), closeReq...), '\n'),
	}
}

// Read implements io.Reader over the three segments in order.
func (p *putStream) Read(b []byte) (int, error) {
	if len(p.head) > 0 {
		n := copy(b, p.head)
		p.head = p.head[n:]
		return n, nil
	}
	if p.b64 != nil {
		n, err := p.b64.Read(b)
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, io.EOF) {
			return n, err
		}
		p.b64 = nil
		if n > 0 {
			return n, nil
		}
	}
	if len(p.tail) > 0 {
		n := copy(b, p.tail)
		p.tail = p.tail[n:]
		return n, nil
	}
	return 0, io.EOF
}

// TestSessionPutDoesNotHoldTheBody pins that a GOCACHEPROG put costs the same
// memory whatever the output weighs.
//
// The body arrives base64-encoded on a single line, which is the shape most
// likely to tempt a decoder into reading the line before decoding it. Doing so
// would put the whole output, and its encoding, in the daemon's heap — once per
// concurrent build.
func TestSessionPutDoesNotHoldTheBody(t *testing.T) {
	transfer := func(size int64) {
		cfg := newTestConfig(t)
		s := newTestServer(t, cfg)
		s.RunSession(context.Background(), newPutStream(t, size), io.Discard)
	}

	small := allocsDuring(func() { transfer(smallPayload) })
	large := allocsDuring(func() { transfer(largePayload) })
	t.Logf("session put allocated %d bytes for %d MiB and %d bytes for %d MiB",
		small, smallPayload>>20, large, largePayload>>20)
	if large > small+allocSlack {
		t.Fatalf("session put allocated %d bytes for a %d MiB output against %d for %d MiB: "+
			"growth of %d exceeds the %d allowed, so the body is being held rather than streamed",
			large, largePayload>>20, small, smallPayload>>20, large-small, allocSlack)
	}
}

// allocsDuring reports the bytes allocated while fn ran.
func allocsDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestServeSweepsAbandonedTemporaries pins that a daemon reclaims what its
// predecessor was killed in the middle of writing.
//
// A partial body is referenced by nothing and counted by nothing, so no
// eviction pass will ever find it, and it is as large as whatever transfer was
// interrupted. Sweeping on the way up is the only moment it is safe to do at
// all: once this daemon is serving, a temporary belonging to a live write looks
// exactly the same.
func TestServeSweepsAbandonedTemporaries(t *testing.T) {
	cfg := newTestConfig(t)

	// Lay down what a killed process leaves: one in the staging area, one
	// beside the published bodies where Put's own temporary goes.
	staging := filepath.Join(cfg.BlobDir(), "staging")
	shard := filepath.Join(cfg.BlobDir(), "output", "ab")
	for _, dir := range []string{staging, shard} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
	}
	abandoned := []string{
		filepath.Join(staging, "body.tmp.deadbeef"),
		filepath.Join(shard, "ab"+strings.Repeat("0", 62)+".tmp.feedface"),
	}
	// A published body, to pin that the sweep is not simply deleting the tree.
	published := filepath.Join(shard, "ab"+strings.Repeat("1", 62))
	for _, p := range append(append([]string{}, abandoned...), published) {
		if err := os.WriteFile(p, []byte("partial"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	ts := startServer(t, cfg)
	// A completed handshake proves the accept loop is running, and the sweep
	// runs before it, so there is nothing to poll for. Dialling alone would not:
	// the socket is bound before Serve is entered, so a connect succeeds out of
	// the listen backlog while the sweep is still ahead of it.
	conn, err := Dial(ts.cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	resp, err := handshake(conn, testVersion, OpStatus)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if !resp.OK {
		t.Fatalf("handshake refused: %s", resp.Err)
	}

	for _, p := range abandoned {
		if _, serr := os.Stat(p); !errors.Is(serr, fs.ErrNotExist) {
			t.Errorf("temporary %s survived the sweep: %v", p, serr)
		}
	}
	if _, serr := os.Stat(published); serr != nil {
		t.Errorf("the sweep removed a published body: %v", serr)
	}
}
