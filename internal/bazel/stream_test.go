// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The two payload sizes every transfer here is measured at.
//
// What matters is the ratio, not the absolute figures: a path that holds the
// body allocates the difference between them, and a path that streams allocates
// the same handful of kilobytes for both. The larger is kept to tens of
// megabytes so the suite still runs under -race in a moment.
const (
	smallPayload = 1 << 20
	largePayload = 32 << 20
)

// allocSlack is how much the larger transfer may allocate over the smaller.
//
// It is a sixteenth of the extra payload, so a path that buffers the body fails
// by an order of magnitude rather than by a hair, while ordinary per-request
// noise — a copy buffer, a header map, a few interface boxes — passes with room
// to spare.
const allocSlack = (largePayload - smallPayload) / 16

// zeroReader yields n zero bytes without ever holding them, so a test measuring
// what the code under test allocates is not measuring its own fixture.
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

// zeroDigest is the digest of n zero bytes, computed without materialising them.
func zeroDigest(t *testing.T, n int64) Digest {
	t.Helper()
	h := sha256.New()
	if _, err := io.Copy(h, &zeroReader{n: n}); err != nil {
		t.Fatalf("hash: %v", err)
	}
	var d Digest
	copy(d[:], h.Sum(nil))
	return d
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

// assertFlatAllocs runs transfer at both payload sizes and fails unless what it
// allocated stayed flat.
func assertFlatAllocs(t *testing.T, what string, transfer func(t *testing.T, size int64)) {
	t.Helper()
	small := allocsDuring(func() { transfer(t, smallPayload) })
	large := allocsDuring(func() { transfer(t, largePayload) })
	t.Logf("%s allocated %d bytes for %d MiB and %d bytes for %d MiB",
		what, small, smallPayload>>20, large, largePayload>>20)
	if large > small+allocSlack {
		t.Fatalf("%s allocated %d bytes for a %d MiB payload against %d for %d MiB: "+
			"growth of %d exceeds the %d allowed, so the payload is being held rather than streamed",
			what, large, largePayload>>20, small, smallPayload>>20, large-small, allocSlack)
	}
}

// TestStorePutDoesNotHoldTheBody pins that storing a CAS blob costs the same
// memory whatever the blob weighs.
//
// This is the property the whole upload path rests on. A body arrives from a
// client that chose its size, and as many of them arrive at once as the client
// chose to send, so a server that holds one in memory has a footprint of blob
// size times concurrency and no say in either factor.
func TestStorePutDoesNotHoldTheBody(t *testing.T) {
	assertFlatAllocs(t, "Store.Put", func(t *testing.T, size int64) {
		st, _, _ := newStore(t)
		d := zeroDigest(t, size)
		if err := st.Put(context.Background(), KindCAS, d, &zeroReader{n: size}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	})
}

// TestHTTPUploadDoesNotHoldTheBody pins the same property one layer out, over
// the transport a Bazel client actually speaks.
func TestHTTPUploadDoesNotHoldTheBody(t *testing.T) {
	assertFlatAllocs(t, "PUT /cas", func(t *testing.T, size int64) {
		st, _, _ := newStore(t)
		srv := httptest.NewServer(NewHandler(HandlerParams{Store: st, Logf: t.Logf}))
		defer srv.Close()

		d := zeroDigest(t, size)
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/cas/"+hex.EncodeToString(d[:]), &zeroReader{n: size})
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.ContentLength = size
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
		}
	})
}

// TestHTTPDownloadDoesNotHoldTheBody pins that serving a hit streams off disk
// rather than reading the body in to write it out.
func TestHTTPDownloadDoesNotHoldTheBody(t *testing.T) {
	assertFlatAllocs(t, "GET /cas", func(t *testing.T, size int64) {
		st, _, _ := newStore(t)
		srv := httptest.NewServer(NewHandler(HandlerParams{Store: st, Logf: t.Logf}))
		defer srv.Close()

		d := zeroDigest(t, size)
		if err := st.Put(context.Background(), KindCAS, d, &zeroReader{n: size}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		resp, err := http.Get(srv.URL + "/cas/" + hex.EncodeToString(d[:]))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		n, cerr := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if cerr != nil {
			t.Fatalf("read body: %v", cerr)
		}
		if n != size {
			t.Fatalf("served %d bytes, want %d", n, size)
		}
	})
}

// TestPutLeavesNoStagingFileWhenTheBodyFails pins that a transfer which dies
// partway takes its temporary with it.
//
// A staged body is referenced by nothing and counted by nothing, so one left
// per failed upload is a disk leak in proportion to the traffic that failed —
// and a failed upload is the ordinary consequence of a build being interrupted.
func TestPutLeavesNoStagingFileWhenTheBodyFails(t *testing.T) {
	st, _, blobs := newStore(t)
	d := zeroDigest(t, 1024)

	body := io.MultiReader(&zeroReader{n: 512}, errReader{errors.New("connection reset by peer")})
	err := st.Put(context.Background(), KindCAS, d, body)
	if err == nil {
		t.Fatal("Put succeeded on a body that failed partway")
	}
	if errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Put reported a digest mismatch for a transport failure: %v", err)
	}
	assertStagingEmpty(t, blobs.Root())

	// Nothing may have been published either: a half-written body under an
	// address promising the whole of it is the one outcome worse than a miss.
	if _, _, ok := st.Open(context.Background(), KindCAS, d); ok {
		t.Fatal("a failed upload published a body")
	}
}

// TestPutLeavesNoStagingFileWhenTheClientDisconnects pins the same recovery for
// a client that goes away mid-upload over a real connection, which is how an
// interrupted build actually presents.
func TestPutLeavesNoStagingFileWhenTheClientDisconnects(t *testing.T) {
	st, _, blobs := newStore(t)

	// The handler returning is the signal: without it the test would race the
	// deferred unstage that runs after the body read fails.
	returned := make(chan struct{})
	h := NewHandler(HandlerParams{Store: st, Logf: t.Logf})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	d := zeroDigest(t, largePayload)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		srv.URL+"/cas/"+hex.EncodeToString(d[:]), &cancelAfter{n: 1 << 16, cancel: cancel})
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.ContentLength = largePayload
	if resp, derr := http.DefaultClient.Do(req); derr == nil {
		_ = resp.Body.Close()
		t.Fatal("PUT succeeded despite the client cancelling mid-body")
	}

	<-returned
	assertStagingEmpty(t, blobs.Root())
}

// cancelAfter yields n zero bytes and then cancels the request carrying it,
// standing in for a client that dies mid-upload.
type cancelAfter struct {
	n      int64
	cancel context.CancelFunc
}

// Read implements io.Reader.
func (c *cancelAfter) Read(p []byte) (int, error) {
	if c.n <= 0 {
		c.cancel()
		return 0, context.Canceled
	}
	if int64(len(p)) > c.n {
		p = p[:c.n]
	}
	clear(p)
	c.n -= int64(len(p))
	return len(p), nil
}

// errReader fails on every read, standing in for a peer that went away.
type errReader struct{ err error }

// Read implements io.Reader.
func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// assertStagingEmpty fails the test if any staged body survives under root.
func assertStagingEmpty(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "staging")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatalf("read staging: %v", err)
	}
	var left []string
	for _, e := range entries {
		left = append(left, filepath.Join(dir, e.Name()))
	}
	if len(left) > 0 {
		t.Fatalf("staged bodies left behind: %v", left)
	}
}

// BenchmarkStorePut reports the per-upload allocation at both payload sizes.
// Run with -benchmem: B/op that tracks the payload is the regression this
// package's tests exist to catch, and the benchmark is where it is legible.
func BenchmarkStorePut(b *testing.B) {
	for _, size := range []int64{smallPayload, largePayload} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			st, _, _ := newStore(b)
			b.ReportAllocs()
			b.SetBytes(size)
			for i := 0; b.Loop(); i++ {
				// A fresh digest per iteration, so the already-stored fast path
				// never stands in for the work being measured.
				d := Digest{}
				h := sha256.Sum256([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
				copy(d[:], h[:])
				if err := st.Put(context.Background(), KindAC, d, &zeroReader{n: size}); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}
