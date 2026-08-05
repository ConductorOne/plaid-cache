// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// The two payload sizes the transfers below are measured at, and how much the
// larger may allocate over the smaller. See the same constants in
// internal/bazel for the reasoning.
const (
	smallPayload = 1 << 20
	largePayload = 32 << 20
	allocSlack   = (largePayload - smallPayload) / 16
)

// allocsDuring reports the bytes allocated while fn ran.
func allocsDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// sparseFile returns the path of a file of exactly size bytes.
func sparseFile(t *testing.T, size int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "body")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// TestPutObjectDoesNotHoldTheBody pins that uploading to the shared tier costs
// the same memory whatever the body weighs.
//
// The upload runs on a pool whose width is configurable and defaults to one
// worker per CPU, so a backend that read the body in to send it would put that
// many bodies in the heap at once. It stays flat only because the body is
// handed over as a seekable file: the SDK needs to rewind it to sign or
// checksum it, and a reader it cannot rewind is one it has to materialise. That
// is why PutObject takes an io.ReadSeeker rather than an io.Reader, and this
// test is what says so.
func TestPutObjectDoesNotHoldTheBody(t *testing.T) {
	transfer := func(size int64) {
		s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
			// Discard rather than record: the recorder in the other tests keeps
			// every body, which is exactly the accounting this test must not do.
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		})
		f, err := os.Open(sparseFile(t, size))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = f.Close() }()
		if err := s.PutObject(context.Background(), ids.OutputID{1}, f, size); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}

	small := allocsDuring(func() { transfer(smallPayload) })
	large := allocsDuring(func() { transfer(largePayload) })
	t.Logf("PutObject allocated %d bytes for %d MiB and %d bytes for %d MiB",
		small, smallPayload>>20, large, largePayload>>20)
	if large > small+allocSlack {
		t.Fatalf("PutObject allocated %d bytes for a %d MiB body against %d for %d MiB: "+
			"growth of %d exceeds the %d allowed, so the body is being held rather than streamed",
			large, largePayload>>20, small, smallPayload>>20, large-small, allocSlack)
	}
}

// TestGetObjectDoesNotHoldTheBody pins that a fault from the shared tier is
// handed to the caller as a stream rather than as bytes.
//
// The caller writes it straight to the body store, so a backend that read it in
// first would hold the whole object for as long as the disk took to accept it.
func TestGetObjectDoesNotHoldTheBody(t *testing.T) {
	transfer := func(size int64) {
		s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", itoa(size))
			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, io.LimitReader(zeroSource{}, size))
		})
		body, got, err := s.GetObject(context.Background(), ids.OutputID{2})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		defer func() { _ = body.Close() }()
		if got != size {
			t.Fatalf("ContentLength = %d, want %d", got, size)
		}
		n, cerr := io.Copy(io.Discard, body)
		if cerr != nil {
			t.Fatalf("read body: %v", cerr)
		}
		if n != size {
			t.Fatalf("read %d bytes, want %d", n, size)
		}
	}

	small := allocsDuring(func() { transfer(smallPayload) })
	large := allocsDuring(func() { transfer(largePayload) })
	t.Logf("GetObject allocated %d bytes for %d MiB and %d bytes for %d MiB",
		small, smallPayload>>20, large, largePayload>>20)
	if large > small+allocSlack {
		t.Fatalf("GetObject allocated %d bytes for a %d MiB body against %d for %d MiB: "+
			"growth of %d exceeds the %d allowed, so the body is being held rather than streamed",
			large, largePayload>>20, small, smallPayload>>20, large-small, allocSlack)
	}
}

// zeroSource is an endless run of zero bytes, for a fake server that must
// produce a large body without holding one.
type zeroSource struct{}

// Read implements io.Reader.
func (zeroSource) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// itoa renders a Content-Length without pulling in a formatter for one call.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
