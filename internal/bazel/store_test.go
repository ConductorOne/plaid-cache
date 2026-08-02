// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// read returns the stored body for a digest, or reports the miss.
func read(t *testing.T, s *Store, k Kind, d Digest) ([]byte, bool) {
	t.Helper()
	f, size, ok := s.Open(t.Context(), k, d)
	if !ok {
		return nil, false
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read stored body: %v", err)
	}
	if int64(len(b)) != size {
		t.Fatalf("Open reported %d bytes, body is %d", size, len(b))
	}
	return b, true
}

// digestOf is the CAS digest for a body.
func digestOf(b []byte) Digest { return Digest(sha256.Sum256(b)) }

// TestPutSkipsACASBodyItAlreadyHas pins the answer to the one thing Bazel's
// HTTP protocol cannot ask: which blobs the server already holds.
//
// findMissingDigests over this transport reports every digest as absent, so
// Bazel re-uploads an output whenever the action producing it re-runs, however
// many times it has uploaded the same bytes before. Writing and hashing a
// several-hundred-megabyte body again is pure waste, so the second upload is
// drained and dropped — and the cache must record exactly one put.
func TestPutSkipsACASBodyItAlreadyHas(t *testing.T) {
	s, c, _ := newStore(t)
	body := []byte("an output that keeps being re-uploaded")
	d := digestOf(body)

	for i := range 3 {
		if err := s.Put(t.Context(), KindCAS, d, bytes.NewReader(body)); err != nil {
			t.Fatalf("Put #%d: %v", i+1, err)
		}
	}
	if got := c.Metrics().Put; got != 1 {
		t.Fatalf("three uploads of one body stored %d times, want 1", got)
	}
	if got, ok := read(t, s, KindCAS, d); !ok || !bytes.Equal(got, body) {
		t.Fatalf("stored body = %q (%v), want %q", got, ok, body)
	}
}

// TestSkippedPutDoesNotReplaceTheStoredBody pins that the skip is safe against
// a client whose second upload disagrees with its first.
//
// Skipping means the body is not hashed, so an upload that would have been
// rejected is accepted instead — but only in the sense that it is thrown away.
// The bytes on disk stay the ones that were verified when they were stored,
// which is what content addressing promises.
func TestSkippedPutDoesNotReplaceTheStoredBody(t *testing.T) {
	s, _, _ := newStore(t)
	body := []byte("the bytes that hash to this digest")
	d := digestOf(body)

	if err := s.Put(t.Context(), KindCAS, d, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(t.Context(), KindCAS, d, bytes.NewReader([]byte("different bytes entirely"))); err != nil {
		t.Fatalf("Put of a duplicate: %v", err)
	}
	got, ok := read(t, s, KindCAS, d)
	if !ok || !bytes.Equal(got, body) {
		t.Fatalf("stored body = %q (%v), want the originally verified %q", got, ok, body)
	}
}

// TestPutRepairsACASEntryWhoseBodyIsGone pins that the skip checks the body
// rather than only the index. An entry pointing at a body somebody deleted must
// not make the upload that would have replaced it a no-op.
func TestPutRepairsACASEntryWhoseBodyIsGone(t *testing.T) {
	s, _, blobs := newStore(t)
	body := []byte("a body that is about to go missing")
	d := digestOf(body)

	if err := s.Put(t.Context(), KindCAS, d, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := os.Remove(blobs.Path(ids.OutputID(d))); err != nil {
		t.Fatalf("removing the body: %v", err)
	}
	if err := s.Put(t.Context(), KindCAS, d, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put after the body went missing: %v", err)
	}
	if got, ok := read(t, s, KindCAS, d); !ok || !bytes.Equal(got, body) {
		t.Fatalf("stored body = %q (%v), want %q", got, ok, body)
	}
}

// TestPutReplacesAnActionCacheEntry pins that the skip is for the CAS only. An
// action-cache entry is named by the action rather than by its body, and a
// re-run action legitimately produces a different ActionResult, so the newer
// one has to win.
func TestPutReplacesAnActionCacheEntry(t *testing.T) {
	s, _, _ := newStore(t)
	d := digestOf([]byte("some action"))

	first := []byte("the first ActionResult")
	second := []byte("a later ActionResult for the same action")
	if err := s.Put(t.Context(), KindAC, d, bytes.NewReader(first)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(t.Context(), KindAC, d, bytes.NewReader(second)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, ok := read(t, s, KindAC, d); !ok || !bytes.Equal(got, second) {
		t.Fatalf("stored body = %q (%v), want the later %q", got, ok, second)
	}
}

// TestPutReportsADigestMismatch pins that the caller can tell a client error
// from a storage one, since only the first is worth telling the client about.
func TestPutReportsADigestMismatch(t *testing.T) {
	s, _, _ := newStore(t)
	d := digestOf([]byte("one thing"))

	err := s.Put(t.Context(), KindCAS, d, bytes.NewReader([]byte("another thing")))
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Put of a mismatched body = %v, want ErrDigestMismatch", err)
	}
	if _, ok := read(t, s, KindCAS, d); ok {
		t.Fatal("a rejected upload was stored anyway")
	}
}
