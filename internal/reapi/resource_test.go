// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"crypto/sha256"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// hexOf is the digest a resource name carries for a body.
func hexOf(s string) string { return bazel.Digest(sha256.Sum256([]byte(s))).String() }

// TestParseReadResource pins the download grammar, including the two optional
// pieces — a compressor and a digest-function segment — that change what the
// handler has to do rather than only what it has to parse.
func TestParseReadResource(t *testing.T) {
	h := hexOf("body")
	for _, tc := range []struct {
		name       string
		in         string
		compressor repb.Compressor_Value
		digestFunc string
		size       int64
	}{
		{"plain", "blobs/" + h + "/12", repb.Compressor_IDENTITY, "", 12},
		{"zstd", "compressed-blobs/zstd/" + h + "/12", repb.Compressor_ZSTD, "", 12},
		{"digest function", "blobs/blake3/" + h + "/12", repb.Compressor_IDENTITY, "blake3", 12},
		{"empty blob", "blobs/" + h + "/0", repb.Compressor_IDENTITY, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseReadResource(tc.in)
			if err != nil {
				t.Fatalf("parseReadResource(%q): %v", tc.in, err)
			}
			if got.compressor != tc.compressor {
				t.Fatalf("compressor = %v, want %v", got.compressor, tc.compressor)
			}
			if got.digestFunc != tc.digestFunc {
				t.Fatalf("digest function = %q, want %q", got.digestFunc, tc.digestFunc)
			}
			if got.size != tc.size {
				t.Fatalf("size = %d, want %d", got.size, tc.size)
			}
			if got.digest.String() != h {
				t.Fatalf("digest = %s, want %s", got.digest, h)
			}
		})
	}
}

// TestParseWriteResource pins the upload grammar, whose trailing metadata is
// the client's to choose and the server's to ignore — but which stays part of
// the name, because that is what a resumed write is matched by.
func TestParseWriteResource(t *testing.T) {
	h := hexOf("body")
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"plain", "uploads/some-uuid/blobs/" + h + "/12"},
		{"with metadata", "uploads/some-uuid/blobs/" + h + "/12/path/to/file.o"},
		{"zstd", "uploads/some-uuid/compressed-blobs/zstd/" + h + "/12"},
		{"zstd with metadata", "uploads/some-uuid/compressed-blobs/zstd/" + h + "/12/out.o"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWriteResource(tc.in)
			if err != nil {
				t.Fatalf("parseWriteResource(%q): %v", tc.in, err)
			}
			if got.digest.String() != h || got.size != 12 {
				t.Fatalf("parsed %s/%d, want %s/12", got.digest, got.size, h)
			}
			if got.name != tc.in {
				t.Fatalf("name = %q, want the original %q", got.name, tc.in)
			}
		})
	}
}

// TestMalformedResourcesAreRejected pins that a name this server cannot make
// sense of is refused rather than guessed at. A guess here is a body stored
// under a key nobody will ask for, or worse, one somebody else will.
func TestMalformedResourcesAreRejected(t *testing.T) {
	h := hexOf("body")
	read := []string{
		"",
		"blobs",
		"blobs/" + h,
		"blobs/" + h + "/notanumber",
		"blobs/" + h + "/12/trailing",
		"blobs/deadbeef/12",
		"instance/blobs/" + h + "/12",
		"compressed-blobs/gzip/" + h + "/12",
		"compressed-blobs/" + h + "/12",
	}
	for _, in := range read {
		if got, err := parseReadResource(in); err == nil {
			t.Fatalf("parseReadResource(%q) = %+v, want an error", in, got)
		}
	}

	write := []string{
		"uploads",
		"uploads/uuid",
		"uploads//blobs/" + h + "/12",
		"uploads/uuid/blobs/" + h,
		"blobs/" + h + "/12",
		"instance/uploads/uuid/blobs/" + h + "/12",
	}
	for _, in := range write {
		if got, err := parseWriteResource(in); err == nil {
			t.Fatalf("parseWriteResource(%q) = %+v, want an error", in, got)
		}
	}
}

// TestNegativeSizeIsRejected pins that a size which cannot describe a body is
// refused, since it would otherwise reach the read path as a length.
func TestNegativeSizeIsRejected(t *testing.T) {
	if _, err := parseReadResource("blobs/" + hexOf("body") + "/-1"); err == nil {
		t.Fatalf("a negative size was accepted")
	}
}
