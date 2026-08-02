// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"testing"

	"github.com/conductorone/plaid-cache/internal/bazel"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestFindMissingBlobsAnswersHonestly pins the reason this transport exists.
//
// Bazel's HTTP cache client answers its own findMissingDigests by declaring
// every digest absent, so it re-uploads outputs the server already holds every
// time an action re-runs. A stub here that did the same — reporting everything
// missing — would compile, pass a round-trip test, and leave the gRPC path no
// better than the HTTP one. What has to hold is that a stored blob comes back
// present, an unstored one comes back missing, and a mixed request separates
// them correctly.
func TestFindMissingBlobsAnswersHonestly(t *testing.T) {
	h := newHarness(t)

	stored := [][]byte{[]byte("first output"), []byte("second output")}
	absent := [][]byte{[]byte("never uploaded"), []byte("nor this one")}
	for _, b := range stored {
		putBlob(t, h, b)
	}

	all := make([]*repb.Digest, 0, 4)
	for _, b := range stored {
		all = append(all, digestOf(b))
	}
	for _, b := range absent {
		all = append(all, digestOf(b))
	}

	resp, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{BlobDigests: all})
	if err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}

	want := map[string]bool{}
	for _, b := range absent {
		want[digestOf(b).GetHash()] = true
	}
	if len(resp.GetMissingBlobDigests()) != len(want) {
		t.Fatalf("reported %d missing, want %d", len(resp.GetMissingBlobDigests()), len(want))
	}
	for _, d := range resp.GetMissingBlobDigests() {
		if !want[d.GetHash()] {
			t.Fatalf("reported %s missing, but it is stored", d.GetHash())
		}
	}
}

// TestFindMissingBlobsReportsAllOfAnEmptyCache pins the other half: a cache
// with nothing in it must not claim to hold anything.
func TestFindMissingBlobsReportsAllOfAnEmptyCache(t *testing.T) {
	h := newHarness(t)
	digests := []*repb.Digest{digestOf([]byte("a")), digestOf([]byte("b")), digestOf([]byte("c"))}

	resp, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{BlobDigests: digests})
	if err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}
	if len(resp.GetMissingBlobDigests()) != len(digests) {
		t.Fatalf("reported %d of %d missing from an empty cache", len(resp.GetMissingBlobDigests()), len(digests))
	}
}

// TestFindMissingBlobsNeverReportsTheEmptyBlob pins that the blob every client
// is entitled to skip uploading is never asked for.
func TestFindMissingBlobsNeverReportsTheEmptyBlob(t *testing.T) {
	h := newHarness(t)
	resp, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{digestOf(nil)},
	})
	if err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}
	if len(resp.GetMissingBlobDigests()) != 0 {
		t.Fatalf("reported the empty blob missing")
	}
}

// TestPresenceProbesDoNotSkewTheHitRate pins that FindMissingBlobs is counted
// as neither a hit nor a miss.
//
// It is a write-side question — "need I send this?" — and folding it into the
// read counters would make the cache's reported hit rate describe something
// other than how often a build found what it wanted.
func TestPresenceProbesDoNotSkewTheHitRate(t *testing.T) {
	h := newHarness(t)
	putBlob(t, h, []byte("an output"))
	before := h.cache.Metrics()

	if _, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{digestOf([]byte("an output")), digestOf([]byte("absent"))},
	}); err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}

	after := h.cache.Metrics()
	if after.GetLocalHit != before.GetLocalHit || after.GetMiss != before.GetMiss {
		t.Fatalf("probing moved the read counters: hits %d->%d, misses %d->%d",
			before.GetLocalHit, after.GetLocalHit, before.GetMiss, after.GetMiss)
	}
}

// TestBatchUpdateAndReadBlobs pins the small-blob path in both directions.
func TestBatchUpdateAndReadBlobs(t *testing.T) {
	h := newHarness(t)
	bodies := [][]byte{[]byte("one"), []byte("two"), []byte("three")}

	var reqs []*repb.BatchUpdateBlobsRequest_Request
	for _, b := range bodies {
		reqs = append(reqs, &repb.BatchUpdateBlobsRequest_Request{Digest: digestOf(b), Data: b})
	}
	up, err := h.cas.BatchUpdateBlobs(ctx(t), &repb.BatchUpdateBlobsRequest{Requests: reqs})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	for i, r := range up.GetResponses() {
		if c := codes.Code(r.GetStatus().GetCode()); c != codes.OK {
			t.Fatalf("blob %d stored with status %v", i, c)
		}
	}

	var digests []*repb.Digest
	for _, b := range bodies {
		digests = append(digests, digestOf(b))
	}
	down, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{Digests: digests})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if len(down.GetResponses()) != len(bodies) {
		t.Fatalf("read %d blobs, want %d", len(down.GetResponses()), len(bodies))
	}
	for i, r := range down.GetResponses() {
		if c := codes.Code(r.GetStatus().GetCode()); c != codes.OK {
			t.Fatalf("blob %d read with status %v", i, c)
		}
		if !bytes.Equal(r.GetData(), bodies[i]) {
			t.Fatalf("blob %d = %q, want %q", i, r.GetData(), bodies[i])
		}
	}
}

// TestBatchReadReportsAMissPerBlob pins that a missing blob is a per-blob
// status and not a failed call: the rest of the batch is still served, which is
// what stops one evicted output from making a client re-request the others.
func TestBatchReadReportsAMissPerBlob(t *testing.T) {
	h := newHarness(t)
	present := []byte("stored")
	putBlob(t, h, present)

	resp, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{digestOf(present), digestOf([]byte("absent"))},
	})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if c := codes.Code(resp.GetResponses()[0].GetStatus().GetCode()); c != codes.OK {
		t.Fatalf("stored blob read with status %v", c)
	}
	if c := codes.Code(resp.GetResponses()[1].GetStatus().GetCode()); c != codes.NotFound {
		t.Fatalf("absent blob read with status %v, want NotFound", c)
	}
}

// TestBatchUpdateRejectsABadDigest pins that a body which does not hash to the
// address naming it is refused rather than published under it.
func TestBatchUpdateRejectsABadDigest(t *testing.T) {
	h := newHarness(t)
	body := []byte("these are the bytes")
	lie := digestOf([]byte("but this is the digest"))
	lie.SizeBytes = int64(len(body))

	resp, err := h.cas.BatchUpdateBlobs(ctx(t), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: lie, Data: body}},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := codes.Code(resp.GetResponses()[0].GetStatus().GetCode()); c != codes.InvalidArgument {
		t.Fatalf("mismatched body stored with status %v, want InvalidArgument", c)
	}
	if f, _, ok := h.store.Open(ctx(t), bazel.KindCAS, mustDigest(t, lie.GetHash())); ok {
		_ = f.Close()
		t.Fatalf("a body that does not hash to its digest was stored under it")
	}
}

// TestBatchUpdateRejectsAWrongLength pins that the declared size is checked, so
// a client that truncated a blob hears about it rather than storing a short one.
func TestBatchUpdateRejectsAWrongLength(t *testing.T) {
	h := newHarness(t)
	body := []byte("a body of a known length")
	d := digestOf(body)
	d.SizeBytes++

	resp, err := h.cas.BatchUpdateBlobs(ctx(t), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: d, Data: body}},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := codes.Code(resp.GetResponses()[0].GetStatus().GetCode()); c != codes.InvalidArgument {
		t.Fatalf("wrong-length body stored with status %v, want InvalidArgument", c)
	}
}

// TestBatchRoundTripsCompressedBlobs pins that a client using zstd for inline
// batch data gets its bytes back, and that what is stored is the uncompressed
// body — so the same entry serves a client that does not use compression.
func TestBatchRoundTripsCompressedBlobs(t *testing.T) {
	h := newHarness(t)
	body := bytes.Repeat([]byte("compressible "), 500)

	packed, err := encodeAll(body)
	if err != nil {
		t.Fatalf("encodeAll: %v", err)
	}
	up, err := h.cas.BatchUpdateBlobs(ctx(t), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{Digest: digestOf(body), Data: packed, Compressor: repb.Compressor_ZSTD},
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := codes.Code(up.GetResponses()[0].GetStatus().GetCode()); c != codes.OK {
		t.Fatalf("compressed blob stored with status %v", c)
	}

	// Read it back plain: storage keeps one uncompressed copy whatever the wire
	// used.
	plain, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{digestOf(body)}})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if !bytes.Equal(plain.GetResponses()[0].GetData(), body) {
		t.Fatalf("plain read did not return the original body")
	}

	// And compressed, which must decode to the same thing.
	packedResp, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{
		Digests:               []*repb.Digest{digestOf(body)},
		AcceptableCompressors: []repb.Compressor_Value{repb.Compressor_ZSTD},
	})
	if err != nil {
		t.Fatalf("BatchReadBlobs compressed: %v", err)
	}
	got := packedResp.GetResponses()[0]
	if got.GetCompressor() != repb.Compressor_ZSTD {
		t.Fatalf("compressor = %v, want ZSTD", got.GetCompressor())
	}
	back, err := decodeAll(got.GetData())
	if err != nil {
		t.Fatalf("decodeAll: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("compressed read did not decode to the original body")
	}
}

// TestBatchReadRefusesMoreThanItAdvertises pins that the limit published in
// Capabilities is the limit enforced, so a client that respects it is never
// handed a short answer it has to notice.
func TestBatchReadRefusesMoreThanItAdvertises(t *testing.T) {
	h := newHarness(t)
	_, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{{Hash: digestOf([]byte("x")).GetHash(), SizeBytes: maxBatchTotalSizeBytes + 1}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized batch = %v, want InvalidArgument", err)
	}
}

// TestNamedInstanceIsRefused pins that this cache serves one unnamed instance.
//
// Serving several names out of one keyspace would let two logically separate
// caches share entries without either being told, which is a cache-poisoning
// shape rather than a convenience.
func TestNamedInstanceIsRefused(t *testing.T) {
	h := newHarness(t)
	_, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		InstanceName: "some-instance",
		BlobDigests:  []*repb.Digest{digestOf([]byte("x"))},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("named instance = %v, want InvalidArgument", err)
	}
}

// TestGetTreeIsNotServed pins that the one CAS method belonging to remote
// execution says so rather than failing obscurely.
func TestGetTreeIsNotServed(t *testing.T) {
	h := newHarness(t)
	stream, err := h.cas.GetTree(ctx(t), &repb.GetTreeRequest{RootDigest: digestOf([]byte("x"))})
	if err != nil {
		t.Fatalf("GetTree: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetTree = %v, want Unimplemented", err)
	}
}

// TestUnservedDigestFunctionIsRefused pins that a client naming a hash this
// cache cannot compute is told so, rather than having its digests silently
// treated as SHA-256 ones.
func TestUnservedDigestFunctionIsRefused(t *testing.T) {
	h := newHarness(t)
	_, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		BlobDigests:    []*repb.Digest{digestOf([]byte("x"))},
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("blake3 request = %v, want InvalidArgument", err)
	}
}

// putBlob stores one blob through the batch path.
func putBlob(t *testing.T, h *harness, body []byte) {
	t.Helper()
	resp, err := h.cas.BatchUpdateBlobs(ctx(t), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: digestOf(body), Data: body}},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := codes.Code(resp.GetResponses()[0].GetStatus().GetCode()); c != codes.OK {
		t.Fatalf("store returned %v", c)
	}
}

// mustDigest parses a hex digest a test built.
func mustDigest(t *testing.T, hex string) bazel.Digest {
	t.Helper()
	d, err := bazel.ParseDigest(hex)
	if err != nil {
		t.Fatalf("ParseDigest(%q): %v", hex, err)
	}
	return d
}
