// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"slices"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/config"
)

// TestReservedSegmentsMatchConfig pins that configuration refuses exactly the
// instance names this package could not parse back out of a resource name. A
// name configuration accepted but the parser split differently would be listed
// and still refused.
func TestReservedSegmentsMatchConfig(t *testing.T) {
	var got []string
	for k := range resourceKeywords {
		got = append(got, k)
	}
	want := slices.Clone(config.ReservedInstanceSegments)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("resourceKeywords = %v, config.ReservedInstanceSegments = %v", got, want)
	}
}

// TestListedInstanceSharesTheUnnamedKeyspace pins what listing a name means:
// a client sending it reads and writes the same entries as one sending none,
// over every service.
func TestListedInstanceSharesTheUnnamedKeyspace(t *testing.T) {
	const name = "ci/linux-arm64"
	h := newHarnessWithInstances(t, true, []string{name})

	// Action cache: written under the name, read without it.
	action := digestOf([]byte("some action"))
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		InstanceName: name,
		ActionDigest: action,
		ActionResult: result(0, "it worked"),
	}); err != nil {
		t.Fatalf("UpdateActionResult under %q: %v", name, err)
	}
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); err != nil {
		t.Fatalf("GetActionResult unnamed: %v", err)
	}

	// ByteStream: uploaded under the name, read back without it.
	body := []byte("a blob written under a listed instance")
	d := digestOf(body)
	upload(t, h, name+"/"+writeName("u1", d), body, len(body))
	got, err := download(t, h, readName(d))
	if err != nil {
		t.Fatalf("download unnamed: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("download = %q, want %q", got, body)
	}
	if got, err = download(t, h, name+"/"+readName(d)); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("download under %q = %q, %v; want %q", name, got, err, body)
	}

	// CAS: the blob is present to a client sending the name.
	resp, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		InstanceName: name,
		BlobDigests:  []*repb.Digest{d},
	})
	if err != nil {
		t.Fatalf("FindMissingBlobs under %q: %v", name, err)
	}
	if len(resp.GetMissingBlobDigests()) != 0 {
		t.Fatalf("missing = %v, want none", resp.GetMissingBlobDigests())
	}
}

// TestUnlistedInstanceIsRefusedEverywhere pins that a name nobody listed is
// refused by every service, the action cache included. A client whose action
// results were answered while every blob they named was refused would see hits
// it could never use.
func TestUnlistedInstanceIsRefusedEverywhere(t *testing.T) {
	h := newHarnessWithInstances(t, true, []string{"listed"})
	const name = "not-listed"
	d := digestOf([]byte("x"))

	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{
		InstanceName: name, ActionDigest: d,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetActionResult = %v, want InvalidArgument", err)
	}
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		InstanceName: name, ActionDigest: d, ActionResult: result(0, "x"),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateActionResult = %v, want InvalidArgument", err)
	}
	if _, err := h.cas.FindMissingBlobs(ctx(t), &repb.FindMissingBlobsRequest{
		InstanceName: name, BlobDigests: []*repb.Digest{d},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("FindMissingBlobs = %v, want InvalidArgument", err)
	}
	if _, err := download(t, h, name+"/"+readName(d)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Read = %v, want InvalidArgument", err)
	}
}

// TestDefaultServerRefusesNamedActionCacheRequests pins that the action cache
// follows the same default as the other services: with nothing listed, only the
// unnamed instance is served.
func TestDefaultServerRefusesNamedActionCacheRequests(t *testing.T) {
	h := newHarness(t)
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{
		InstanceName: "some-instance", ActionDigest: digestOf([]byte("x")),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetActionResult = %v, want InvalidArgument", err)
	}
}

// TestResourceNamesWithAListedInstanceParse pins the parser side, including a
// multi-segment name, which is where the boundary search could go wrong.
func TestResourceNamesWithAListedInstanceParse(t *testing.T) {
	h := hexOf("body")
	set := newInstanceSet([]string{"a/b"})
	if _, err := parseReadResource("a/b/blobs/"+h+"/12", set); err != nil {
		t.Fatalf("read under a listed instance: %v", err)
	}
	if _, err := parseWriteResource("a/b/uploads/u/blobs/"+h+"/12", set); err != nil {
		t.Fatalf("write under a listed instance: %v", err)
	}
	if _, err := parseReadResource("a/blobs/"+h+"/12", set); err == nil {
		t.Fatalf("read under a prefix of a listed instance was accepted")
	}
}
