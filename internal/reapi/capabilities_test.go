// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// TestCapabilitiesDescribeACacheOnlyServer pins what a client is told at
// connection time.
//
// The digest function has to be SHA-256 because that is already this cache's
// identifier for a body; the batch limit has to be the one the batch RPCs
// enforce, or a client that obeys it gets refused; and execution capabilities
// have to be absent, so a client pointed here to run actions finds out at once
// instead of one failed call at a time.
func TestCapabilitiesDescribeACacheOnlyServer(t *testing.T) {
	h := newHarness(t)
	caps, err := h.caps.GetCapabilities(ctx(t), &repb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}

	cc := caps.GetCacheCapabilities()
	if got := cc.GetDigestFunctions(); len(got) != 1 || got[0] != repb.DigestFunction_SHA256 {
		t.Fatalf("digest functions = %v, want [SHA256]", got)
	}
	if !cc.GetActionCacheUpdateCapabilities().GetUpdateEnabled() {
		t.Fatalf("the action cache is advertised as read-only")
	}
	if got := cc.GetMaxBatchTotalSizeBytes(); got != maxBatchTotalSizeBytes {
		t.Fatalf("advertised batch limit = %d, want the %d the batch RPCs enforce", got, int64(maxBatchTotalSizeBytes))
	}
	if caps.GetExecutionCapabilities() != nil {
		t.Fatalf("a cache-only server advertised execution capabilities")
	}
	if caps.GetLowApiVersion().GetMajor() != 2 || caps.GetHighApiVersion().GetMajor() != 2 {
		t.Fatalf("api version range = %v..%v, want v2", caps.GetLowApiVersion(), caps.GetHighApiVersion())
	}
}

// TestAdvertisedCompressorsAreTheOnesServed pins that the compressors named in
// Capabilities are exactly the ones the resource-name grammar accepts. A client
// takes this list at its word and will send what it says.
func TestAdvertisedCompressorsAreTheOnesServed(t *testing.T) {
	h := newHarness(t)
	caps, err := h.caps.GetCapabilities(ctx(t), &repb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	for _, c := range caps.GetCacheCapabilities().GetSupportedCompressors() {
		if c == repb.Compressor_IDENTITY {
			continue
		}
		name := ""
		for n, v := range compressorByName {
			if v == c {
				name = n
			}
		}
		if name == "" {
			t.Fatalf("compressor %v is advertised but no resource name selects it", c)
		}
	}
}
