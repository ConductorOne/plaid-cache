// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
)

// capabilitiesService answers what this server will and will not do.
type capabilitiesService struct {
	repb.UnimplementedCapabilitiesServer
	srv *Server
}

// The API version range served. The cache half of v2 has been stable across
// these minor versions, and nothing here depends on a feature added within
// them, so the range is the whole of v2 up to the revision this was written
// against.
var (
	lowAPIVersion  = &semver.SemVer{Major: 2, Minor: 0}
	highAPIVersion = &semver.SemVer{Major: 2, Minor: 3}
)

// GetCapabilities describes the cache.
//
// The absent field is the informative one: ExecutionCapabilities is nil, which
// is how a client asked to run actions here learns at connection time that it
// cannot, rather than one failed call at a time.
func (c *capabilitiesService) GetCapabilities(_ context.Context, _ *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	return &repb.ServerCapabilities{
		CacheCapabilities: &repb.CacheCapabilities{
			// SHA-256 alone, because a 32-byte SHA-256 digest is already this
			// cache's identifier for a body, which is what lets a Bazel output
			// and a Go build's output of the same bytes be one file.
			DigestFunctions: []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes: maxBatchTotalSizeBytes,
			// Nothing here interprets an ActionResult's symlinks; bodies are
			// opaque and paths are the client's business either way.
			SymlinkAbsolutePathStrategy:     repb.SymlinkAbsolutePathStrategy_ALLOWED,
			SupportedCompressors:            []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD},
			SupportedBatchUpdateCompressors: []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD},
			// No blob-size ceiling is advertised. The bound on what this cache
			// will hold is a byte budget over the whole of it, enforced by
			// eviction, and turning that into a per-blob limit would refuse a
			// large output that the budget has room for.
		},
		LowApiVersion:  lowAPIVersion,
		HighApiVersion: highAPIVersion,
	}, nil
}
