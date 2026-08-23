// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"io"
	"sync/atomic"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// rrccTreeLimit bounds one observation so a malicious cache entry cannot turn
// an action-cache lookup into unbounded metadata work.
const rrccTreeLimit = 16 << 20

// RRCCMetricsSnapshot reports local-closure observations for Bazel's experimental
// remote repository-contents cache entries.
type RRCCMetricsSnapshot struct {
	Complete      int64
	MarkerMissing int64
	TreeMissing   int64
	FileMissing   int64
	Malformed     int64
}

type rrccMetrics struct {
	complete      atomic.Int64
	markerMissing atomic.Int64
	treeMissing   atomic.Int64
	fileMissing   atomic.Int64
	malformed     atomic.Int64
}

// Snapshot returns the current RRCC local-closure observation counts.
func (m *rrccMetrics) Snapshot() RRCCMetricsSnapshot {
	return RRCCMetricsSnapshot{
		Complete:      m.complete.Load(),
		MarkerMissing: m.markerMissing.Load(),
		TreeMissing:   m.treeMissing.Load(),
		FileMissing:   m.fileMissing.Load(),
		Malformed:     m.malformed.Load(),
	}
}

// RRCCMetrics returns local-only closure observations accumulated by this REAPI server.
func (s *Server) RRCCMetrics() RRCCMetricsSnapshot { return s.rrccMetrics.Snapshot() }

// validateRRCCLocalClosure accepts ordinary action results and synthetic
// repository-cache results whose complete closure is local. A missing local body
// is a cache miss: returning the ActionResult would make Bazel fail later while
// lazily reading the injected repository.
func (s *Server) validateRRCCLocalClosure(ctx context.Context, result *repb.ActionResult) bool {
	marker, tree, ok := rrccOutputs(result)
	if !ok {
		return true
	}
	if !s.hasLocalCAS(ctx, marker) {
		s.rrccMetrics.markerMissing.Add(1)
		s.logf("bazel grpc: rrcc local closure missing marker %s", marker.GetHash())
		return false
	}
	treeDigest, err := digest(tree)
	if err != nil {
		s.rrccMetrics.malformed.Add(1)
		s.logf("bazel grpc: rrcc local closure has malformed tree digest: %v", err)
		return false
	}
	file, size, ok := s.store.OpenLocal(ctx, bazel.KindCAS, treeDigest)
	if !ok {
		s.rrccMetrics.treeMissing.Add(1)
		s.logf("bazel grpc: rrcc local closure missing tree %s", tree.GetHash())
		return false
	}
	defer func() { _ = file.Close() }()
	if size > rrccTreeLimit {
		s.rrccMetrics.malformed.Add(1)
		s.logf("bazel grpc: rrcc local closure tree %s is %d bytes, refusing to inspect", tree.GetHash(), size)
		return false
	}
	body, err := io.ReadAll(io.LimitReader(file, rrccTreeLimit))
	if err != nil {
		s.rrccMetrics.malformed.Add(1)
		s.logf("bazel grpc: read rrcc tree %s: %v", tree.GetHash(), err)
		return false
	}
	var contents repb.Tree
	if err := proto.Unmarshal(body, &contents); err != nil {
		s.rrccMetrics.malformed.Add(1)
		s.logf("bazel grpc: rrcc tree %s does not parse: %v", tree.GetHash(), err)
		return false
	}
	for _, directory := range append([]*repb.Directory{contents.GetRoot()}, contents.GetChildren()...) {
		for _, node := range directory.GetFiles() {
			if !s.hasLocalCAS(ctx, node.GetDigest()) {
				s.rrccMetrics.fileMissing.Add(1)
				s.logf("bazel grpc: rrcc local closure missing file %s", node.GetDigest().GetHash())
				return false
			}
		}
	}
	s.rrccMetrics.complete.Add(1)
	return true
}

// hasLocalCAS reports whether a valid digest is currently present in the local cache.
func (s *Server) hasLocalCAS(ctx context.Context, d *repb.Digest) bool {
	parsed, err := digest(d)
	return err == nil && s.store.Has(ctx, bazel.KindCAS, parsed)
}

// rrccOutputs recognizes Bazel's synthetic remote repository-contents result shape.
func rrccOutputs(result *repb.ActionResult) (marker, tree *repb.Digest, ok bool) {
	for _, output := range result.GetOutputFiles() {
		if output.GetPath() == ".recorded_inputs" {
			marker = output.GetDigest()
		}
	}
	for _, output := range result.GetOutputDirectories() {
		if output.GetPath() == "repo_contents" {
			tree = output.GetTreeDigest()
		}
	}
	return marker, tree, marker != nil && tree != nil
}
