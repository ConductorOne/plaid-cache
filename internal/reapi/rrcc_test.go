// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestRRCCLocalClosureMetrics records a complete synthetic repository closure.
func TestRRCCLocalClosureMetrics(t *testing.T) {
	h := newHarness(t)
	marker := []byte("recorded inputs")
	file := []byte("package refactor")
	putBlob(t, h, marker)
	putBlob(t, h, file)
	tree := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{Name: "BUILD.bazel", Digest: digestOf(file)}}}}
	treeDigest := putTree(t, h, tree)
	action := digestOf([]byte("rrcc complete"))

	putRRCCActionResult(t, h, action, digestOf(marker), treeDigest)
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got := h.srv.RRCCMetrics(); got.Complete != 1 {
		t.Fatalf("RRCCMetrics = %+v, want one complete closure", got)
	}
}

// TestRRCCLocalClosureMetricsRecordsMissingMarker turns a missing repository marker into a miss.
func TestRRCCLocalClosureMetricsRecordsMissingMarker(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("rrcc missing marker"))
	missingMarker := digestOf([]byte("missing marker"))
	tree := putTree(t, h, &repb.Tree{})

	putRRCCActionResult(t, h, action, missingMarker, tree)
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetActionResult = %v, want NotFound", err)
	}
	if got := h.srv.RRCCMetrics(); got.MarkerMissing != 1 {
		t.Fatalf("RRCCMetrics = %+v, want one missing marker", got)
	}
}

// TestRRCCLocalClosureMetricsRecordsMissingTree turns a missing repository Tree into a miss.
func TestRRCCLocalClosureMetricsRecordsMissingTree(t *testing.T) {
	h := newHarness(t)
	marker := []byte("recorded inputs")
	putBlob(t, h, marker)
	action := digestOf([]byte("rrcc missing tree"))
	missingTree := digestOf([]byte("missing Tree"))

	putRRCCActionResult(t, h, action, digestOf(marker), missingTree)
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetActionResult = %v, want NotFound", err)
	}
	if got := h.srv.RRCCMetrics(); got.TreeMissing != 1 {
		t.Fatalf("RRCCMetrics = %+v, want one missing tree", got)
	}
}

// TestRRCCLocalClosureMetricsRecordsMissingFile turns a missing nested repository file into a miss.
func TestRRCCLocalClosureMetricsRecordsMissingFile(t *testing.T) {
	h := newHarness(t)
	marker := []byte("recorded inputs")
	putBlob(t, h, marker)
	missing := digestOf([]byte("missing BUILD.bazel"))
	tree := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{Name: "BUILD.bazel", Digest: missing}}}}
	treeDigest := putTree(t, h, tree)
	action := digestOf([]byte("rrcc missing file"))

	putRRCCActionResult(t, h, action, digestOf(marker), treeDigest)
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetActionResult = %v, want NotFound", err)
	}
	if got := h.srv.RRCCMetrics(); got.FileMissing != 1 {
		t.Fatalf("RRCCMetrics = %+v, want one missing file", got)
	}
}

// TestRRCCLocalClosureMetricsIgnoreOrdinaryActions keeps normal action-cache hits off RRCC metrics.
func TestRRCCLocalClosureMetricsIgnoreOrdinaryActions(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("ordinary action"))
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{ActionDigest: action, ActionResult: result(0, "ordinary")}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got := h.srv.RRCCMetrics(); got != (RRCCMetricsSnapshot{}) {
		t.Fatalf("RRCCMetrics = %+v, want zero", got)
	}
}

// putRRCCActionResult stores Bazel's synthetic repository-cache result shape.
func putRRCCActionResult(t *testing.T, h *harness, action, marker, tree *repb.Digest) {
	t.Helper()
	result := &repb.ActionResult{
		OutputFiles:       []*repb.OutputFile{{Path: ".recorded_inputs", Digest: marker}},
		OutputDirectories: []*repb.OutputDirectory{{Path: "repo_contents", TreeDigest: tree}},
	}
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{ActionDigest: action, ActionResult: result}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}
}

// putTree stores a Tree as a CAS blob and returns its digest.
func putTree(t *testing.T, h *harness, tree *repb.Tree) *repb.Digest {
	t.Helper()
	body, err := proto.Marshal(tree)
	if err != nil {
		t.Fatalf("marshal Tree: %v", err)
	}
	putBlob(t, h, body)
	return digestOf(body)
}
