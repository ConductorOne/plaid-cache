// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/bazel"
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

// TestRRCCLocalClosureAcceptsEmptyFiles keeps implicit root and nested files readable on a repository hit.
func TestRRCCLocalClosureAcceptsEmptyFiles(t *testing.T) {
	h := newHarness(t)
	marker := []byte("recorded inputs")
	file := []byte("package refactor")
	putBlob(t, h, marker)
	putBlob(t, h, file)
	empty := digestOf(nil)
	child := &repb.Directory{Files: []*repb.FileNode{{Name: "empty.txt", Digest: empty}}}
	childBody, err := proto.Marshal(child)
	if err != nil {
		t.Fatalf("marshal Directory: %v", err)
	}
	tree := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				{Name: "empty.txt", Digest: empty},
				{Name: "BUILD.bazel", Digest: digestOf(file)},
			},
			Directories: []*repb.DirectoryNode{{Name: "nested", Digest: digestOf(childBody)}},
		},
		Children: []*repb.Directory{child},
	}
	treeDigest := putTree(t, h, tree)
	action := digestOf([]byte("rrcc empty files"))
	putRRCCActionResult(t, h, action, digestOf(marker), treeDigest)

	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	want := &repb.ActionResult{
		OutputFiles:       []*repb.OutputFile{{Path: ".recorded_inputs", Digest: digestOf(marker)}},
		OutputDirectories: []*repb.OutputDirectory{{Path: "repo_contents", TreeDigest: treeDigest}},
	}
	if !proto.Equal(got, want) {
		t.Fatalf("GetActionResult = %v, want %v", got, want)
	}
	body, err := download(t, h, readName(empty))
	if err != nil || len(body) != 0 {
		t.Fatalf("Read empty file = %q, %v, want zero bytes and no error", body, err)
	}
	if h.store.Has(ctx(t), bazel.KindCAS, emptyDigest) {
		t.Fatal("empty blob was physically stored")
	}
}

// TestRRCCLocalClosureRejectsInvalidFileDigests prevents the implicit empty blob from bypassing validation.
func TestRRCCLocalClosureRejectsInvalidFileDigests(t *testing.T) {
	file := []byte("stored non-empty file")
	for _, tt := range []struct {
		name   string
		digest *repb.Digest
	}{
		{name: "nil"},
		{name: "malformed hash", digest: &repb.Digest{Hash: "not-sha256"}},
		{name: "negative size", digest: &repb.Digest{Hash: emptyDigest.String(), SizeBytes: -1}},
		{name: "empty hash with nonzero size", digest: &repb.Digest{Hash: emptyDigest.String(), SizeBytes: 1}},
		{name: "nonempty hash with zero size", digest: &repb.Digest{Hash: digestOf(file).GetHash()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			marker := []byte("recorded inputs")
			putBlob(t, h, marker)
			putBlob(t, h, file)
			tree := putTree(t, h, &repb.Tree{Root: &repb.Directory{
				Files: []*repb.FileNode{{Name: "invalid.txt", Digest: tt.digest}},
			}})
			action := digestOf([]byte("rrcc invalid file " + tt.name))
			putRRCCActionResult(t, h, action, digestOf(marker), tree)
			if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
				t.Fatalf("GetActionResult = %v, want NotFound", err)
			}
		})
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
	tree := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
		{Name: "empty.txt", Digest: digestOf(nil)},
		{Name: "BUILD.bazel", Digest: missing},
	}}}
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
