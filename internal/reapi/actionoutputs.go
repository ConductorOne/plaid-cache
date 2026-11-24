// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"io"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// actionOutputCheckTimeout bounds the total cost of probing a shared tier
// whose output metadata may outlive its blobs.
const actionOutputCheckTimeout = 2 * time.Second

// actionOutputsAvailable rejects an action-cache hit if Bazel would fail to
// fetch any of its outputs. A miss lets Bazel regenerate the missing bytes.
func (s *Server) actionOutputsAvailable(ctx context.Context, result *repb.ActionResult) bool {
	ctx, cancel := context.WithTimeout(ctx, actionOutputCheckTimeout)
	defer cancel()

	for _, output := range result.GetOutputFiles() {
		if !s.hasCAS(ctx, output.GetDigest()) {
			return false
		}
	}
	for _, stream := range []*repb.Digest{result.GetStdoutDigest(), result.GetStderrDigest()} {
		if stream != nil && !s.hasCAS(ctx, stream) {
			return false
		}
	}
	for _, output := range result.GetOutputDirectories() {
		tree, root := output.GetTreeDigest(), output.GetRootDirectoryDigest()
		if tree == nil && root == nil {
			return false
		}
		if tree != nil && !s.treeOutputsAvailable(ctx, tree) {
			return false
		}
		if root != nil && !s.directoryOutputsAvailable(ctx, root) {
			return false
		}
	}
	return ctx.Err() == nil
}

// treeOutputsAvailable checks the file bodies in a Tree, which embeds all
// Directory metadata in a single CAS object.
func (s *Server) treeOutputsAvailable(ctx context.Context, digest *repb.Digest) bool {
	var tree repb.Tree
	if !s.readOutputMetadata(ctx, digest, &tree) || tree.GetRoot() == nil {
		return false
	}
	if !s.outputFilesAvailable(ctx, tree.GetRoot()) {
		return false
	}
	for _, directory := range tree.GetChildren() {
		if !s.outputFilesAvailable(ctx, directory) {
			return false
		}
	}
	return true
}

// directoryOutputsAvailable checks directory metadata and file bodies when
// individual Directory messages are stored instead of, or alongside, a Tree.
func (s *Server) directoryOutputsAvailable(ctx context.Context, root *repb.Digest) bool {
	pending := []*repb.Digest{root}
	seen := make(map[string]int64)
	for len(pending) > 0 {
		d := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if size, ok := seen[d.GetHash()]; ok {
			if size != d.GetSizeBytes() {
				return false
			}
			continue
		}
		var directory repb.Directory
		if !s.readOutputMetadata(ctx, d, &directory) || !s.outputFilesAvailable(ctx, &directory) {
			return false
		}
		seen[d.GetHash()] = d.GetSizeBytes()
		for _, child := range directory.GetDirectories() {
			pending = append(pending, child.GetDigest())
		}
	}
	return true
}

// outputFilesAvailable probes file bodies without downloading them.
func (s *Server) outputFilesAvailable(ctx context.Context, directory *repb.Directory) bool {
	for _, file := range directory.GetFiles() {
		if !s.hasCAS(ctx, file.GetDigest()) {
			return false
		}
	}
	return true
}

// readOutputMetadata downloads only bounded directory metadata. An empty
// Directory serializes to zero bytes and can use the implicit empty blob.
func (s *Server) readOutputMetadata(ctx context.Context, ref *repb.Digest, message proto.Message) bool {
	d, err := digest(ref)
	if err != nil || ctx.Err() != nil {
		return false
	}
	if d == emptyDigest {
		return ref.GetSizeBytes() == 0
	}
	if ref.GetSizeBytes() <= 0 || ref.GetSizeBytes() > outputMetadataLimit {
		return false
	}
	file, size, ok := s.store.Open(ctx, bazel.KindCAS, d)
	if !ok {
		return false
	}
	defer func() { _ = file.Close() }()
	if size != ref.GetSizeBytes() {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(file, size+1))
	return err == nil && int64(len(body)) == size && proto.Unmarshal(body, message) == nil
}
