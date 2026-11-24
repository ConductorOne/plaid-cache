// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// TestActionCacheExpiredSharedBlob lets a cold node recover when only the
// output body expires from the shared tier, leaving its metadata intact.
func TestActionCacheExpiredSharedBlob(t *testing.T) {
	backend := &sharedActionCache{uploads: make(chan struct{}, 4)}
	writer := newHarnessWithRemote(t, true, nil, backend)
	reader := newHarnessWithRemote(t, true, nil, backend)
	output := []byte("an output shared across builds")
	outputDigest := digestOf(output)
	action := digestOf([]byte("an action with a shared output"))
	putBlob(t, writer, output)
	_, err := writer.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		ActionDigest: action,
		ActionResult: &repb.ActionResult{
			OutputFiles: []*repb.OutputFile{{Path: "out", Digest: outputDigest}},
		},
	})
	if err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}
	backend.waitForUpload(t)
	backend.waitForUpload(t)

	request := &repb.GetActionResultRequest{ActionDigest: action}
	if _, err := reader.ac.GetActionResult(ctx(t), request); err != nil {
		t.Fatalf("GetActionResult with shared output: %v", err)
	}
	backend.objects.Delete(ids.OutputID(mustDigest(t, outputDigest.GetHash())))
	if _, err := reader.ac.GetActionResult(ctx(t), request); status.Code(err) != codes.NotFound {
		t.Fatalf("GetActionResult after shared output expired = %v, want NotFound", err)
	}

	putBlob(t, reader, output)
	backend.waitForUpload(t)
	cold := newHarnessWithRemote(t, true, nil, backend)
	if _, err := cold.ac.GetActionResult(ctx(t), request); err != nil {
		t.Fatalf("GetActionResult on a cold node after repopulating the output: %v", err)
	}
}

// TestActionCacheMissingDirectoryFile checks nested files for either directory
// encoding and allows an action hit after the missing file has been rebuilt.
func TestActionCacheMissingDirectoryFile(t *testing.T) {
	for _, format := range []string{"tree", "directory"} {
		t.Run(format, func(t *testing.T) {
			h := newHarness(t)
			body := []byte("nested output")
			child := &repb.Directory{Files: []*repb.FileNode{{Name: "out", Digest: digestOf(body)}}}
			childBytes, err := proto.Marshal(child)
			if err != nil {
				t.Fatalf("marshal child: %v", err)
			}
			root := &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "subdir", Digest: digestOf(childBytes)},
			}}
			directory := &repb.OutputDirectory{Path: "output"}
			if format == "tree" {
				directory.TreeDigest = putTree(t, h, &repb.Tree{Root: root, Children: []*repb.Directory{child}})
			} else {
				rootBytes, err := proto.Marshal(root)
				if err != nil {
					t.Fatalf("marshal root: %v", err)
				}
				putBlob(t, h, rootBytes)
				putBlob(t, h, childBytes)
				directory.RootDirectoryDigest = digestOf(rootBytes)
			}
			action := digestOf([]byte("directory action"))
			_, err = h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
				ActionDigest: action,
				ActionResult: &repb.ActionResult{OutputDirectories: []*repb.OutputDirectory{directory}},
			})
			if err != nil {
				t.Fatalf("UpdateActionResult: %v", err)
			}
			request := &repb.GetActionResultRequest{ActionDigest: action}
			if _, err := h.ac.GetActionResult(ctx(t), request); status.Code(err) != codes.NotFound {
				t.Fatalf("GetActionResult without nested file = %v, want NotFound", err)
			}
			putBlob(t, h, body)
			if _, err := h.ac.GetActionResult(ctx(t), request); err != nil {
				t.Fatalf("GetActionResult after restoring nested file: %v", err)
			}
		})
	}
}

// sharedActionCache lets multiple test nodes share a backend that can lose
// objects independently of the action records that reference them.
type sharedActionCache struct {
	remote.Noop
	actions sync.Map
	objects sync.Map
	uploads chan struct{}
}

// GetAction fetches a record independently of the output object.
func (s *sharedActionCache) GetAction(_ context.Context, action ids.ActionID) (ids.OutputID, time.Time, error) {
	if output, ok := s.actions.Load(action); ok {
		return output.(ids.OutputID), time.Now(), nil
	}
	return ids.OutputID{}, time.Time{}, remote.ErrMiss
}

// PutAction signals once both the body and its action mapping are published.
func (s *sharedActionCache) PutAction(ctx context.Context, action ids.ActionID, output ids.OutputID, _ time.Time) error {
	s.actions.Store(action, output)
	select {
	case s.uploads <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetObject models an expired object with the same miss as the shared backend.
func (s *sharedActionCache) GetObject(_ context.Context, output ids.OutputID) (io.ReadCloser, int64, error) {
	if body, ok := s.objects.Load(output); ok {
		data := body.([]byte)
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	}
	return nil, 0, remote.ErrMiss
}

// PutObject copies the stream because the cache closes it after upload.
func (s *sharedActionCache) PutObject(_ context.Context, output ids.OutputID, body io.ReadSeeker, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.objects.LoadOrStore(output, data)
	return nil
}

// waitForUpload waits for asynchronous writes before simulating a cold read.
func (s *sharedActionCache) waitForUpload(t *testing.T) {
	t.Helper()
	select {
	case <-s.uploads:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shared cache upload")
	}
}
