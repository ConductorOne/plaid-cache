// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// A body may be held compressed at rest (see blob.ZstdPath). The plain
// encoding stays canonical for every reader that is handed a path or wants
// plain bytes — the Go toolchain, Bazel's HTTP protocol, identity gRPC reads,
// the shared tier — and the compressed one is served only to a reader that
// asked for zstd. A body held only compressed is expanded to its plain
// encoding the first time a plain reader needs it.

// OpenZstd resolves an action and returns its body's compressed encoding when
// one is held, with Result.Size the plain length. When only the plain encoding
// is held it returns that instead, as Open would, with compressed false.
func (c *Cache) OpenZstd(ctx context.Context, a ids.ActionID) (f *os.File, res Result, compressed bool, err error) {
	e, ok, ierr := c.idx.Get(a)
	if ierr == nil && ok {
		if zf, _, zerr := c.blobs.OpenZstd(e.OutputID); zerr == nil {
			c.touches.enqueue(a, e.LastUsedAt)
			c.metrics.GetLocalHit.Add(1)
			return zf, Result{OutputID: e.OutputID, Size: e.Size, DiskPath: zf.Name(), Time: time.Unix(0, e.CreatedAt)}, true, nil
		}
	}
	f, res, err = c.Open(ctx, a)
	return f, res, false, err
}

// PutStagedZstd records an action whose body is a staged zstd stream that the
// caller has already decoded and verified against o and size.
func (c *Cache) PutStagedZstd(ctx context.Context, a ids.ActionID, o ids.OutputID, stagedPath string, size int64) (string, error) {
	path, diskBytes, err := c.blobs.AdoptZstd(o, stagedPath)
	if err != nil {
		return "", fmt.Errorf("PutStagedZstd: %w", err)
	}
	c.record(a, o, path, size, diskBytes)
	return path, nil
}

// expand writes the plain encoding of a body held only compressed, and reports
// whether it did. A body with no compressed encoding either is a plain miss.
//
// PROTOTYPE GAP: the plain copy's disk bytes are not added to the index's
// accounting, so a body expanded this way is under-charged until it is
// evicted. Closing it needs an index call that adjusts an output's recorded
// disk bytes.
func (c *Cache) expand(o ids.OutputID, size int64) (bool, error) {
	zf, _, err := c.blobs.OpenZstd(o)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("expand: %w", err)
	}
	defer func() { _ = zf.Close() }() // read-only
	dec, err := zstd.NewReader(zf, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return false, fmt.Errorf("expand: %w", err)
	}
	defer dec.Close()
	if _, _, err := c.blobs.Put(o, dec, size); err != nil {
		return false, fmt.Errorf("expand: %w", err)
	}
	return true, nil
}

// plainUploadSource returns a reader of a body's plain bytes for the shared
// tier, which holds plain objects only: other daemons, older versions of this
// one and go-cache-plugin all read objects there as the body itself.
//
// A compressed body is expanded to a temporary file because the backend wants
// a seekable reader for retries.
func plainUploadSource(path string) (io.ReadSeeker, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	if !strings.HasSuffix(path, ".zst") {
		return f, func() { _ = f.Close() }, nil
	}
	defer func() { _ = f.Close() }() // read-only
	tmp, err := os.CreateTemp("", "plaid-upload-*")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	defer dec.Close()
	if _, err := io.Copy(tmp, dec); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, nil, err
	}
	return tmp, cleanup, nil
}
