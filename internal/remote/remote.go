// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package remote implements the shared tier of the cache: a bucket that
// several machines read from and write to.
//
// The tier is deliberately best-effort. A build must never fail, and should
// never stall, because the network or the bucket is unavailable; a cache that
// can break a build is worse than no cache. Callers therefore treat every
// error here as a miss, and uploads run in the background.
package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// ErrMiss reports that a key is absent. It wraps fs.ErrNotExist so callers can
// use errors.Is against either.
var ErrMiss = fmt.Errorf("remote: key not present: %w", fs.ErrNotExist)

// Backend is the shared tier.
//
// Every method must be safe for concurrent use. A miss is reported as an
// error wrapping fs.ErrNotExist, never as a nil value with a nil error, so
// that a miss cannot be mistaken for an empty object.
type Backend interface {
	// GetAction resolves an action to the output it produced.
	GetAction(ctx context.Context, a ids.ActionID) (ids.OutputID, time.Time, error)

	// PutAction records that an action produced an output. Actions are
	// overwritten in place: re-running an action legitimately yields a new
	// output when the toolchain changes.
	PutAction(ctx context.Context, a ids.ActionID, o ids.OutputID, mtime time.Time) error

	// GetObject opens a body. The caller must close the reader.
	GetObject(ctx context.Context, o ids.OutputID) (io.ReadCloser, int64, error)

	// PutObject stores a body. Bodies are content-addressed and therefore
	// immutable, so an implementation may skip the write if the key exists.
	PutObject(ctx context.Context, o ids.OutputID, r io.ReadSeeker, size int64) error

	// Close releases any resources and waits for in-flight work.
	Close() error
}

// actionRecord is the body of an action object: the output ID in hex and the
// original modification time in Unix nanoseconds, space separated.
//
// The timestamp is carried across machines so that a body faulted in from the
// shared tier lands locally with the mtime it had when it was produced,
// keeping age-based decisions consistent between a machine that built an
// object and one that merely downloaded it.
func formatActionRecord(o ids.OutputID, mtime time.Time) string {
	return o.String() + " " + strconv.FormatInt(mtime.UnixNano(), 10)
}

// parseActionRecord is the inverse of formatActionRecord.
func parseActionRecord(b []byte) (ids.OutputID, time.Time, error) {
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return ids.OutputID{}, time.Time{}, errors.New("parseActionRecord: want 2 fields")
	}
	o, err := ids.ParseOutputID(f[0])
	if err != nil {
		return ids.OutputID{}, time.Time{}, fmt.Errorf("parseActionRecord: %w", err)
	}
	ns, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return ids.OutputID{}, time.Time{}, fmt.Errorf("parseActionRecord: timestamp: %w", err)
	}
	return o, time.Unix(ns/1e9, ns%1e9), nil
}

// keyspace builds remote object keys.
//
// The layout is deliberately identical to tailscale/go-cache-plugin: actions
// and outputs live in separate key spaces, sharded by the first byte of the
// id, and an action's body is the hex output id and a Unix-nanosecond
// timestamp separated by a space. The two tools can therefore read and write
// the same bucket and prefix, which makes migrating between them, or running
// a mixed fleet, a configuration change rather than a cache flush.
//
// Splitting actions from outputs is what allows many actions to share one
// stored body, since outputs are content-addressed. richardartoul/gobuildcache
// takes the other approach — one self-contained object per action, keyed by
// the action id alone — so its entries are not readable here, and it gets no
// cross-action deduplication of identical outputs.
//
// The shard directory also keeps any single listing prefix small, which
// matters for a bucket holding millions of objects.
type keyspace struct{ prefix string }

// actionKey returns the key for an action record.
func (k keyspace) actionKey(a ids.ActionID) string {
	s := a.String()
	return path.Join(k.prefix, "action", s[:2], s)
}

// objectKey returns the key for a body.
func (k keyspace) objectKey(o ids.OutputID) string {
	s := o.String()
	return path.Join(k.prefix, "output", s[:2], s)
}

// Noop is a Backend that stores nothing and misses on every read. It is what
// plaid-cache uses when no bucket is configured, which is the default: there
// is no bucket worth guessing, and running local-only must be a first-class
// mode rather than a degraded one.
type Noop struct{}

// GetAction always misses.
func (Noop) GetAction(context.Context, ids.ActionID) (ids.OutputID, time.Time, error) {
	return ids.OutputID{}, time.Time{}, ErrMiss
}

// PutAction discards the record.
func (Noop) PutAction(context.Context, ids.ActionID, ids.OutputID, time.Time) error { return nil }

// GetObject always misses.
func (Noop) GetObject(context.Context, ids.OutputID) (io.ReadCloser, int64, error) {
	return nil, 0, ErrMiss
}

// PutObject discards the body.
func (Noop) PutObject(context.Context, ids.OutputID, io.ReadSeeker, int64) error { return nil }

// Close is a no-op.
func (Noop) Close() error { return nil }
