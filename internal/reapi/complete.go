// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"sync/atomic"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// ActionResultChecksSnapshot counts action results by whether every blob they
// name could be served when they were looked up.
type ActionResultChecksSnapshot struct {
	Complete   int64 `json:"complete"`
	Incomplete int64 `json:"incomplete"`
}

type actionResultChecks struct {
	complete   atomic.Int64
	incomplete atomic.Int64
}

// ActionResultChecks returns the completeness counts accumulated by this server.
func (s *Server) ActionResultChecks() ActionResultChecksSnapshot {
	return ActionResultChecksSnapshot{
		Complete:   s.resultChecks.complete.Load(),
		Incomplete: s.resultChecks.incomplete.Load(),
	}
}

// outputsAvailable reports whether every blob an action result names can be
// served, locally or from the shared tier, and the first one that cannot.
//
// An action result is a promise about its outputs, and Bazel keeps it without
// keeping them: it downloads only what it needs now and fetches the rest when
// a later action asks, which may be minutes later and on the strength of this
// answer alone. If a named blob is already gone that later fetch fails the
// build as "lost inputs", and a retry is served this same result again. A miss
// here instead makes Bazel run the action, which uploads the outputs afresh.
//
// The probe is a presence check, not a read. Local presence is an index lookup
// and a stat, usually answered from the presence memo, and is also what keeps
// the blob from local eviction and offers it to the shared-tier refresher; only
// a blob absent locally costs a round trip to the bucket.
//
// An output directory is checked by its Tree blob only. Checking every file it
// lists means reading the tree, which is a fetch on the lookup path for an
// output kind the Go build does not produce; the tree and its files are
// written together and refreshed together, so the tree's presence is a good
// proxy.
func (s *Server) outputsAvailable(ctx context.Context, res *repb.ActionResult) (*repb.Digest, bool) {
	check := func(d *repb.Digest) bool {
		if d == nil {
			return true
		}
		return s.hasCAS(ctx, d)
	}
	for _, f := range res.GetOutputFiles() {
		if !check(f.GetDigest()) {
			return f.GetDigest(), false
		}
	}
	for _, d := range res.GetOutputDirectories() {
		if !check(d.GetTreeDigest()) {
			return d.GetTreeDigest(), false
		}
	}
	if !check(res.GetStdoutDigest()) {
		return res.GetStdoutDigest(), false
	}
	if !check(res.GetStderrDigest()) {
		return res.GetStderrDigest(), false
	}
	return nil, true
}
