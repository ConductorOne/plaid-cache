// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"strings"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// result is a small ActionResult a test can store and recognise again.
func result(code int32, stdout string) *repb.ActionResult {
	return &repb.ActionResult{
		ExitCode:     code,
		StdoutDigest: digestOf([]byte(stdout)),
	}
}

// storedResult is result with its stdout blob already in the CAS, as it is
// when Bazel records a result: the server serves a result only while every
// blob it names is available.
func (h *harness) storedResult(t *testing.T, code int32, stdout string) *repb.ActionResult {
	t.Helper()
	r := result(code, stdout)
	d, err := digest(r.GetStdoutDigest())
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if err := h.store.Put(ctx(t), bazel.KindCAS, d, strings.NewReader(stdout)); err != nil {
		t.Fatalf("store stdout: %v", err)
	}
	return r
}

// TestActionCacheRoundTrip pins that what an action produced comes back.
func TestActionCacheRoundTrip(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("some action"))
	want := h.storedResult(t, 0, "it worked")

	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		ActionDigest: action,
		ActionResult: want,
	}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}

	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got.GetExitCode() != want.GetExitCode() {
		t.Fatalf("exit code = %d, want %d", got.GetExitCode(), want.GetExitCode())
	}
	if got.GetStdoutDigest().GetHash() != want.GetStdoutDigest().GetHash() {
		t.Fatalf("stdout digest = %q, want %q", got.GetStdoutDigest().GetHash(), want.GetStdoutDigest().GetHash())
	}
}

// TestActionCacheMissIsNotFound pins that an unknown action is a miss the
// client can act on, and not an error it has to interpret.
func TestActionCacheMissIsNotFound(t *testing.T) {
	h := newHarness(t)
	_, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{
		ActionDigest: digestOf([]byte("an action that never ran")),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("miss = %v, want NotFound", err)
	}
}

// TestActionCacheOverwrites pins that a later run replaces an earlier one,
// which is what makes a stale entry recoverable rather than permanent.
func TestActionCacheOverwrites(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("an action that gets re-run"))

	for _, code := range []int32{1, 0} {
		if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
			ActionDigest: action,
			ActionResult: h.storedResult(t, code, "output"),
		}); err != nil {
			t.Fatalf("UpdateActionResult(%d): %v", code, err)
		}
	}

	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got.GetExitCode() != 0 {
		t.Fatalf("exit code = %d, want the second run's 0", got.GetExitCode())
	}
}

// TestActionResultAndActionShareOneDigestWithoutColliding pins the hazard that
// makes namespacing load-bearing rather than tidy.
//
// Bazel stores an action's Action message in the CAS under the very digest that
// keys its ActionResult in the action cache, so every action issues two writes
// under one digest. Indexed under the raw digest they would overwrite each
// other, and every lookup after that would answer the wrong question.
func TestActionResultAndActionShareOneDigestWithoutColliding(t *testing.T) {
	h := newHarness(t)

	// The CAS blob is a real Action message; the digest naming it is its own
	// content hash, exactly as Bazel would compute it.
	actionBlob := []byte("a serialised Action message")
	shared := digestOf(actionBlob)
	putBlob(t, h, actionBlob)

	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		ActionDigest: shared,
		ActionResult: h.storedResult(t, 0, "the action's output"),
	}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}

	// The CAS blob must still be the Action message, byte for byte.
	read, err := h.cas.BatchReadBlobs(ctx(t), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{shared}})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if !bytes.Equal(read.GetResponses()[0].GetData(), actionBlob) {
		t.Fatalf("the action-cache write overwrote the CAS blob under the same digest")
	}

	// And the action-cache entry must still be the result.
	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: shared})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got.GetExitCode() != 0 || got.GetStdoutDigest() == nil {
		t.Fatalf("action result did not survive the CAS write: %v", got)
	}
}

// TestActionCacheReadsWhatTheHTTPTransportWrote pins that the two front ends
// are two ways to reach one cache.
//
// The HTTP transport stores a serialised ActionResult as an opaque body. If the
// two disagreed about the keyspace or the encoding, a build that uploaded over
// one and read over the other would silently miss everything.
func TestActionCacheReadsWhatTheHTTPTransportWrote(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("an action stored the other way"))
	d := mustDigest(t, action.GetHash())

	body := marshalResult(t, h.storedResult(t, 7, "written over http"))
	if err := h.store.Put(ctx(t), bazel.KindAC, d, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if got.GetExitCode() != 7 {
		t.Fatalf("exit code = %d, want the 7 stored over HTTP", got.GetExitCode())
	}
}

// TestActionCacheTreatsAnUnparseableBodyAsAMiss pins that a body which is not
// an ActionResult — which only the opaque HTTP keyspace can produce — costs a
// re-run rather than an error the client cannot recover from.
func TestActionCacheTreatsAnUnparseableBodyAsAMiss(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("an action with a corrupt entry"))
	d := mustDigest(t, action.GetHash())

	if err := h.store.Put(ctx(t), bazel.KindAC, d, bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unparseable entry = %v, want NotFound", err)
	}
}

// TestActionCacheRejectsAMalformedDigest pins that a digest of the wrong width
// is refused rather than zero-padded into a key that names something else.
func TestActionCacheRejectsAMalformedDigest(t *testing.T) {
	h := newHarness(t)
	_, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{
		ActionDigest: &repb.Digest{Hash: "abcd", SizeBytes: 2},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("short digest = %v, want InvalidArgument", err)
	}
}

// TestActionCacheMissesWhenAnOutputIsGone pins that an action result naming a
// blob the cache can no longer serve is answered as a miss. Served as a hit,
// Bazel keeps the result without the blob and fails a later action with "lost
// inputs" when it finally fetches it; as a miss, it re-runs the action.
func TestActionCacheMissesWhenAnOutputIsGone(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("an action whose output expired"))
	out := []byte("compiled package metadata")
	res := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{{Path: "pkg.a", Digest: digestOf(out)}},
	}
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		ActionDigest: action, ActionResult: res,
	}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}

	_, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("result naming an absent output = %v, want NotFound", err)
	}

	d, err := digest(digestOf(out))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if err := h.store.Put(ctx(t), bazel.KindCAS, d, bytes.NewReader(out)); err != nil {
		t.Fatalf("store output: %v", err)
	}
	got, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
	if err != nil {
		t.Fatalf("GetActionResult once the output is back: %v", err)
	}
	if len(got.GetOutputFiles()) != 1 {
		t.Fatalf("output files = %v, want the one stored", got.GetOutputFiles())
	}
	if c := h.srv.ActionResultChecks(); c.Incomplete != 1 || c.Complete != 1 {
		t.Fatalf("checks = %+v, want one incomplete then one complete", c)
	}
}

// TestActionCacheChecksEveryKindOfOutput pins that an output directory's tree
// and the stderr blob are checked as well as output files.
func TestActionCacheChecksEveryKindOfOutput(t *testing.T) {
	for name, res := range map[string]*repb.ActionResult{
		"directory": {OutputDirectories: []*repb.OutputDirectory{{Path: "d", TreeDigest: digestOf([]byte("a tree"))}}},
		"stderr":    {StderrDigest: digestOf([]byte("warnings"))},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			action := digestOf([]byte("action " + name))
			if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
				ActionDigest: action, ActionResult: res,
			}); err != nil {
				t.Fatalf("UpdateActionResult: %v", err)
			}
			_, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action})
			if status.Code(err) != codes.NotFound {
				t.Fatalf("GetActionResult = %v, want NotFound", err)
			}
		})
	}
}

// TestActionCacheServesAResultNamingTheEmptyBlob pins that the empty blob,
// which is never stored, does not make a result incomplete.
func TestActionCacheServesAResultNamingTheEmptyBlob(t *testing.T) {
	h := newHarness(t)
	action := digestOf([]byte("an action with empty output"))
	res := &repb.ActionResult{
		OutputFiles:  []*repb.OutputFile{{Path: "empty", Digest: digestOf(nil)}},
		StdoutDigest: digestOf([]byte{}),
	}
	if _, err := h.ac.UpdateActionResult(ctx(t), &repb.UpdateActionResultRequest{
		ActionDigest: action, ActionResult: res,
	}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}
	if _, err := h.ac.GetActionResult(ctx(t), &repb.GetActionResultRequest{ActionDigest: action}); err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
}
