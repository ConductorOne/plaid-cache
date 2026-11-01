// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRPCMetricsClassifyCalls pins how a finished call is counted: by method
// and by outcome, with a lookup miss kept apart from a failure, into the first
// bucket whose bound covers it, and not at all for a method this server does
// not serve.
func TestRPCMetricsClassifyCalls(t *testing.T) {
	var m rpcMetrics
	ac := "/build.bazel.remote.execution.v2.ActionCache/GetActionResult"
	m.observe(ac, 200*time.Microsecond, nil)
	m.observe(ac, time.Millisecond, status.Error(codes.NotFound, "miss"))
	m.observe(ac, time.Minute, errors.New("broken"))
	m.observe("/some.Other/Method", time.Millisecond, nil)

	got := map[string]RPCMethodSnapshot{}
	for _, c := range m.snapshot().Calls {
		if c.Method != "GetActionResult" {
			t.Fatalf("counted a call to %q, want only GetActionResult", c.Method)
		}
		got[c.Outcome] = c
	}
	if len(got) != 3 {
		t.Fatalf("outcomes = %v, want ok, not_found and error", got)
	}
	if ok := got["ok"]; ok.Count != 1 || ok.Buckets[1] != 1 {
		t.Fatalf("ok = %+v, want one call in the 250µs bucket", ok)
	}
	if nf := got["not_found"]; nf.Count != 1 || nf.Buckets[3] != 1 {
		t.Fatalf("not_found = %+v, want one call in the 1ms bucket", nf)
	}
	// Past the last bound: counted, but in no bucket, which +Inf accounts for.
	if e := got["error"]; e.Count != 1 {
		t.Fatalf("error = %+v, want one call", e)
	} else {
		for _, n := range e.Buckets {
			if n != 0 {
				t.Fatalf("error buckets = %v, want a call past every bound in none of them", e.Buckets)
			}
		}
	}
}
