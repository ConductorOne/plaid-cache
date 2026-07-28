// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// seedLegacyStage writes one go-cache-plugin record and its body under dir, the
// way that cache does: the action file is named for the action id and holds the
// output id and the body size.
func seedLegacyStage(t *testing.T, dir, seed string, body []byte) (ids.ActionID, ids.OutputID) {
	t.Helper()
	a := ids.ActionID(sha256.Sum256([]byte(seed)))
	o := ids.OutputID(sha256.Sum256(body))

	bodyPath := filepath.Join(dir, "output", o.String()[:2], o.String())
	if err := os.MkdirAll(filepath.Dir(bodyPath), 0o755); err != nil {
		t.Fatalf("mkdir body: %v", err)
	}
	if err := os.WriteFile(bodyPath, body, 0o644); err != nil {
		t.Fatalf("write body: %v", err)
	}
	actionPath := filepath.Join(dir, "action", a.String()[:2], a.String())
	if err := os.MkdirAll(filepath.Dir(actionPath), 0o755); err != nil {
		t.Fatalf("mkdir action: %v", err)
	}
	if err := os.WriteFile(actionPath, []byte(fmt.Sprintf("%s %d\n", o, len(body))), 0o644); err != nil {
		t.Fatalf("write action: %v", err)
	}
	return a, o
}

// requestAdopt performs the control exchange and returns the daemon's response.
func requestAdopt(t *testing.T, cfg *config.Config, p *AdoptParams) AdoptResponse {
	t.Helper()
	conn := dialServer(t, cfg)
	if err := writeJSONLine(conn, Hello{Version: testVersion, Op: OpAdopt, Adopt: p}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var hello HelloResponse
	if err := conn.ReadJSONLine(&hello); err != nil {
		t.Fatalf("hello response: %v", err)
	}
	if !hello.OK {
		t.Fatalf("daemon refused adopt: %s", hello.Err)
	}
	var resp AdoptResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		t.Fatalf("adopt response: %v", err)
	}
	return resp
}

// TestAdoptOverTheSocketImportsWhileTheDaemonHoldsTheIndex is the whole point of
// the operation.
//
// Exactly one process may hold the index, and on the machine that actually needs
// a stage migrated that process is the daemon, busy serving builds: an env
// switching to plaid-cache starts using the daemon at the same moment its old
// stage becomes dead weight. Importing in the client meant the migration had to
// win a race against every build on the machine, which on a busy one it does not.
// Here the daemon is running and serving, and the import still lands.
func TestAdoptOverTheSocketImportsWhileTheDaemonHoldsTheIndex(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	legacy := filepath.Join(cfg.Dir, "legacy-stage")
	body := []byte("compiled output bytes")
	action, output := seedLegacyStage(t, legacy, "action-one", body)

	resp := requestAdopt(t, cfg, &AdoptParams{Dir: legacy})
	if resp.Err != "" {
		t.Fatalf("adopt over the socket: %s", resp.Err)
	}
	if resp.Records != 1 || resp.Adopted != 1 {
		t.Fatalf("records=%d adopted=%d, want 1 and 1", resp.Records, resp.Adopted)
	}
	if resp.Linked != 1 || resp.Copied != 0 {
		t.Fatalf("linked=%d copied=%d, want the body hardlinked", resp.Linked, resp.Copied)
	}

	// The entry must be in the index the daemon holds, not merely reported.
	e, ok, err := ts.idx.Get(action)
	if err != nil {
		t.Fatalf("index get: %v", err)
	}
	if !ok {
		t.Fatal("the adopted action is not in the daemon's index")
	}
	if e.OutputID != output {
		t.Fatalf("indexed output %s, want %s", e.OutputID, output)
	}

	// And the body must be readable through the daemon's own store, with the
	// content the stage had.
	got, _, err := ts.blobs.Get(output)
	if err != nil {
		t.Fatalf("blob get: %v", err)
	}
	b, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("read adopted body: %v", err)
	}
	if string(b) != string(body) {
		t.Fatalf("adopted body reads %q, want %q", b, body)
	}
}

// TestAdoptOverTheSocketServesABuildAfterwards pins that the import leaves the
// daemon usable. A migration that wedged the process serving every build on the
// machine would be worse than no migration.
func TestAdoptOverTheSocketServesABuildAfterwards(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	legacy := filepath.Join(cfg.Dir, "legacy-stage")
	action, _ := seedLegacyStage(t, legacy, "action-two", []byte("some object"))

	if resp := requestAdopt(t, cfg, &AdoptParams{Dir: legacy}); resp.Adopted != 1 {
		t.Fatalf("adopted=%d, want 1", resp.Adopted)
	}

	// A status request is the cheapest proof the daemon is still answering, and
	// it must now count the imported entry.
	conn := dialServer(t, cfg)
	if err := writeJSONLine(conn, Hello{Version: testVersion, Op: OpStatus}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var hello HelloResponse
	if err := conn.ReadJSONLine(&hello); err != nil {
		t.Fatalf("hello response: %v", err)
	}
	var st StatusResponse
	if err := conn.ReadJSONLine(&st); err != nil {
		t.Fatalf("status after adopt: %v", err)
	}
	if st.Actions < 1 {
		t.Fatalf("status reports %d actions after importing one", st.Actions)
	}
	if _, ok, err := ts.idx.Get(action); err != nil || !ok {
		t.Fatalf("imported entry not readable after the import: ok=%v err=%v", ok, err)
	}
}

// TestAdoptOverTheSocketDryRunWritesNothing pins that a dry run is inspection
// only, even though it runs inside the process that could write.
func TestAdoptOverTheSocketDryRunWritesNothing(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	legacy := filepath.Join(cfg.Dir, "legacy-stage")
	action, _ := seedLegacyStage(t, legacy, "action-three", []byte("body bytes"))

	resp := requestAdopt(t, cfg, &AdoptParams{Dir: legacy, DryRun: true})
	if resp.Err != "" {
		t.Fatalf("dry run: %s", resp.Err)
	}
	if resp.Adopted != 1 {
		t.Fatalf("dry run reported adopted=%d, want the count it would import", resp.Adopted)
	}
	if _, ok, err := ts.idx.Get(action); err != nil {
		t.Fatalf("index get: %v", err)
	} else if ok {
		t.Fatal("a dry run wrote to the index")
	}
}

// TestAdoptOverTheSocketRejectsAnEmptyRequest pins that a request naming no stage
// is refused rather than treated as some default directory.
func TestAdoptOverTheSocketRejectsAnEmptyRequest(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	for name, p := range map[string]*AdoptParams{
		"nil":       nil,
		"empty dir": {Dir: ""},
	} {
		if resp := requestAdopt(t, cfg, p); resp.Err == "" {
			t.Fatalf("%s: accepted a request naming no stage", name)
		}
	}
}

// TestAdoptWithoutABodyStoreFailsRatherThanPanics pins the guard on a daemon
// assembled without a body store. Adoption publishes bodies directly, so there is
// nothing to publish into — and a nil dereference here would take down the
// process every build on the machine depends on.
func TestAdoptWithoutABodyStoreFailsRatherThanPanics(t *testing.T) {
	cfg := newTestConfig(t)
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	c := cache.New(cache.Params{Config: cfg, Index: ix, Remote: remote.Noop{}})
	t.Cleanup(func() { _ = c.Close() })

	s := NewServer(ServerParams{Config: cfg, Cache: c, Index: ix, Version: testVersion})
	resp := s.adopt(t.Context(), &AdoptParams{Dir: filepath.Join(cfg.Dir, "legacy-stage")})
	if resp.Err == "" {
		t.Fatal("a daemon with no body store reported a successful import")
	}
	// The message is the point. adopt.Run refuses a nil store on its own, so what
	// this guard adds is a reason naming the daemon rather than the internals of a
	// package the reader has no way to connect to their situation.
	if !strings.Contains(resp.Err, "no body store") {
		t.Fatalf("error does not say what is missing: %q", resp.Err)
	}
}
