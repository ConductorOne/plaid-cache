// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
)

// fakeDaemon answers one control exchange on the real socket path, recording what
// it was asked for. It stands in for a running daemon so that the routing in
// runAdopt can be tested without a second process: what is under test here is
// which way the client goes, not what the daemon does with the request.
type fakeDaemon struct {
	hello  chan daemon.Hello
	reply  daemon.AdoptResponse
	closed chan struct{}
}

func startFakeDaemon(t *testing.T, cfg *config.Config, reply daemon.AdoptResponse) *fakeDaemon {
	t.Helper()
	if err := os.MkdirAll(cfg.SocketDir(), 0o700); err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	ln, err := net.Listen("unix", cfg.SocketPath())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeDaemon{hello: make(chan daemon.Hello, 1), reply: reply, closed: make(chan struct{})}
	t.Cleanup(func() { _ = ln.Close(); <-f.closed })
	go func() {
		defer close(f.closed)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		dec := json.NewDecoder(conn)
		var h daemon.Hello
		if err := dec.Decode(&h); err != nil {
			return
		}
		f.hello <- h
		enc := json.NewEncoder(conn)
		_ = enc.Encode(daemon.HelloResponse{Version: buildVersion(), OK: true})
		_ = enc.Encode(f.reply)
	}()
	return f
}

// TestAdoptPrefersARunningDaemon pins that the import is asked of the process that
// holds the index rather than attempted against a lock it cannot take.
//
// This is the regression this whole operation exists for. Adoption writes to the
// index, exactly one process may hold it, and on the machine that actually needs a
// stage migrated that process is the daemon serving builds. Opening the index here
// instead would fail for as long as anything is building — which on a busy env is
// always.
func TestAdoptPrefersARunningDaemon(t *testing.T) {
	a, out, errb := newApp(t, "adopt", "/some/stage")
	cfg, ok := a.loadConfig()
	if !ok {
		t.Fatal("loadConfig")
	}
	fake := startFakeDaemon(t, cfg, daemon.AdoptResponse{
		Records: 3, Adopted: 2, AlreadyPresent: 1, Linked: 2, Bytes: 4096, Elapsed: "12ms",
	})

	if code := a.run(); code != exitOK {
		t.Fatalf("adopt exit=%d, want 0 (stderr: %s)", code, errb.String())
	}
	select {
	case h := <-fake.hello:
		if h.Op != daemon.OpAdopt {
			t.Fatalf("daemon asked for op %q, want %q", h.Op, daemon.OpAdopt)
		}
		if h.Adopt == nil || h.Adopt.Dir != "/some/stage" {
			t.Fatalf("stage not forwarded to the daemon: %+v", h.Adopt)
		}
		if h.Adopt.DryRun {
			t.Fatal("forwarded a dry run that was not requested")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the daemon was never asked: adopt did not take the socket path")
	}

	// The counts must be the daemon's, rendered by the same formatter the local
	// path uses — a second formatter is a second thing to drift.
	for _, want := range []string{"3 records", "2 adopted", "2 linked", "1 already present", "12ms"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}

// TestAdoptForwardsDryRunToTheDaemon pins that inspection stays inspection when
// the daemon is the one doing the work.
func TestAdoptForwardsDryRunToTheDaemon(t *testing.T) {
	a, out, errb := newApp(t, "adopt", "-dry-run", "/some/stage")
	cfg, ok := a.loadConfig()
	if !ok {
		t.Fatal("loadConfig")
	}
	fake := startFakeDaemon(t, cfg, daemon.AdoptResponse{Records: 1, Adopted: 1, Elapsed: "1ms"})

	if code := a.run(); code != exitOK {
		t.Fatalf("adopt -dry-run exit=%d (stderr: %s)", code, errb.String())
	}
	h := <-fake.hello
	if h.Adopt == nil || !h.Adopt.DryRun {
		t.Fatalf("dry run not forwarded: %+v", h.Adopt)
	}
	if !strings.Contains(out.String(), "would adopt:") {
		t.Fatalf("dry run not labelled in the output:\n%s", out.String())
	}
}

// TestAdoptReportsADaemonSideFailure pins that an error from the daemon fails the
// command. A migration that silently reported success would leave the caller
// believing a stage is safe to delete when it is not.
func TestAdoptReportsADaemonSideFailure(t *testing.T) {
	a, _, errb := newApp(t, "adopt", "/some/stage")
	cfg, ok := a.loadConfig()
	if !ok {
		t.Fatal("loadConfig")
	}
	startFakeDaemon(t, cfg, daemon.AdoptResponse{Err: "adopt: stage unreadable"})

	if code := a.run(); code == exitOK {
		t.Fatal("adopt reported success on a daemon-side failure")
	}
	if !strings.Contains(errb.String(), "stage unreadable") {
		t.Fatalf("the daemon's reason was not reported: %s", errb.String())
	}
}

// TestAdoptWithoutADaemonHoldsTheIndexItself pins that the fallback still works,
// so a machine with no daemon running can still migrate a stage.
func TestAdoptWithoutADaemonHoldsTheIndexItself(t *testing.T) {
	dir := t.TempDir()
	stage := filepath.Join(dir, "stage")
	seedCLIStage(t, stage, []byte("output bytes"))

	a, out, errb := newApp(t, "adopt", stage)
	if code := a.run(); code != exitOK {
		t.Fatalf("adopt with no daemon exit=%d (stderr: %s)", code, errb.String())
	}
	if !strings.Contains(out.String(), "1 adopted") {
		t.Fatalf("the local path did not import the stage:\n%s", out.String())
	}
}

// TestAdoptSaysWhoTookTheIndex pins the message on the narrow race where a daemon
// appears between the dial and the open. It is a retry, not a misconfiguration,
// and the wording should say so.
func TestAdoptSaysWhoTookTheIndex(t *testing.T) {
	a, _, errb := newApp(t, "adopt", "/some/stage")
	cfg, ok := a.loadConfig()
	if !ok {
		t.Fatal("loadConfig")
	}
	// Hold the index without listening on the socket: no daemon answers, so the
	// client falls through to opening the index and finds it locked.
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	if code := a.run(); code == exitOK {
		t.Fatal("adopt succeeded with the index held by another process")
	}
	if !strings.Contains(errb.String(), "try again") {
		t.Fatalf("the message does not suggest retrying: %s", errb.String())
	}
}

// seedCLIStage writes one go-cache-plugin record and its body under dir.
func seedCLIStage(t *testing.T, dir string, body []byte) {
	t.Helper()
	a := ids.ActionID(sha256.Sum256([]byte("cli-action")))
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
	rec := fmt.Sprintf("%s %d\n", o, len(body))
	if err := os.WriteFile(actionPath, []byte(rec), 0o644); err != nil {
		t.Fatalf("write action: %v", err)
	}
}
