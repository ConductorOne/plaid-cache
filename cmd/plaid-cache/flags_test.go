// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/config"
)

// TestLimitFlagsUnsetChangeNothing pins that omitting the flags leaves the
// environment-derived configuration alone, so flags are an override and not a
// reset to some flag default.
func TestLimitFlagsUnsetChangeNothing(t *testing.T) {
	var l limitFlags
	cfg := &config.Config{MaxBytes: 123, TTL: 7 * time.Hour}
	if err := l.applyTo(cfg); err != nil {
		t.Fatalf("applyTo: %v", err)
	}
	if cfg.MaxBytes != 123 || cfg.TTL != 7*time.Hour {
		t.Fatalf("unset flags changed the config: %+v", cfg)
	}
	p, err := l.gcParams()
	if err != nil {
		t.Fatalf("gcParams: %v", err)
	}
	if p != nil {
		t.Fatalf("gcParams = %+v with no flags given, want nil", p)
	}
}

// TestLimitFlagsOverrideConfig pins flag-over-environment precedence.
func TestLimitFlagsOverrideConfig(t *testing.T) {
	l := limitFlags{maxBytes: "512MiB", ttl: "90m"}
	cfg := &config.Config{MaxBytes: 123, TTL: 7 * time.Hour}
	if err := l.applyTo(cfg); err != nil {
		t.Fatalf("applyTo: %v", err)
	}
	if cfg.MaxBytes != 512<<20 {
		t.Fatalf("MaxBytes = %d, want %d", cfg.MaxBytes, int64(512<<20))
	}
	if cfg.TTL != 90*time.Minute {
		t.Fatalf("TTL = %v, want 90m", cfg.TTL)
	}
}

// TestLimitFlagsZeroIsMeaningful pins that an explicit zero disables a
// constraint rather than reading as "not given".
//
// This is why the parsed values are pointers: zero is a legitimate setting for
// both limits, so it cannot double as the unset sentinel.
func TestLimitFlagsZeroIsMeaningful(t *testing.T) {
	l := limitFlags{maxBytes: "0", ttl: "0s"}
	cfg := &config.Config{MaxBytes: 999, TTL: time.Hour}
	if err := l.applyTo(cfg); err != nil {
		t.Fatalf("applyTo: %v", err)
	}
	if cfg.MaxBytes != 0 {
		t.Fatalf("MaxBytes = %d, want 0 (an explicit zero must disable the ceiling)", cfg.MaxBytes)
	}
	if cfg.TTL != 0 {
		t.Fatalf("TTL = %v, want 0", cfg.TTL)
	}

	p, err := l.gcParams()
	if err != nil {
		t.Fatalf("gcParams: %v", err)
	}
	if p == nil || p.MaxBytes == nil || *p.MaxBytes != 0 {
		t.Fatalf("gcParams = %+v, want an explicit zero MaxBytes", p)
	}
	if p.TTL == nil || *p.TTL != "0s" {
		t.Fatalf("gcParams TTL = %v, want \"0s\"", p.TTL)
	}

	// Each flag alone must also survive, or a zero could be discarded by a
	// guard that only looks at the other one.
	only := limitFlags{maxBytes: "0"}
	if p, err = only.gcParams(); err != nil {
		t.Fatalf("gcParams: %v", err)
	}
	if p == nil || p.MaxBytes == nil || *p.MaxBytes != 0 {
		t.Fatalf("gcParams for -max-bytes=0 alone = %+v, want an explicit zero", p)
	}
	if p.TTL != nil {
		t.Fatalf("gcParams TTL = %v with only -max-bytes given, want nil", *p.TTL)
	}

	only = limitFlags{ttl: "0s"}
	if p, err = only.gcParams(); err != nil {
		t.Fatalf("gcParams: %v", err)
	}
	if p == nil || p.TTL == nil || *p.TTL != "0s" {
		t.Fatalf("gcParams for -ttl=0s alone = %+v, want an explicit zero", p)
	}
	if p.MaxBytes != nil {
		t.Fatalf("gcParams MaxBytes = %v with only -ttl given, want nil", *p.MaxBytes)
	}
}

// TestLimitFlagsRejectBadValues pins that a typo is a usage error, reported with
// the same wording the environment path uses.
func TestLimitFlagsRejectBadValues(t *testing.T) {
	for _, l := range []limitFlags{
		{maxBytes: "20 gigs"},
		{maxBytes: "-5"},
		{ttl: "nope"},
		{ttl: "-1h"},
	} {
		if err := l.applyTo(&config.Config{}); err == nil {
			t.Fatalf("applyTo(%+v) succeeded, want an error", l)
		}
		if _, err := l.gcParams(); err == nil {
			t.Fatalf("gcParams(%+v) succeeded, want an error", l)
		}
	}
}

// TestGCFlagsAreUsageErrorsNotFailures pins that a bad flag exits as a usage
// error and names the flag, rather than being reported as a cache failure.
func TestGCFlagsAreUsageErrorsNotFailures(t *testing.T) {
	for _, args := range [][]string{
		{"gc", "-max-bytes=20 gigs"},
		{"gc", "-ttl=nope"},
		{"gc", "-bogus"},
		{"gc", "stray-arg"},
		{"serve", "-max-bytes=nope"},
	} {
		clearCacheEnv(t)
		t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
		var out, errb bytes.Buffer
		code := (&app{args: args, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}).run()
		if code != exitUsage {
			t.Fatalf("run(%v) = %d, want %d (stderr: %s)", args, code, exitUsage, &errb)
		}
		if errb.Len() == 0 {
			t.Fatalf("run(%v) produced no diagnostic", args)
		}
	}
}

// TestServePprofAddressFlagOverridesEnvironment pins that an explicit profile
// address wins over the environment, matching the other optional listeners.
func TestServePprofAddressFlagOverridesEnvironment(t *testing.T) {
	a, _, errb := newApp(t, "serve", "-pprof-addr", "not-an-address")
	t.Setenv("PLAID_GOCACHE_PPROF_ADDR", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- a.runServe(ctx) }()

	select {
	case code := <-done:
		if code != exitError {
			t.Fatalf("runServe(-pprof-addr) = %d, want %d (stderr: %s)", code, exitError, errb)
		}
		if !strings.Contains(errb.String(), "pprof listener") {
			t.Fatalf("stderr = %q, want the pprof listener error", errb)
		}
	case <-time.After(5 * time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("runServe did not return after cancelling its context")
		}
		t.Fatal("runServe did not use the explicit pprof address")
	}
}

// TestGCAppliesFlagsWithoutADaemon pins that the flags work on the standalone
// path too, where this process owns the index.
func TestGCAppliesFlagsWithoutADaemon(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Dir: dir, MaxBytes: 1 << 30, TTL: time.Hour,
		TouchGranularity: time.Hour, UploadConcurrency: 1,
	}
	seedCache(t, cfg, bytes.Repeat([]byte("g"), 4096))
	if s := statsAt(t, cfg); s.Actions == 0 {
		t.Fatal("expected the seed to be recorded")
	}

	clearCacheEnv(t)
	t.Setenv("PLAID_GOCACHE_DIR", dir)
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "1GiB") // generous in the environment
	var out, errb bytes.Buffer
	// The flag has to beat the environment, or nothing is pruned.
	code := (&app{args: []string{"gc", "-max-bytes=1"}, stdin: strings.NewReader(""), stdout: &out, stderr: &errb}).run()
	if code != exitOK {
		t.Fatalf("run(gc -max-bytes=1) = %d (stderr: %s)", code, &errb)
	}
	if s := statsAt(t, cfg); s.Actions != 0 {
		t.Fatalf("actions after gc = %d, want 0; the flag did not override the environment", s.Actions)
	}
}
