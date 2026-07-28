// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
)

// limitFlags are the eviction limits a subcommand accepts on the command line.
//
// They are strings rather than typed values so that "not given" is
// distinguishable from "given as zero": zero is meaningful for both limits, it
// disables that constraint. Parsing reuses the configuration package's parsers,
// so a bad value fails with the same message it would from the environment.
type limitFlags struct {
	maxBytes string
	ttl      string
}

// register wires the flags into a flag set.
func (l *limitFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&l.maxBytes, "max-bytes", "",
		"eviction size ceiling, e.g. 20GB or 512MiB (default: PLAID_GOCACHE_MAX_BYTES)")
	fs.StringVar(&l.ttl, "ttl", "",
		"eviction age limit as a Go duration, e.g. 168h (default: PLAID_GOCACHE_TTL)")
}

// parsed returns the values the caller actually supplied. A nil pointer means
// the flag was not given.
func (l *limitFlags) parsed() (maxBytes *int64, ttl *time.Duration, err error) {
	if l.maxBytes != "" {
		n, perr := config.ParseBytes(l.maxBytes)
		if perr != nil {
			return nil, nil, fmt.Errorf("-max-bytes: %w", perr)
		}
		maxBytes = &n
	}
	if l.ttl != "" {
		d, perr := time.ParseDuration(l.ttl)
		if perr != nil {
			return nil, nil, fmt.Errorf("-ttl: %q is not a duration (want e.g. 168h, 90m)", l.ttl)
		}
		if d < 0 {
			return nil, nil, fmt.Errorf("-ttl: %v is negative", d)
		}
		ttl = &d
	}
	return maxBytes, ttl, nil
}

// applyTo overrides a resolved configuration with whatever was given, leaving
// the rest of the environment-derived settings alone. Flags win over the
// environment, which wins over the defaults.
func (l *limitFlags) applyTo(cfg *config.Config) error {
	maxBytes, ttl, err := l.parsed()
	if err != nil {
		return err
	}
	if maxBytes != nil {
		cfg.MaxBytes = *maxBytes
	}
	if ttl != nil {
		cfg.TTL = *ttl
	}
	return nil
}

// gcParams renders the flags as the per-pass override sent to a running daemon,
// or nil when none were given.
func (l *limitFlags) gcParams() (*daemon.GCParams, error) {
	maxBytes, ttl, err := l.parsed()
	if err != nil {
		return nil, err
	}
	if maxBytes == nil && ttl == nil {
		return nil, nil
	}
	p := &daemon.GCParams{MaxBytes: maxBytes}
	if ttl != nil {
		s := ttl.String()
		p.TTL = &s
	}
	return p, nil
}

// parseFlagsAllowingArgs is parseFlags for subcommands that take operands.
func (a *app) parseFlagsAllowingArgs(name string, register func(*flag.FlagSet), args []string) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet("plaid-cache "+name, flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	register(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs, nil
}

// parseFlags parses a subcommand's arguments.
//
// Errors and usage go to the app's stderr rather than os.Stderr so tests can
// capture them, and ContinueOnError keeps a bad flag from calling os.Exit from
// underneath run.
func (a *app) parseFlags(name string, register func(*flag.FlagSet), args []string) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet("plaid-cache "+name, flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	register(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return fs, nil
}
