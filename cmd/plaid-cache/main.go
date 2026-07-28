// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Command plaid-cache is a GOCACHEPROG-compatible build cache for the Go
// toolchain, with bounded local growth and an optional shared remote tier.
//
// With no subcommand it speaks the GOCACHEPROG protocol on stdin and stdout,
// which is how the Go toolchain invokes it:
//
//	GOCACHEPROG=plaid-cache go build ./...
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

// Build information, set by the release pipeline via -ldflags. The
// ReadBuildInfo fallback covers `go install ...@latest`, which passes no
// ldflags at all and would otherwise report an empty version.
var (
	version = ""
	commit  = ""
	date    = ""
)

// Exit codes.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// app holds the process's I/O so that run can be driven from a test without
// touching the real stdio or calling os.Exit.
type app struct {
	args   []string
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

func main() {
	a := &app{
		args:   os.Args[1:],
		stdin:  os.Stdin,
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
	os.Exit(a.run())
}

// run dispatches a subcommand and returns the process exit code. It never
// calls os.Exit, so tests can drive it directly.
func (a *app) run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var sub string
	if len(a.args) > 0 {
		sub = a.args[0]
	}

	switch sub {
	case "":
		// No subcommand: act as the GOCACHEPROG plugin. This is the hot path,
		// executed once per `go` invocation.
		return a.runPlugin(ctx)
	case "serve":
		return a.runServe(ctx)
	case "status":
		return a.runStatus(ctx)
	case "gc":
		return a.runGC(ctx)
	case "adopt":
		return a.runAdopt(ctx)
	case "clean":
		return a.runClean(ctx)
	case "version", "-v", "--version":
		fmt.Fprintln(a.stdout, versionString())
		return exitOK
	case "help", "-h", "--help":
		a.usage(a.stdout)
		return exitOK
	default:
		fmt.Fprintf(a.stderr, "plaid-cache: unknown subcommand %q\n\n", sub)
		a.usage(a.stderr)
		return exitUsage
	}
}

// usage prints the command summary.
func (a *app) usage(w io.Writer) {
	fmt.Fprint(w, `plaid-cache is a GOCACHEPROG-compatible Go build cache.

Usage:
  plaid-cache              speak the GOCACHEPROG protocol on stdin/stdout
  plaid-cache serve        run the cache daemon in the foreground
  plaid-cache status       report cache contents and counters
  plaid-cache gc           force one eviction pass
  plaid-cache adopt DIR    import a go-cache-plugin stage into this cache
  plaid-cache clean        remove the entire local cache
  plaid-cache version      print build information

The Go toolchain invokes the first form:
  GOCACHEPROG=plaid-cache go build ./...

adopt imports an existing go-cache-plugin local stage: it reconstructs the
action-to-output mapping from that stage's records and publishes its bodies by
hardlink, so nothing is copied and the imported bytes count toward this cache's
size ceiling. It asks a running daemon to do the import, so a machine that is
already building does not have to stop to migrate its old cache.

Both serve and gc accept -max-bytes and -ttl, which override the environment.
On gc the override applies to that pass only, and is forwarded to a running
daemon rather than being silently ignored by it.

Configuration is read from the environment; see the README for the full list.
`)
}

// versionString renders build information, falling back to the module's
// recorded version when the release pipeline's ldflags are absent.
func versionString() string {
	v, c, d := version, commit, date
	if v == "" {
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
			v = bi.Main.Version
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					if c == "" {
						c = s.Value
					}
				case "vcs.time":
					if d == "" {
						d = s.Value
					}
				}
			}
		}
	}
	if v == "" {
		v = "devel"
	}
	s := "plaid-cache " + v
	if c != "" {
		s += " (" + c + ")"
	}
	if d != "" {
		s += " built " + d
	}
	return s
}

// buildVersion is the identity the daemon and its clients compare. A daemon
// running different code than its client interprets the same on-disk index,
// so a mismatch must be resolved rather than tolerated.
func buildVersion() string { return versionString() }
