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

// outf writes to the command's output.
//
// It exists so that the error every Fprintf returns is discarded deliberately
// in one place, with the reason written down, rather than at a hundred call
// sites where the reason would be assumed. There is nothing a command can do
// about a failed write to its own stdout: the exit code already says whether
// the work succeeded, and a diagnostic about being unable to print has nowhere
// left to go. The common case is not even a fault — `plaid-cache stats | head`
// closes the pipe, and the report ending early is exactly what was asked for.
func (a *app) outf(format string, args ...any) {
	_, _ = fmt.Fprintf(a.stdout, format, args...)
}

// errf writes one diagnostic, for the same reasons. Callers supply the
// "plaid-cache: " prefix themselves, so that a message composed from several
// pieces carries it once rather than once per piece.
func (a *app) errf(format string, args ...any) {
	_, _ = fmt.Fprintf(a.stderr, format, args...)
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
	case "stats":
		return a.runStats(ctx)
	case "gc":
		return a.runGC(ctx)
	case "adopt":
		return a.runAdopt(ctx)
	case "clean":
		return a.runClean(ctx)
	case "version", "-v", "--version":
		a.outf("%s\n", versionString())
		return exitOK
	case "help", "-h", "--help":
		a.usage(a.stdout)
		return exitOK
	default:
		a.errf("plaid-cache: unknown subcommand %q\n\n", sub)
		a.usage(a.stderr)
		return exitUsage
	}
}

// usage prints the command summary.
func (a *app) usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `plaid-cache is a GOCACHEPROG-compatible Go build cache.

Usage:
  plaid-cache              speak the GOCACHEPROG protocol on stdin/stdout
  plaid-cache serve        run the cache daemon in the foreground
  plaid-cache status       report cache contents and counters
  plaid-cache stats        report the persisted activity history
  plaid-cache gc           force one eviction pass
  plaid-cache adopt DIR    import a go-cache-plugin stage into this cache
  plaid-cache clean        remove the entire local cache
  plaid-cache version      print build information

The Go toolchain invokes the first form:
  GOCACHEPROG=plaid-cache go build ./...

status describes the cache now; stats describes what it has done. The counters
are persisted, so they survive the daemon's idle exit and cover every process
that has used this cache rather than whichever one answers. -json emits the
whole history for a tool to read.

adopt imports an existing go-cache-plugin local stage: it reconstructs the
action-to-output mapping from that stage's records and publishes its bodies by
hardlink, so nothing is copied and the imported bytes count toward this cache's
size ceiling. It asks a running daemon to do the import, so a machine that is
already building does not have to stop to migrate its old cache.

Both serve and gc accept -max-bytes and -ttl, which override the environment.
On gc the override applies to that pass only, and is forwarded to a running
daemon rather than being silently ignored by it.

serve also accepts -bazel-addr and -bazel-grpc-addr, which serve Bazel's two
remote-cache protocols beside the GOCACHEPROG socket, over the same local store
and shared tier:
  plaid-cache serve -bazel-addr localhost:9095
  bazel build --remote_cache=http://localhost:9095 //...

  plaid-cache serve -bazel-grpc-addr localhost:9096
  bazel build --remote_cache=grpc://localhost:9096 //...

Prefer gRPC. It is the only one of the two that can be asked which blobs the
cache already holds, so an action that re-runs stops re-uploading outputs the
cache has. Both may be served at once, and a build that uses either reads what
the other stored.

-pprof-addr serves Go runtime profiles on its own loopback address rather than
adding them to the Bazel listener. It is off unless asked for and has no
authentication:
  plaid-cache serve -pprof-addr 127.0.0.1:6060
  go tool pprof http://127.0.0.1:6060/debug/pprof/heap

-bazel-monitoring adds two routes to the HTTP address — /status and /metrics —
so a daemon serving a room full of builders can be read without a shell on its
host. They are off unless asked for, because they describe the host rather than
the cache's contents:
  plaid-cache serve -bazel-addr localhost:9095 -bazel-monitoring
  plaid-cache status -from localhost:9095
  curl localhost:9095/metrics

/metrics is Prometheus text exposition, which an OpenTelemetry Collector
scrapes as it stands. status -from prints the same report as a local status,
minus the lines that would describe this machine rather than that one.

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
