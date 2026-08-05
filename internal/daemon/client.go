// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
)

// ErrNoDaemon reports that no daemon could be reached or started. Callers
// treat it as a signal to fall back to direct operation rather than to fail.
var ErrNoDaemon = errors.New("daemon unreachable")

// Conn is a connection to the daemon that owns its own read buffer.
//
// The buffer must belong to the connection rather than to whoever happens to
// be reading it. The daemon writes the control response and the first bytes
// of the session back to back, so a reader created just to consume the
// control line routinely pulls session bytes into a buffer that is then
// discarded. That loses the protocol handshake and hangs the build, and it
// does so only when the two writes land in one read — which is to say, only
// against a warm daemon, and not on the cold start that a test is most
// likely to exercise.
type Conn struct {
	net.Conn
	br *bufio.Reader
}

// newConn wraps a dialed connection.
func newConn(c net.Conn) *Conn { return &Conn{Conn: c, br: bufio.NewReader(c)} }

// Read reads through the connection's buffer, so no buffered byte is lost
// between one reader and the next.
func (c *Conn) Read(p []byte) (int, error) { return c.br.Read(p) }

// CloseWrite half-closes the connection. net.Conn does not declare it, so it
// is forwarded explicitly rather than relying on embedding to promote it.
func (c *Conn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// ReadJSONLine decodes one newline-delimited JSON value from the connection.
func (c *Conn) ReadJSONLine(v any) error {
	line, err := c.br.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("ReadJSONLine: %w", err)
	}
	if err := json.Unmarshal(line, v); err != nil {
		return fmt.Errorf("ReadJSONLine: %w", err)
	}
	return nil
}

// spawnBackoff is the retry schedule for connecting to a daemon we just
// started. The first few attempts are fast because a warm start is
// milliseconds; the tail is slower because a cold start pays for opening
// Pebble and replaying its write-ahead log.
var spawnBackoff = []time.Duration{
	10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond,
	80 * time.Millisecond, 150 * time.Millisecond, 250 * time.Millisecond,
	500 * time.Millisecond, 750 * time.Millisecond, 1 * time.Second,
	1 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second,
}

// maxVersionReplacements bounds how many times a client will shut a
// mismatched daemon down and try again, so that two client versions racing to
// replace each other cannot loop forever.
const maxVersionReplacements = 2

// Connect returns a connection to the daemon for the given operation,
// starting a daemon if none is running.
//
// The caller must close the returned connection.
func Connect(ctx context.Context, cfg *config.Config, version string, op Op, logf cache.Logf) (*Conn, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	for attempt := 0; attempt <= maxVersionReplacements; attempt++ {
		conn, err := dialOrSpawn(ctx, cfg, logf)
		if err != nil {
			return nil, err
		}
		resp, err := handshake(conn, version, op)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("Connect: %w", err)
		}
		if resp.OK {
			return conn, nil
		}
		_ = conn.Close()

		if resp.Version == version || resp.Version == "" {
			return nil, fmt.Errorf("Connect: daemon refused: %s", resp.Err)
		}
		// A daemon built from different code is holding the index. Replace it
		// rather than share an on-disk format neither side agreed on.
		logf("daemon version %s != client %s, replacing", resp.Version, version)
		if err := requestShutdown(ctx, cfg, resp.Version); err != nil {
			logf("shutdown mismatched daemon: %v", err)
		}
		if err := waitSocketGone(ctx, cfg.SocketPath()); err != nil {
			return nil, fmt.Errorf("Connect: %w", err)
		}
	}
	return nil, fmt.Errorf("Connect: %w: gave up replacing mismatched daemon", ErrNoDaemon)
}

// respawnAfter is how long to keep dialing a daemon we started before starting
// another one.
//
// A daemon can exit for a reason that has already cleared by the time we notice:
// a predecessor mid-shutdown removes its socket before it releases the index
// lock, so a daemon spawned inside that window loses the lock and exits quietly,
// and it is the only one anybody was going to start. Waiting out the whole
// backoff ladder on a socket that will never appear costs the build its cache and
// ends in direct mode. The window is milliseconds wide, so trying again shortly
// is enough.
const respawnAfter = 600 * time.Millisecond

// maxSpawns bounds the retries, so an index that genuinely cannot be opened
// produces a handful of short-lived processes rather than a fork storm.
const maxSpawns = 4

// dialOrSpawn connects to a running daemon, starting one if needed.
func dialOrSpawn(ctx context.Context, cfg *config.Config, logf cache.Logf) (*Conn, error) {
	if conn, err := net.Dial("unix", cfg.SocketPath()); err == nil {
		return newConn(conn), nil
	}
	if err := spawn(cfg, logf); err != nil {
		return nil, fmt.Errorf("dialOrSpawn: %w", err)
	}
	spawns := 1
	var sinceSpawn time.Duration
	var lastErr error
	for _, d := range spawnBackoff {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
		conn, err := net.Dial("unix", cfg.SocketPath())
		if err == nil {
			return newConn(conn), nil
		}
		lastErr = err
		sinceSpawn += d
		if sinceSpawn < respawnAfter || spawns >= maxSpawns {
			continue
		}
		// Retrying costs one fork against a daemon that is merely slow to bind:
		// it loses the lock to the one already starting and exits.
		if serr := spawn(cfg, logf); serr != nil {
			return nil, fmt.Errorf("dialOrSpawn: %w", serr)
		}
		spawns++
		sinceSpawn = 0
	}
	return nil, fmt.Errorf("dialOrSpawn: %w after %d spawns: %v", ErrNoDaemon, spawns, lastErr)
}

// spawn starts a detached daemon.
//
// Losing a spawn race is expected and harmless: several clients may start at
// once, but Pebble's exclusive directory lock lets exactly one of them open
// the index, and the losers exit while their clients retry dialing the
// winner. That is why this does not attempt any locking of its own.
func spawn(cfg *config.Config, logf cache.Logf) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("spawn: %w", err)
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("spawn: %w", err)
	}
	// A detached process has no terminal to inherit, so its diagnostics would
	// be lost without a file to land in.
	log, err := os.OpenFile(cfg.LogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("spawn: open log: %w", err)
	}
	// This handle exists to be inherited: the spawned daemon holds its own, and
	// writes through that one long after this descriptor is gone.
	defer func() { _ = log.Close() }()

	cmd := exec.Command(self, "serve")
	cmd.Stdin = nil
	cmd.Stdout = log
	cmd.Stderr = log
	detach(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn: %w", err)
	}
	logf("spawned daemon pid %d", cmd.Process.Pid)
	// Release rather than Wait: the daemon outlives this process, and leaving
	// it as an unreaped child would tie its lifetime to ours.
	return cmd.Process.Release()
}

// handshake performs the control exchange.
func handshake(conn *Conn, version string, op Op) (HelloResponse, error) {
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return HelloResponse{}, fmt.Errorf("handshake: %w", err)
	}
	if err := writeJSONLine(conn, Hello{Version: version, Op: op}); err != nil {
		return HelloResponse{}, fmt.Errorf("handshake: write: %w", err)
	}
	var resp HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		return HelloResponse{}, fmt.Errorf("handshake: %w", err)
	}
	// Clear the deadline so a long build is not cut off mid-session.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return HelloResponse{}, fmt.Errorf("handshake: %w", err)
	}
	return resp, nil
}

// requestShutdown asks a running daemon to exit.
func requestShutdown(ctx context.Context, cfg *config.Config, daemonVersion string) error {
	c, err := net.Dial("unix", cfg.SocketPath())
	if err != nil {
		return fmt.Errorf("requestShutdown: %w", err)
	}
	conn := newConn(c)
	defer func() { _ = conn.Close() }()
	// Speak the daemon's own version so it does not reject us for mismatch
	// before it reads the op.
	if _, err := handshake(conn, daemonVersion, OpShutdown); err != nil {
		return fmt.Errorf("requestShutdown: %w", err)
	}
	return nil
}

// WaitSocketGone blocks until the socket path disappears, bounding how long a
// caller waits for a daemon it asked to shut down.
func WaitSocketGone(ctx context.Context, path string) error { return waitSocketGone(ctx, path) }

// waitSocketGone blocks until the socket path disappears.
func waitSocketGone(ctx context.Context, path string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return errors.New("waitSocketGone: socket still present after shutdown")
}

// RunPlugin relays the GOCACHEPROG protocol between the Go toolchain and the
// daemon.
//
// The relay is deliberately byte-level: this process does not parse the
// protocol at all, so it cannot desynchronize the stream, and it costs
// almost nothing to start for every `go` invocation.
func RunPlugin(ctx context.Context, cfg *config.Config, version string, stdin io.Reader, stdout io.Writer, logf cache.Logf) error {
	conn, err := Connect(ctx, cfg, version, OpSession, logf)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return relay(conn, stdin, stdout)
}

// relay copies in both directions until the daemon closes the session.
func relay(conn *Conn, stdin io.Reader, stdout io.Writer) error {
	// Signal end-of-input to the daemon when the toolchain closes its side,
	// so the daemon's decoder sees EOF rather than blocking forever.
	go func() {
		_, _ = io.Copy(conn, stdin)
		_ = conn.CloseWrite()
	}()

	// The downstream direction is authoritative: when the daemon has nothing
	// more to say, the session is over. The upstream goroutine may still be
	// parked reading stdin, which is fine because this process exits next.
	if _, err := io.Copy(stdout, conn); err != nil {
		return fmt.Errorf("relay: %w", err)
	}
	return nil
}

// Dial connects to a daemon that is already running, without starting one.
//
// Status and shutdown queries must not bring a daemon into existence as a
// side effect of asking whether one exists.
func Dial(cfg *config.Config) (*Conn, error) {
	c, err := net.Dial("unix", cfg.SocketPath())
	if err != nil {
		return nil, fmt.Errorf("Dial: %w", err)
	}
	return newConn(c), nil
}
