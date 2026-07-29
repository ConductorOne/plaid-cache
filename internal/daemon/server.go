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
	"sync"
	"time"

	"github.com/conductorone/plaid-cache/internal/adopt"
	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/wire"
)

// Server is the long-lived process that owns the index.
type Server struct {
	cfg     *config.Config
	cache   *cache.Cache
	idx     *index.Index
	blobs   *blob.Store
	logf    cache.Logf
	version string
	started time.Time

	mu        sync.Mutex
	active    int
	idleTimer *time.Timer

	// maxGets bounds concurrent get handling per session. Zero means
	// maxConcurrentGets; a test lowers it so a wedged session can be made to
	// hold every slot deterministically.
	maxGets int

	// sessionIdle overrides sessionIdleTimeout. Zero uses the constant; a test
	// shortens it so a silent session can be observed being closed.
	sessionIdle time.Duration

	conns    sync.WaitGroup
	stopped  chan struct{}
	stopOnce sync.Once
}

// ServerParams carries the daemon's dependencies.
type ServerParams struct {
	Config *config.Config
	Cache  *cache.Cache
	Index  *index.Index

	// Blobs is the body store. It is passed separately from Cache because
	// adoption publishes bodies directly rather than through a get or a put.
	Blobs *blob.Store

	Logf    cache.Logf
	Version string

	// MaxConcurrentGets overrides the per-session get bound. Zero uses
	// maxConcurrentGets.
	MaxConcurrentGets int

	// SessionIdleTimeout overrides sessionIdleTimeout. Zero uses the constant.
	SessionIdleTimeout time.Duration
}

// maxConcurrentGets bounds in-flight remote faults per session.
//
// The bound is per session rather than per daemon on purpose. A shared
// semaphore couples unrelated builds: a client that stops reading its responses
// leaves its handlers parked in a write, and once they hold every slot the
// decode loop of every other session blocks acquiring one. One stuck build
// would stall all of them.
const maxConcurrentGets = 64

// sessionIdleTimeout closes a session that has gone silent.
//
// Without it a connection that completes the handshake and then says nothing
// keeps the daemon's active-connection count above zero, so the idle timer can
// never fire and the daemon lives forever. Gaps between cache requests during a
// long compile are seconds, not hours, so an hour of silence means the peer is
// gone.
const sessionIdleTimeout = time.Hour

// maxAcceptFailures and acceptRetryDelay bound how long Serve tolerates a
// listener that keeps failing before it gives up.
const (
	maxAcceptFailures = 10
	acceptRetryDelay  = 100 * time.Millisecond
)

// NewServer constructs a Server. Call Serve to run it.
func NewServer(p ServerParams) *Server {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Server{
		cfg:         p.Config,
		cache:       p.Cache,
		idx:         p.Index,
		blobs:       p.Blobs,
		logf:        logf,
		version:     p.Version,
		started:     time.Now(),
		maxGets:     p.MaxConcurrentGets,
		sessionIdle: p.SessionIdleTimeout,
		stopped:     make(chan struct{}),
	}
}

// sessionIdleTimeout returns the effective per-session silence budget.
func (s *Server) sessionIdleTimeout() time.Duration {
	if s.sessionIdle > 0 {
		return s.sessionIdle
	}
	return sessionIdleTimeout
}

// Listen binds the unix socket.
//
// A socket file left behind by a daemon that died without cleaning up would
// make every future client fail to connect, so an existing path that nothing
// is listening on is removed rather than treated as an error. The exclusive
// Pebble lock, not the socket, is what actually prevents two daemons.
func Listen(cfg *config.Config) (net.Listener, error) {
	path := cfg.SocketPath()
	if _, err := os.Stat(path); err == nil {
		if c, derr := net.Dial("unix", path); derr == nil {
			c.Close()
			return nil, fmt.Errorf("Listen: another daemon is already listening on %s", path)
		}
		if rerr := os.Remove(path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			return nil, fmt.Errorf("Listen: remove stale socket: %w", rerr)
		}
	}
	// Narrow the containing directory before binding. bind creates the socket
	// with umask-derived permissions and only the chmod below narrows it, so
	// for that window the socket may be connectable by anyone who can reach
	// it. A caller who reaches it can serve build outputs to a compile, so the
	// window is worth closing rather than shrinking: a directory only its
	// owner may traverse makes the socket unreachable regardless of its own
	// mode. This matters most when SocketDir falls back under the system
	// temporary directory, which is world writable.
	dir := cfg.SocketDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("Listen: create socket dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("Listen: chmod socket dir: %w", err)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("Listen: %w", err)
	}
	// The cache holds build outputs; only its owner should be able to read or
	// poison them. Belt and braces with the directory mode above.
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("Listen: chmod socket: %w", err)
	}
	return ln, nil
}

// Serve accepts connections until the daemon is asked to stop, the context is
// cancelled, or the idle timeout expires.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	defer ln.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Both loops touch the index, and the caller closes the index once Serve
	// returns, so Serve must not return while either is mid-write. Cancelling
	// first and waiting second is the whole of it; the wait is registered as one
	// closure rather than a second defer because the accept-failure path returns
	// without cancelling, and a bare wait ahead of the cancel would hang there.
	var bg sync.WaitGroup
	if !s.cfg.DisableEviction && s.cfg.EvictInterval > 0 {
		bg.Add(1)
		go func() { defer bg.Done(); s.evictLoop(ctx) }()
	}
	bg.Add(1)
	go func() { defer bg.Done(); s.metricsLoop(ctx) }()
	defer func() { cancel(); bg.Wait() }()
	s.armIdleTimer()

	// Unblock Accept when we are asked to stop: a unix listener has no other
	// way to interrupt a blocked accept.
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopped:
		}
		ln.Close()
	}()

	var acceptFails int
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopped:
				s.drain()
				return nil
			case <-ctx.Done():
				s.drain()
				return ctx.Err()
			default:
			}
			// A transient accept failure — typically the process or system
			// running out of descriptors — must not take the daemon down with
			// it, because every build on this machine depends on it. Back off and
			// keep serving; give up only if it never clears.
			acceptFails++
			if acceptFails > maxAcceptFailures {
				return fmt.Errorf("Serve: accept: %w", err)
			}
			s.logf("accept: %v (retry %d/%d)", err, acceptFails, maxAcceptFailures)
			select {
			case <-time.After(acceptRetryDelay):
			case <-s.stopped:
				s.drain()
				return nil
			case <-ctx.Done():
				s.drain()
				return ctx.Err()
			}
			continue
		}
		acceptFails = 0
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			s.handle(ctx, conn)
		}()
	}
}

// drain waits for in-flight connections to finish.
func (s *Server) drain() {
	s.conns.Wait()
	s.mu.Lock()
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.mu.Unlock()
}

// Stop asks Serve to return.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopped) })
}

// Stopped reports the channel closed when the daemon is asked to stop.
func (s *Server) Stopped() <-chan struct{} { return s.stopped }

// armIdleTimer starts or restarts the countdown to an idle exit. It is called
// with no connections active.
func (s *Server) armIdleTimer() {
	if s.cfg.IdleTimeout <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.idleTimer = time.AfterFunc(s.cfg.IdleTimeout, func() {
		s.mu.Lock()
		idle := s.active == 0
		s.mu.Unlock()
		if idle {
			s.logf("idle for %v, exiting", s.cfg.IdleTimeout)
			s.Stop()
		}
	})
}

// enter records a connection becoming active, suspending the idle countdown.
func (s *Server) enter() {
	s.mu.Lock()
	s.active++
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.mu.Unlock()
}

// leave records a connection finishing, restarting the countdown if it was
// the last one.
func (s *Server) leave() {
	s.mu.Lock()
	s.active--
	last := s.active == 0
	s.mu.Unlock()
	if last {
		s.armIdleTimer()
	}
}

// evictLoop runs eviction on a timer.
//
// Running it on a timer rather than at process exit is the whole point of the
// daemon: a cache that only prunes when a build ends can exceed its ceiling
// for the entire duration of a long build.
func (s *Server) evictLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.EvictInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		case <-t.C:
			res, err := s.cache.Evict(ctx)
			if err != nil {
				s.logf("evict: %v", err)
				continue
			}
			if res.ActionsPruned > 0 || res.ObjectsPruned > 0 {
				s.logf("evict: pruned %d actions, %d objects, freed %d bytes in %v",
					res.ActionsPruned, res.ObjectsPruned, res.BytesFreed, res.Elapsed)
			}
		}
	}
}

// handle serves one connection: a control exchange, then possibly a session.
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	s.enter()
	defer s.leave()

	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		s.logf("set handshake deadline: %v", err)
		return
	}

	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil {
		s.logf("read hello: %v", err)
		return
	}
	var h Hello
	if err := json.Unmarshal(line, &h); err != nil {
		s.logf("parse hello: %v", err)
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, Err: "malformed hello"})
		return
	}

	// Replace the handshake deadline with the (much longer) session one. It is
	// not cleared outright: an unbounded deadline lets a peer that completes
	// the handshake and then stalls keep this connection counted as active,
	// which stops the idle timer from ever firing.
	if err := conn.SetReadDeadline(time.Now().Add(s.sessionIdleTimeout())); err != nil {
		s.logf("set session deadline: %v", err)
		return
	}

	if h.Version != s.version {
		// Refuse rather than adapt. Two builds of this tool may disagree
		// about the on-disk index, and the client's job on seeing this is to
		// shut us down and start a matching daemon.
		_ = writeJSONLine(conn, HelloResponse{
			Version: s.version,
			Err:     fmt.Sprintf("version mismatch: daemon %s, client %s", s.version, h.Version),
		})
		return
	}

	switch h.Op {
	case OpSession:
		if err := writeJSONLine(conn, HelloResponse{Version: s.version, OK: true}); err != nil {
			return
		}
		// Refresh the read deadline on every decoded request so a silent
		// session is closed rather than pinning the daemon awake forever.
		bump := func() {
			if err := conn.SetReadDeadline(time.Now().Add(s.sessionIdleTimeout())); err != nil {
				s.logf("set session deadline: %v", err)
			}
		}
		bump()
		s.session(ctx, br, conn, bump)
	case OpStatus:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.status())
	case OpGC:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.gc(ctx, h.GC))
	case OpStats:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.stats(h.Stats))
	case OpAdopt:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.adopt(ctx, h.Adopt))
	case OpShutdown:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		s.logf("shutdown requested by client")
		s.Stop()
	default:
		_ = writeJSONLine(conn, HelloResponse{
			Version: s.version,
			Err:     fmt.Sprintf("unknown op %q, want one of: session, status, stats, gc, adopt, shutdown", h.Op),
		})
	}
}

// status assembles a StatusResponse.
func (s *Server) status() StatusResponse {
	r := StatusResponse{
		PID:      os.Getpid(),
		MaxBytes: s.cfg.MaxBytes,
		TTL:      s.cfg.TTL.String(),
		Uptime:   time.Since(s.started).Round(time.Second).String(),
		Metrics:  s.cache.Metrics(),
	}
	// The lifetime figures include what this process has counted but not yet
	// flushed, so the two sets cannot disagree about activity that just
	// happened — a lifetime total smaller than the session's would be a
	// puzzle worth nobody's time.
	if err := s.cache.FlushMetrics(); err != nil {
		s.logf("status: flush metrics: %v", err)
	}
	if total, since, err := s.idx.TotalActivity(); err != nil {
		s.logf("status: lifetime activity: %v", err)
	} else {
		r.Lifetime, r.LifetimeSince = total, since
	}
	st, err := s.idx.Stats()
	if err != nil {
		r.Err = err.Error()
		return r
	}
	r.Actions, r.Objects, r.DiskBytes = st.Actions, st.Objects, st.DiskBytes
	if oldest, newest, ok, err := s.idx.AgeSpan(); err != nil {
		s.logf("age span: %v", err)
	} else if ok {
		now := time.Now()
		r.OldestAge = now.Sub(time.Unix(0, oldest)).Round(time.Second).String()
		r.NewestAge = now.Sub(time.Unix(0, newest)).Round(time.Second).String()
	}
	return r
}

// gc forces an eviction pass, honouring any per-pass overrides the client sent.
//
// An override applies to this pass only; it does not change what the eviction
// ticker will do next. That keeps a one-off "prune harder than usual" from
// silently becoming the daemon's new policy.
// defaultStatsWindow is the history a stats request covers when it asks for no
// particular span.
const defaultStatsWindow = 24 * time.Hour

// stats reports the persisted history.
//
// It flushes first, so that a report asked for immediately after a build does
// not omit that build. The counters are otherwise written on a timer, which is
// the right trade for a path that runs beside every compile but the wrong one
// for somebody watching the number.
func (s *Server) stats(p *StatsParams) StatsResponse {
	window := defaultStatsWindow
	if p != nil && p.Since != "" {
		d, err := time.ParseDuration(p.Since)
		if err != nil {
			return StatsResponse{Err: fmt.Sprintf("stats: since %q: %v", p.Since, err)}
		}
		if d < 0 {
			return StatsResponse{Err: fmt.Sprintf("stats: since %v is negative", d)}
		}
		window = d
	}
	if err := s.cache.FlushMetrics(); err != nil {
		s.logf("stats: flush: %v", err)
	}

	total, since, err := s.idx.TotalActivity()
	if err != nil {
		return StatsResponse{Err: fmt.Sprintf("stats: %v", err)}
	}
	cutoff := time.Now().Add(-window)
	buckets, err := s.idx.ActivitySince(cutoff)
	if err != nil {
		return StatsResponse{Err: fmt.Sprintf("stats: %v", err)}
	}
	var windowed cache.MetricsSnapshot
	for _, b := range buckets {
		windowed = windowed.Add(b.Activity)
	}
	return StatsResponse{
		Lifetime:      total,
		LifetimeSince: since,
		Window:        windowed,
		WindowSince:   cutoff.UTC().Truncate(time.Hour).Unix(),
		Buckets:       buckets,
	}
}

// metricsFlushInterval is how often the daemon persists its counters.
//
// It bounds what an abrupt kill loses, which is why it is seconds rather than
// the minute it started as. A clean exit flushes on the way out, including the
// idle timeout, so the interval only matters when the process is killed — and a
// container being torn down takes the daemon with it. Measured before this was
// shortened: a daemon killed seven seconds into its first build lost every one
// of the 1277 lookups it had counted.
//
// The cost of ticking this often is close to nothing, because a tick with
// nothing to report writes nothing at all: the flush compares against what was
// last persisted and returns early on an empty delta. During a build it is one
// small write every few seconds, against the thousands the build itself makes.
const metricsFlushInterval = 5 * time.Second

// metricsLoop persists the counters periodically.
//
// Without it the record would only be written when the daemon closes, which is
// exactly the moment least likely to be reached — an idle exit runs it, a kill
// or an OOM does not.
func (s *Server) metricsLoop(ctx context.Context) {
	t := time.NewTicker(metricsFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		case <-t.C:
			if err := s.cache.FlushMetrics(); err != nil {
				s.logf("flush metrics: %v", err)
			}
		}
	}
}

// adopt imports a go-cache-plugin stage into the index this daemon holds.
//
// The connection stays counted as active for the duration, so a long import
// cannot be cut short by the idle timer, and the eviction ticker may run
// alongside it: an entry adopted and then immediately pruned is a correct
// outcome, not a conflict, and a body that is linked but not yet indexed is
// invisible to eviction because references live in the index rather than on the
// filesystem.
func (s *Server) adopt(ctx context.Context, p *AdoptParams) AdoptResponse {
	if p == nil || p.Dir == "" {
		return AdoptResponse{Err: "adopt: no stage directory given"}
	}
	if s.blobs == nil {
		// A daemon assembled without a body store cannot publish one. Saying so
		// beats a nil dereference that takes every build on the machine with it.
		return AdoptResponse{Err: "adopt: this daemon has no body store"}
	}
	s.logf("adopting go-cache-plugin stage %s (dry-run=%v)", p.Dir, p.DryRun)
	res, err := adopt.Run(ctx, adopt.Params{
		LegacyDir: p.Dir,
		Index:     s.idx,
		Blobs:     s.blobs,
		DryRun:    p.DryRun,
		Logf:      s.logf,
	})
	r := AdoptResponse{
		Records:        res.Records,
		Adopted:        res.Adopted,
		AlreadyPresent: res.AlreadyPresent,
		MissingBody:    res.MissingBody,
		SizeMismatch:   res.SizeMismatch,
		Malformed:      res.Malformed,
		Linked:         res.Linked,
		Copied:         res.Copied,
		Bytes:          res.Bytes,
		Elapsed:        res.Elapsed.Round(time.Millisecond).String(),
	}
	if err != nil {
		r.Err = err.Error()
	}
	return r
}

func (s *Server) gc(ctx context.Context, p *GCParams) GCResponse {
	maxBytes, ttl := s.cfg.MaxBytes, s.cfg.TTL
	if p != nil {
		if p.MaxBytes != nil {
			maxBytes = *p.MaxBytes
		}
		if p.TTL != nil {
			d, perr := time.ParseDuration(*p.TTL)
			if perr != nil {
				return GCResponse{Err: fmt.Sprintf("gc: ttl %q: %v", *p.TTL, perr)}
			}
			ttl = d
		}
		s.logf("gc with overrides: max_bytes=%d ttl=%v", maxBytes, ttl)
	}
	res, err := s.cache.EvictWith(ctx, maxBytes, ttl)
	r := GCResponse{
		ActionsPruned:   res.ActionsPruned,
		ObjectsPruned:   res.ObjectsPruned,
		BytesFreed:      res.BytesFreed,
		Elapsed:         res.Elapsed.Round(time.Millisecond).String(),
		AppliedMaxBytes: maxBytes,
		AppliedTTL:      ttl.String(),
	}
	if err != nil {
		r.Err = err.Error()
	}
	return r
}

// RunSession serves the GOCACHEPROG protocol directly over the given streams,
// bypassing the socket.
//
// This is the fallback path used when no daemon can be reached: the process
// that would have been a thin relay instead does the work itself.
func (s *Server) RunSession(ctx context.Context, r io.Reader, w io.Writer) {
	s.session(ctx, r, w, nil)
}

// ServeMisses speaks the protocol correctly while storing nothing.
//
// It is the floor beneath every other fallback: when the cache cannot be
// opened at all, the toolchain still gets well-formed answers and simply
// rebuilds. A build must not fail because its cache is broken.
func ServeMisses(r io.Reader, w io.Writer) error {
	dec := wire.NewDecoder(r)
	enc := wire.NewEncoder(w)
	if err := enc.Encode(&wire.Response{ID: 0, KnownCommands: []wire.Cmd{wire.CmdGet, wire.CmdPut, wire.CmdClose}}); err != nil {
		return fmt.Errorf("ServeMisses: %w", err)
	}
	for {
		req, body, err := dec.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("ServeMisses: %w", err)
		}
		// The body must be consumed even though it is discarded, or the next
		// request would be read from the middle of it.
		if body != nil {
			if _, err := io.Copy(io.Discard, body); err != nil {
				return fmt.Errorf("ServeMisses: drain body: %w", err)
			}
		}
		resp := &wire.Response{ID: req.ID}
		switch req.Command {
		case wire.CmdGet:
			resp.Miss = true
		case wire.CmdClose:
			if err := enc.Encode(resp); err != nil {
				return fmt.Errorf("ServeMisses: %w", err)
			}
			return nil
		}
		// A put reports no DiskPath: nothing was stored, and the toolchain
		// treats an absent path as "not cached" rather than as an error.
		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("ServeMisses: %w", err)
		}
	}
}

// session runs the GOCACHEPROG protocol over the connection.
//
// Puts are handled inline because the request body is framed on the same
// stream and must be consumed before the next request can be read. Gets are
// dispatched concurrently because they may fault from the shared tier, and
// the protocol explicitly permits out-of-order responses.
func (s *Server) session(ctx context.Context, r io.Reader, w io.Writer, onActivity func()) {
	dec := wire.NewDecoder(r)
	enc := wire.NewEncoder(w)

	// Per-session, so one wedged peer cannot starve the others.
	limit := s.maxGets
	if limit <= 0 {
		limit = maxConcurrentGets
	}
	getLimit := make(chan struct{}, limit)
	if onActivity == nil {
		onActivity = func() {}
	}

	if err := enc.Encode(&wire.Response{ID: 0, KnownCommands: []wire.Cmd{wire.CmdGet, wire.CmdPut, wire.CmdClose}}); err != nil {
		s.logf("session handshake: %v", err)
		return
	}

	var inflight sync.WaitGroup
	defer inflight.Wait()

	for {
		req, body, err := dec.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.logf("session decode: %v", err)
			}
			return
		}
		onActivity()

		switch req.Command {
		case wire.CmdGet:
			select {
			case getLimit <- struct{}{}:
			case <-ctx.Done():
				return
			}
			inflight.Add(1)
			go func(id int64, actionID []byte) {
				defer inflight.Done()
				defer func() { <-getLimit }()
				if err := enc.Encode(s.handleGet(ctx, id, actionID)); err != nil {
					s.logf("session encode get: %v", err)
				}
			}(req.ID, req.ActionID)

		case wire.CmdPut:
			if err := enc.Encode(s.handlePut(ctx, req, body)); err != nil {
				s.logf("session encode put: %v", err)
				return
			}

		case wire.CmdClose:
			inflight.Wait()
			if err := enc.Encode(&wire.Response{ID: req.ID}); err != nil {
				s.logf("session encode close: %v", err)
			}
			return

		default:
			if err := enc.Encode(&wire.Response{
				ID:  req.ID,
				Err: fmt.Sprintf("unknown command %q", req.Command),
			}); err != nil {
				return
			}
		}
	}
}

// handleGet resolves one get request.
func (s *Server) handleGet(ctx context.Context, id int64, actionID []byte) *wire.Response {
	a, err := ids.ActionIDFromBytes(actionID)
	if err != nil {
		return &wire.Response{ID: id, Err: err.Error()}
	}
	res, err := s.cache.Get(ctx, a)
	if err != nil {
		return &wire.Response{ID: id, Err: err.Error()}
	}
	if res.Miss {
		return &wire.Response{ID: id, Miss: true}
	}
	t := res.Time
	return &wire.Response{
		ID:       id,
		OutputID: res.OutputID[:],
		Size:     res.Size,
		DiskPath: res.DiskPath,
		Time:     &t,
	}
}

// handlePut stores one put request.
func (s *Server) handlePut(ctx context.Context, req *wire.Request, body io.Reader) *wire.Response {
	a, err := ids.ActionIDFromBytes(req.ActionID)
	if err != nil {
		return &wire.Response{ID: req.ID, Err: err.Error()}
	}
	o, err := ids.OutputIDFromBytes(req.OutputID)
	if err != nil {
		return &wire.Response{ID: req.ID, Err: err.Error()}
	}
	if body == nil {
		body = emptyReader{}
	}
	path, err := s.cache.Put(ctx, a, o, body, req.BodySize)
	if err != nil {
		return &wire.Response{ID: req.ID, Err: err.Error()}
	}
	return &wire.Response{ID: req.ID, DiskPath: path}
}

// emptyReader stands in for a put with no body line, which the toolchain
// sends for zero-length outputs.
type emptyReader struct{}

// Read always reports end of input.
func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
