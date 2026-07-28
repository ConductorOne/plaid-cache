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
	logf    cache.Logf
	version string
	started time.Time

	// getLimit bounds concurrent Get handling. A get can fault from the
	// shared tier, so without a bound a single pipelined build could open one
	// connection per in-flight action.
	getLimit chan struct{}

	mu        sync.Mutex
	active    int
	idleTimer *time.Timer

	conns    sync.WaitGroup
	stopped  chan struct{}
	stopOnce sync.Once
}

// ServerParams carries the daemon's dependencies.
type ServerParams struct {
	Config  *config.Config
	Cache   *cache.Cache
	Index   *index.Index
	Logf    cache.Logf
	Version string
}

// maxConcurrentGets bounds in-flight remote faults per daemon.
const maxConcurrentGets = 64

// NewServer constructs a Server. Call Serve to run it.
func NewServer(p ServerParams) *Server {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Server{
		cfg:      p.Config,
		cache:    p.Cache,
		idx:      p.Index,
		logf:     logf,
		version:  p.Version,
		started:  time.Now(),
		getLimit: make(chan struct{}, maxConcurrentGets),
		stopped:  make(chan struct{}),
	}
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
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("Listen: %w", err)
	}
	// The cache holds build outputs; only its owner should be able to read or
	// poison them.
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

	if !s.cfg.DisableEviction && s.cfg.EvictInterval > 0 {
		go s.evictLoop(ctx)
	}
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
			return fmt.Errorf("Serve: accept: %w", err)
		}
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

	// Clear the deadline: a session lives as long as the build does.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		s.logf("clear deadline: %v", err)
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
		s.session(ctx, br, conn)
	case OpStatus:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.status())
	case OpGC:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		_ = writeJSONLine(conn, s.gc(ctx))
	case OpShutdown:
		_ = writeJSONLine(conn, HelloResponse{Version: s.version, OK: true})
		s.logf("shutdown requested by client")
		s.Stop()
	default:
		_ = writeJSONLine(conn, HelloResponse{
			Version: s.version,
			Err:     fmt.Sprintf("unknown op %q, want one of: session, status, gc, shutdown", h.Op),
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
	st, err := s.idx.Stats()
	if err != nil {
		r.Err = err.Error()
		return r
	}
	r.Actions, r.Objects, r.DiskBytes = st.Actions, st.Objects, st.DiskBytes
	return r
}

// gc forces an eviction pass.
func (s *Server) gc(ctx context.Context) GCResponse {
	res, err := s.cache.Evict(ctx)
	r := GCResponse{
		ActionsPruned: res.ActionsPruned,
		ObjectsPruned: res.ObjectsPruned,
		BytesFreed:    res.BytesFreed,
		Elapsed:       res.Elapsed.Round(time.Millisecond).String(),
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
	s.session(ctx, r, w)
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
func (s *Server) session(ctx context.Context, r io.Reader, w io.Writer) {
	dec := wire.NewDecoder(r)
	enc := wire.NewEncoder(w)

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

		switch req.Command {
		case wire.CmdGet:
			select {
			case s.getLimit <- struct{}{}:
			case <-ctx.Done():
				return
			}
			inflight.Add(1)
			go func(id int64, actionID []byte) {
				defer inflight.Done()
				defer func() { <-s.getLimit }()
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
