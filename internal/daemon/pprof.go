// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Pprof listener timeouts bound setup and shutdown without limiting a profile
// capture whose duration is selected by the caller.
const (
	pprofReadHeaderTimeout = 30 * time.Second
	pprofIdleTimeout       = 10 * time.Minute
	pprofShutdownGrace     = 30 * time.Second
)

// ListenPprof binds the loopback address used for Go runtime profiles.
//
// Pprof endpoints have no authentication and can expose process runtime data,
// so accepting a hostname or an unspecified address here would make a
// configuration typo into an externally reachable diagnostics service.
func ListenPprof(addr string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("ListenPprof: split address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("ListenPprof: address %q is not a loopback IP address", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ListenPprof: %w", err)
	}
	return ln, nil
}

// ServePprof serves Go runtime profiles on ln until the daemon is asked to stop
// or the context is cancelled.
//
// The profiling listener is separate from the Bazel HTTP listener: profiles
// expose process runtime data, while a cache address must continue to serve only
// cache routes. Like the other optional TCP listeners, it keeps the daemon
// alive for as long as it is configured to serve.
func (s *Server) ServePprof(ctx context.Context, ln net.Listener) error {
	s.enter()
	defer s.leave()

	srv := &http.Server{
		Handler:           newPprofHandler(),
		ReadHeaderTimeout: pprofReadHeaderTimeout,
		IdleTimeout:       pprofIdleTimeout,
		// Request contexts derive from ctx so stopping the daemon also stops an
		// in-flight profile capture.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopped:
		case <-done:
			return
		}
		// A profile capture is allowed to finish, but one that cannot do so must
		// not keep the cache process alive indefinitely.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pprofShutdownGrace)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("ServePprof: %w", err)
	}
	return nil
}

// newPprofHandler creates the fixed pprof route set without registering it on
// the process-wide default mux, which could otherwise leak those routes onto an
// unrelated HTTP server in the same process.
func newPprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
