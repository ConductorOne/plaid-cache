// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/conductorone/plaid-cache/internal/reapi"
)

// bazelGRPCShutdownGrace is how long in-flight transfers are given to finish
// once the daemon has been asked to stop.
//
// It bounds the wait, not the transfer. A stopping daemon that waited for a
// three-hundred-megabyte upload over a slow link would look hung; one that cut
// every transfer off instantly would waste work that was nearly done.
const bazelGRPCShutdownGrace = 30 * time.Second

// ServeBazelGRPC serves the cache half of the Remote Execution API on ln until
// the daemon is asked to stop or the context is cancelled.
//
// Like the HTTP listener, it counts as one connection for the whole of its
// life, which suppresses the idle exit: a GOCACHEPROG client that finds no
// daemon spawns one, and Bazel cannot, so a daemon told to serve Bazel is a
// daemon that has been asked to stay.
//
// Both listeners may run at once, over the same store. They address the same
// two keyspaces with the same digests, so a build that uploads over one reads
// its own outputs back over the other.
func (s *Server) ServeBazelGRPC(ctx context.Context, ln net.Listener) error {
	bstore, err := s.bazelAdapter()
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("ServeBazelGRPC: %w", err)
	}

	s.enter()
	defer s.leave()

	svc := reapi.New(reapi.Params{
		Store:  bstore,
		Logf:   s.logf,
		Verify: !s.cfg.DisableBazelVerify,
	})
	s.setREAPI(svc)
	defer s.setREAPI(nil)
	g := grpc.NewServer(reapi.ServerOptions()...)
	svc.Register(g)

	done := make(chan struct{})
	defer close(done)

	// A write that breaks leaves a staged body open for its client to resume
	// into. Sweeping is what stops a client that never comes back from holding
	// one for the life of the daemon.
	go func() {
		t := time.NewTicker(reapi.UploadSweepInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				svc.SweepUploads()
			}
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopped:
		case <-done:
			return
		}
		stopped := make(chan struct{})
		go func() {
			g.GracefulStop()
			close(stopped)
		}()
		grace := time.NewTimer(bazelGRPCShutdownGrace)
		defer grace.Stop()
		select {
		case <-stopped:
		case <-grace.C:
			g.Stop()
		}
	}()

	err = g.Serve(ln)
	// Every partially received upload is a staged file this process is
	// responsible for, and nothing is going to resume one now.
	svc.DiscardUploads()
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("ServeBazelGRPC: %w", err)
	}
	return nil
}
