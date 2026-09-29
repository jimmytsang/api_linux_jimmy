// Command worker-server serves the JobWorker gRPC API on loopback over mTLS.
// Run it from the repo root so it finds the dev certificates in certs/.
//
// TODO: out of scope - configuration. The address and certificate paths are
// hardcoded.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jimmytsang/api_linux_jimmy/internal/server"
	"github.com/jimmytsang/api_linux_jimmy/pkg/job"
)

const (
	// Loopback only: the dev server certificate is valid for localhost and
	// 127.0.0.1 alone, so the CLI runs on the same host.
	addr     = "127.0.0.1:50051"
	certFile = "certs/server.crt"
	keyFile  = "certs/server.key"
	caFile   = "certs/ca.crt"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("worker-server", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	tlsConfig, err := server.ServerTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := server.New(tlsConfig, job.NewManager(), logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	logger.Info("listening", "addr", lis.Addr().String())

	select {
	case <-ctx.Done():
		// Stop catching signals, so a second Ctrl-C quits right away instead
		// of waiting out the shutdown.
		stop()
		logger.Info("shutting down")
		srv.Shutdown()
		return <-serveErr
	case err := <-serveErr:
		// Serve failed on its own. Still kill the jobs so none outlive us.
		srv.Shutdown()
		return err
	}
}
