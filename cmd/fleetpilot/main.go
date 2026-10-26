// Command fleetpilot serves the FleetPilot API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/api"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/store/postgres"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := postgres.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	// The fake provider is the only provider. It keeps actual state in this
	// process. Desired state, jobs, health checks, rollouts, and the audit log
	// stay in PostgreSQL.
	provider := fake.New()
	cfg := work.DefaultConfig()
	server := api.New(store, provider, store, store, provider, store, cfg)
	workerDone := make(chan struct{})
	rollerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		server.RunWorker(ctx)
	}()
	go func() {
		defer close(rollerDone)
		server.RunRoller(ctx)
	}()
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "port", port, "provider", "fake", "max_in_flight", cfg.MaxInFlight)
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		stop()
		<-workerDone
		<-rollerDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := httpServer.Shutdown(shutdownCtx)
		<-workerDone
		<-rollerDone
		if err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}
