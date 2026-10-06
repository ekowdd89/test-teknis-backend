package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ekowdd89/test-teknis-backend/internal/cmd"
)

// Diisi saat build: -ldflags "-X main.version=..." (lihat Dockerfile).
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := cmd.NewPublisher(ctx, cmd.WithVersion(version))
	if err != nil {
		slog.Error("failed to initialize publisher", "error", err)
		os.Exit(1)
	}
	if err := c.Run(ctx); err != nil {
		c.Logger().Error("publisher stopped with error", "error", err)
		os.Exit(1)
	}
	c.Logger().Info("publisher stopped")
}
