package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	internalconfig "github.com/parsnips/dynamodb-local-faster/internal/config"
	"github.com/parsnips/dynamodb-local-faster/pkg/dynolocalfaster"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := internalconfig.ParseFlags(os.Args[1:])
	if err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	server, err := dynolocalfaster.New(ctx, cfg)
	if err != nil {
		log.Fatalf("initialize server: %v", err)
	}

	if err := server.Start(ctx); err != nil {
		log.Fatalf("start server: %v", err)
	}

	log.Printf(
		"dynamodb-local-faster listening at %s (mode=%s instances=%d)",
		server.Endpoint(),
		cfg.Mode,
		cfg.Instances,
	)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Close(shutdownCtx); err != nil {
		log.Fatalf("shutdown failed: %v", err)
	}
}
