package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/sink"
	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/telemetry"
)

const readHeaderTimeout = 5 * time.Second

func run(ctx context.Context) error {
	server := http.Server{Addr: ":8080", Handler: sink.NewHandler(), ReadHeaderTimeout: readHeaderTimeout}
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), readHeaderTimeout)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := telemetry.Run(ctx, "sink", run); err != nil {
		log.Fatal(err)
	}
}
