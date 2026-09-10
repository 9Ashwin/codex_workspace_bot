package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
	"github.com/kid0317/codex-workspace-bot/internal/fleetqhermes"
)

func main() {
	set := flag.NewFlagSet("fleetq-hermes", flag.ExitOnError)
	machine := set.String("machine", getenv("FLEETQ_MACHINE", "linux"), "FleetQ machine name")
	natsURL := set.String("nats-url", getenv("FLEETQ_NATS_URL", ""), "NATS URL")
	handler := set.String("handler", os.Getenv("FLEETQ_HERMES_HANDLER"), "Hermes command; message JSON path is appended")
	ackWait := set.Duration("ack-wait", 2*time.Hour, "NATS AckWait")
	fetchWait := set.Duration("fetch-wait", time.Second, "NATS fetch wait")
	if err := set.Parse(os.Args[1:]); err != nil {
		fatal("parse flags: %v", err)
	}
	if strings.TrimSpace(*natsURL) == "" {
		fatal("-nats-url or FLEETQ_NATS_URL is required")
	}
	if strings.TrimSpace(*handler) == "" {
		fatal("-handler or FLEETQ_HERMES_HANDLER is required")
	}
	client, err := fleetq.New(fleetq.Config{
		Machine: *machine, NATSURL: *natsURL, TokenEnv: "FLEETQ_NATS_TOKEN", CredsEnv: "FLEETQ_NATS_CREDS",
		AckWait: *ackWait, FetchWait: *fetchWait, Machines: []string{"linux", "mac-mini", "mac-air"},
	})
	if err != nil {
		fatal("connect FleetQ: %v", err)
	}
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	service := fleetqhermes.Service{Client: client, Machine: *machine, Handler: *handler}
	if err := service.Run(ctx); err != nil {
		fatal("fleetq-hermes: %v", err)
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
