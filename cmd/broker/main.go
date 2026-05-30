package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/makhskham/burrow/internal/broker/api"
	"github.com/makhskham/burrow/internal/config"
	"github.com/makhskham/burrow/internal/metrics"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	srv := api.New(cfg.Broker.ID, cfg.Storage.DataDir)

	// periodic ISR lag check
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			srv.ISRTick()
		}
	}()

	go metrics.ServeHTTP(cfg.Metrics.Addr)

	go func() {
		if err := srv.Listen(cfg.GRPC.Addr); err != nil {
			log.Fatal().Err(err).Msg("broker exited")
		}
	}()

	log.Info().
		Str("id", cfg.Broker.ID).
		Str("grpc", cfg.GRPC.Addr).
		Str("metrics", cfg.Metrics.Addr).
		Msg("Burrow broker ready")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down")
}
