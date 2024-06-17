package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi"
	"github.com/richardbizik/tibor-rest/internal/config"
	"github.com/richardbizik/tibor-rest/internal/handlers"
	"github.com/richardbizik/tibor-rest/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

var srv *http.Server

func main() {
	config.InitConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))
	defaultLogger := slog.Default()
	*defaultLogger = *logger

	kClient, err := kafka.New(kafka.GetDefaultConfig(config.Conf.Kafka))
	if err != nil {
		slog.Error(fmt.Sprintf("unable to create schema registry client: %w", err))
		os.Exit(1)
	}

	server := setupServer(kClient)
	srv = &http.Server{
		ReadHeaderTimeout: time.Second * 5,
		Addr:              fmt.Sprintf(":8080"),
		Handler:           server,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error(err.Error())
		}
	}()
	appStop := make(chan os.Signal, 2)
	slog.Info("Started server")
	handleSigterm(appStop)
}

func setupServer(kafkaClient *kgo.Client) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/", handlers.ProduceKafkaEventHandler(kafkaClient))
	return r
}

func handleSigterm(appStop chan os.Signal) {
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-appStop
	slog.Info("Received sigterm shutting down")
	cleanup()
}

func cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("http server forced to shutdown: %v", err)
	}
}
