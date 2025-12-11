package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/event-ingestion/application"
	_ "github.com/event-ingestion/docs"
	"github.com/event-ingestion/infrastructure/config"
	"github.com/event-ingestion/infrastructure/messaging/kafka"
	worker2 "github.com/event-ingestion/infrastructure/messaging/worker"
	"github.com/event-ingestion/infrastructure/persistence/clickhouse"
	"github.com/event-ingestion/presentation/api/controller"
	"github.com/event-ingestion/presentation/api/middleware"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/swagger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting Event Ingestion Service", "port", cfg.Server.Port)

	slog.Info("Ensuring Kafka topics exist...")
	topics := []kafka.TopicConfig{
		{Name: cfg.Kafka.Topic, Partitions: cfg.Kafka.Partitions},
		{Name: cfg.Kafka.Topic + ".retry", Partitions: 10},
		{Name: cfg.Kafka.Topic + ".dlq", Partitions: 5},
	}
	if err := kafka.EnsureTopicsWithConfig(cfg.Kafka, topics); err != nil {
		slog.Warn("Failed to ensure Kafka topics", "error", err)
	}

	slog.Info("Connecting to ClickHouse...")
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		slog.Error("Failed to connect to ClickHouse", "error", err)
		os.Exit(1)
	}
	defer chClient.Close()

	slog.Info("Initializing ClickHouse schema...")
	if err := chClient.InitSchema(context.Background()); err != nil {
		slog.Error("Failed to initialize schema", "error", err)
		os.Exit(1)
	}

	slog.Info("Creating Kafka producer...")
	producer := kafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	repository := clickhouse.NewEventRepository(chClient)

	eventService := application.NewEventService(producer)
	metricsService := application.NewMetricsService(repository)

	app := fiber.New(fiber.Config{
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		AppName:      "Event Ingestion API",
	})

	app.Use(cors.New())
	app.Use(middleware.RequestID())
	app.Use(middleware.RequestLogger())
	app.Use(middleware.Recovery())

	app.Get("/swagger/*", swagger.HandlerDefault)

	controller.NewEventController(app, eventService)
	controller.NewMetricsController(app, metricsService)
	controller.NewHealthController(app)

	ctx, cancel := context.WithCancel(context.Background())

	eventWorker := worker2.NewEventWorker(cfg.Kafka, repository, cfg.Worker)
	eventWorker.Start(ctx)

	retryWorker := worker2.NewRetryWorker(cfg.Kafka, repository, cfg.Worker)
	retryWorker.Start(ctx)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.Port)
		slog.Info("HTTP server listening", "addr", addr)
		slog.Info("Swagger UI available", "url", fmt.Sprintf("http://localhost:%d/swagger/index.html", cfg.Server.Port))
		if err := app.Listen(addr); err != nil {
			slog.Error("Server error", "error", err)
		}
	}()

	sig := <-shutdown
	slog.Info("Received signal, starting graceful shutdown...", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	cancel()

	eventWorker.Stop()
	retryWorker.Stop()

	slog.Info("Shutdown complete")
}
