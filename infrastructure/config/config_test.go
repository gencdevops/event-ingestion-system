package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clearEnvVars() {
	envVars := []string{
		"SERVER_PORT", "SERVER_READ_TIMEOUT", "SERVER_WRITE_TIMEOUT",
		"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_CONSUMER_GROUP",
		"KAFKA_PARTITIONS", "KAFKA_REPLICATION_FACTOR",
		"CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DATABASE",
		"CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD",
		"WORKER_COUNT", "WORKER_BATCH_SIZE", "WORKER_BATCH_TIMEOUT",
		"RETRY_WORKER_COUNT", "MAX_RETRY_ATTEMPTS",
		"LOG_LEVEL",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}
}

func TestLoad_DefaultValues(t *testing.T) {
	clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Server defaults
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 5*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, 10*time.Second, cfg.Server.WriteTimeout)

	// Kafka defaults
	assert.Equal(t, []string{"localhost:29092", "localhost:29093"}, cfg.Kafka.Brokers)
	assert.Equal(t, "events", cfg.Kafka.Topic)
	assert.Equal(t, "event-consumers", cfg.Kafka.ConsumerGroup)
	assert.Equal(t, 20, cfg.Kafka.Partitions)
	assert.Equal(t, 2, cfg.Kafka.ReplicationFactor)

	// ClickHouse defaults
	assert.Equal(t, "localhost", cfg.ClickHouse.Host)
	assert.Equal(t, 9000, cfg.ClickHouse.Port)
	assert.Equal(t, "event_ingestion", cfg.ClickHouse.Database)
	assert.Equal(t, "default", cfg.ClickHouse.Username)
	assert.Equal(t, "clickhouse123", cfg.ClickHouse.Password)

	// Worker defaults
	assert.Equal(t, 20, cfg.Worker.Count)
	assert.Equal(t, 2000, cfg.Worker.BatchSize)
	assert.Equal(t, 50*time.Millisecond, cfg.Worker.BatchTimeout)
	assert.Equal(t, 2, cfg.Worker.RetryWorkerCount)
	assert.Equal(t, 5, cfg.Worker.MaxRetryAttempts)

	// Log level default
	assert.Equal(t, "info", cfg.LogLevel)
}

func TestLoad_CustomServerConfig(t *testing.T) {
	clearEnvVars()
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("SERVER_READ_TIMEOUT", "10s")
	os.Setenv("SERVER_WRITE_TIMEOUT", "20s")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, 10*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, 20*time.Second, cfg.Server.WriteTimeout)
}

func TestLoad_CustomKafkaConfig(t *testing.T) {
	clearEnvVars()
	os.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092")
	os.Setenv("KAFKA_TOPIC", "custom-events")
	os.Setenv("KAFKA_CONSUMER_GROUP", "custom-consumers")
	os.Setenv("KAFKA_PARTITIONS", "30")
	os.Setenv("KAFKA_REPLICATION_FACTOR", "3")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "custom-events", cfg.Kafka.Topic)
	assert.Equal(t, "custom-consumers", cfg.Kafka.ConsumerGroup)
	assert.Equal(t, 30, cfg.Kafka.Partitions)
	assert.Equal(t, 3, cfg.Kafka.ReplicationFactor)
}

func TestLoad_CustomClickHouseConfig(t *testing.T) {
	clearEnvVars()
	os.Setenv("CLICKHOUSE_HOST", "clickhouse.example.com")
	os.Setenv("CLICKHOUSE_PORT", "9001")
	os.Setenv("CLICKHOUSE_DATABASE", "custom_db")
	os.Setenv("CLICKHOUSE_USERNAME", "admin")
	os.Setenv("CLICKHOUSE_PASSWORD", "secret123")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "clickhouse.example.com", cfg.ClickHouse.Host)
	assert.Equal(t, 9001, cfg.ClickHouse.Port)
	assert.Equal(t, "custom_db", cfg.ClickHouse.Database)
	assert.Equal(t, "admin", cfg.ClickHouse.Username)
	assert.Equal(t, "secret123", cfg.ClickHouse.Password)
}

func TestLoad_CustomWorkerConfig(t *testing.T) {
	clearEnvVars()
	os.Setenv("WORKER_COUNT", "50")
	os.Setenv("WORKER_BATCH_SIZE", "5000")
	os.Setenv("WORKER_BATCH_TIMEOUT", "100ms")
	os.Setenv("RETRY_WORKER_COUNT", "5")
	os.Setenv("MAX_RETRY_ATTEMPTS", "10")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 50, cfg.Worker.Count)
	assert.Equal(t, 5000, cfg.Worker.BatchSize)
	assert.Equal(t, 100*time.Millisecond, cfg.Worker.BatchTimeout)
	assert.Equal(t, 5, cfg.Worker.RetryWorkerCount)
	assert.Equal(t, 10, cfg.Worker.MaxRetryAttempts)
}

func TestLoad_CustomLogLevel(t *testing.T) {
	clearEnvVars()
	os.Setenv("LOG_LEVEL", "debug")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestLoad_SingleKafkaBroker(t *testing.T) {
	clearEnvVars()
	os.Setenv("KAFKA_BROKERS", "localhost:9092")
	defer clearEnvVars()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
}
