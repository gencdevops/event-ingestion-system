package config

import (
	"time"

	"github.com/caarlos0/env/v9"
)

type Config struct {
	Server     ServerConfig
	Kafka      KafkaConfig
	ClickHouse ClickHouseConfig
	Worker     WorkerConfig
	LogLevel   string `env:"LOG_LEVEL" envDefault:"info"`
}

type ServerConfig struct {
	Port         int           `env:"SERVER_PORT" envDefault:"8080"`
	ReadTimeout  time.Duration `env:"SERVER_READ_TIMEOUT" envDefault:"5s"`
	WriteTimeout time.Duration `env:"SERVER_WRITE_TIMEOUT" envDefault:"10s"`
}

type KafkaConfig struct {
	Brokers           []string `env:"KAFKA_BROKERS" envDefault:"localhost:29092,localhost:29093" envSeparator:","`
	Topic             string   `env:"KAFKA_TOPIC" envDefault:"events"`
	ConsumerGroup     string   `env:"KAFKA_CONSUMER_GROUP" envDefault:"event-consumers"`
	Partitions        int      `env:"KAFKA_PARTITIONS" envDefault:"20"`
	ReplicationFactor int      `env:"KAFKA_REPLICATION_FACTOR" envDefault:"2"`
}

type TopicConfig struct {
	Name       string
	Partitions int
}

type ClickHouseConfig struct {
	Host     string `env:"CLICKHOUSE_HOST" envDefault:"localhost"`
	Port     int    `env:"CLICKHOUSE_PORT" envDefault:"9000"`
	Database string `env:"CLICKHOUSE_DATABASE" envDefault:"event_ingestion"`
	Username string `env:"CLICKHOUSE_USERNAME" envDefault:"default"`
	Password string `env:"CLICKHOUSE_PASSWORD" envDefault:"clickhouse123"`
}

type WorkerConfig struct {
	Count            int           `env:"WORKER_COUNT" envDefault:"20"`
	BatchSize        int           `env:"WORKER_BATCH_SIZE" envDefault:"2000"`
	BatchTimeout     time.Duration `env:"WORKER_BATCH_TIMEOUT" envDefault:"50ms"`
	RetryWorkerCount int           `env:"RETRY_WORKER_COUNT" envDefault:"2"`
	MaxRetryAttempts int           `env:"MAX_RETRY_ATTEMPTS" envDefault:"5"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
