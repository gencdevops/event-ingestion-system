package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/event-ingestion/infrastructure/config"
)

type Client struct {
	conn     driver.Conn
	database string
}

func NewClient(cfg config.ClickHouseConfig) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:          5 * time.Second,
		MaxOpenConns:         50,
		MaxIdleConns:         20,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      100,
		MaxCompressionBuffer: 1048576,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		slog.Error("Failed to connect to ClickHouse", "addr", addr, "error", err)
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		slog.Error("Failed to ping ClickHouse", "addr", addr, "error", err)
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	slog.Info("Connected to ClickHouse", "addr", addr, "database", cfg.Database)
	return &Client{conn: conn, database: cfg.Database}, nil
}

func (c *Client) Conn() driver.Conn {
	return c.conn
}

func (c *Client) Database() string {
	return c.database
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) InitSchema(ctx context.Context) error {
	queries := []string{
		fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, c.database),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.events (
			event_id String,
			event_name String,
			channel String,
			campaign_id String,
			user_id String,
			timestamp DateTime,
			tags Array(String),
			metadata String,
			created_at DateTime DEFAULT now(),
			INDEX idx_event_name event_name TYPE bloom_filter GRANULARITY 4,
			INDEX idx_channel channel TYPE bloom_filter GRANULARITY 4,
			INDEX idx_user_id user_id TYPE bloom_filter GRANULARITY 4
		) ENGINE = ReplacingMergeTree(created_at)
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (event_id, event_name, timestamp)`, c.database),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.events_hourly (
			event_name String,
			channel String,
			hour DateTime,
			event_count SimpleAggregateFunction(sum, UInt64),
			unique_users_state AggregateFunction(uniq, String)
		) ENGINE = AggregatingMergeTree()
		PARTITION BY toYYYYMMDD(hour)
		ORDER BY (event_name, channel, hour)`, c.database),

		fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %s.events_hourly_mv
		TO %s.events_hourly AS
		SELECT
			event_name,
			channel,
			toStartOfHour(timestamp) as hour,
			count() as event_count,
			uniqState(user_id) as unique_users_state
		FROM %s.events
		GROUP BY event_name, channel, hour`, c.database, c.database, c.database),
	}

	for i, query := range queries {
		if err := c.conn.Exec(ctx, query); err != nil {
			slog.Error("Failed to execute schema query", "queryIndex", i, "error", err)
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}

	slog.Info("ClickHouse schema initialized", "database", c.database, "tables", []string{"events", "events_hourly", "events_hourly_mv"})
	return nil
}
