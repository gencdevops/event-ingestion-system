//go:build integration

package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func setupClickHouseContainer(t *testing.T) (*Client, func()) {
	ctx := context.Background()

	container, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3",
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("clickhouse123"),
		clickhouse.WithDatabase("test_db"),
	)
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err)

	client, err := NewClient(config.ClickHouseConfig{
		Host:     host,
		Port:     port.Int(),
		Database: "test_db",
		Username: "default",
		Password: "clickhouse123",
	})
	require.NoError(t, err)

	err = client.InitSchema(ctx)
	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		testcontainers.CleanupContainer(t, container)
	}

	return client, cleanup
}

func TestIntegration_InsertBatch_SingleEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	events := []*event.Event{
		{
			EventID:   "test-event-1",
			EventName: "user_signup",
			UserID:    "user-123",
			Channel:   "web",
			Timestamp: time.Now().Unix(),
			Tags:      []string{"tag1", "tag2"},
			Metadata:  map[string]interface{}{"source": "test"},
		},
	}

	err := repo.InsertBatch(ctx, events)
	require.NoError(t, err)
}

func TestIntegration_InsertBatch_MultipleEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	events := make([]*event.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = &event.Event{
			EventID:   "test-event-" + string(rune('a'+i%26)),
			EventName: "user_action",
			UserID:    "user-" + string(rune('0'+i%10)),
			Channel:   []string{"web", "mobile_app", "api"}[i%3],
			Timestamp: time.Now().Add(-time.Duration(i) * time.Minute).Unix(),
			Tags:      []string{"test"},
		}
	}

	err := repo.InsertBatch(ctx, events)
	require.NoError(t, err)
}

func TestIntegration_GetMetrics_BasicQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	// Insert some test events
	now := time.Now()
	events := []*event.Event{
		{
			EventID:   "metric-test-1",
			EventName: "page_view",
			UserID:    "user-1",
			Channel:   "web",
			Timestamp: now.Unix(),
		},
		{
			EventID:   "metric-test-2",
			EventName: "page_view",
			UserID:    "user-2",
			Channel:   "web",
			Timestamp: now.Unix(),
		},
		{
			EventID:   "metric-test-3",
			EventName: "page_view",
			UserID:    "user-1",
			Channel:   "mobile_app",
			Timestamp: now.Unix(),
		},
	}

	err := repo.InsertBatch(ctx, events)
	require.NoError(t, err)

	// Wait for data to be available in the materialized view
	time.Sleep(2 * time.Second)

	// Query metrics
	query := &event.GetMetricsQuery{
		EventName: "page_view",
		From:      now.Add(-1 * time.Hour).Unix(),
		To:        now.Add(1 * time.Hour).Unix(),
	}

	result, err := repo.GetMetrics(ctx, query)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.GreaterOrEqual(t, result.TotalCount, int64(3))
	assert.GreaterOrEqual(t, result.UniqueUsers, int64(2))
}

func TestIntegration_GetMetrics_WithChannelFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	now := time.Now()
	events := []*event.Event{
		{
			EventID:   "channel-test-1",
			EventName: "button_click",
			UserID:    "user-1",
			Channel:   "web",
			Timestamp: now.Unix(),
		},
		{
			EventID:   "channel-test-2",
			EventName: "button_click",
			UserID:    "user-2",
			Channel:   "mobile_app",
			Timestamp: now.Unix(),
		},
	}

	err := repo.InsertBatch(ctx, events)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	query := &event.GetMetricsQuery{
		EventName: "button_click",
		From:      now.Add(-1 * time.Hour).Unix(),
		To:        now.Add(1 * time.Hour).Unix(),
		Channel:   "web",
	}

	result, err := repo.GetMetrics(ctx, query)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.GreaterOrEqual(t, result.TotalCount, int64(1))
}

func TestIntegration_GetMetrics_GroupByChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	now := time.Now()
	events := []*event.Event{
		{
			EventID:   "group-test-1",
			EventName: "purchase",
			UserID:    "user-1",
			Channel:   "web",
			Timestamp: now.Unix(),
		},
		{
			EventID:   "group-test-2",
			EventName: "purchase",
			UserID:    "user-2",
			Channel:   "web",
			Timestamp: now.Unix(),
		},
		{
			EventID:   "group-test-3",
			EventName: "purchase",
			UserID:    "user-3",
			Channel:   "mobile_app",
			Timestamp: now.Unix(),
		},
	}

	err := repo.InsertBatch(ctx, events)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	query := &event.GetMetricsQuery{
		EventName: "purchase",
		From:      now.Add(-1 * time.Hour).Unix(),
		To:        now.Add(1 * time.Hour).Unix(),
		GroupBy:   "channel",
	}

	result, err := repo.GetMetrics(ctx, query)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.NotEmpty(t, result.GroupedData)
}

func TestIntegration_InsertBatch_EmptyEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, cleanup := setupClickHouseContainer(t)
	defer cleanup()

	repo := NewEventRepository(client)
	ctx := context.Background()

	err := repo.InsertBatch(ctx, []*event.Event{})
	require.NoError(t, err)
}
