//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	kafkapkg "github.com/event-ingestion/infrastructure/messaging/kafka"
	clickhousepkg "github.com/event-ingestion/infrastructure/persistence/clickhouse"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clickhousemodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
)

type testInfrastructure struct {
	kafkaBroker       string
	clickhouseHost    string
	clickhousePort    int
	kafkaCleanup      func()
	clickhouseCleanup func()
	clickhouseClient  *clickhousepkg.Client
}

func setupTestInfrastructure(t *testing.T) *testInfrastructure {
	ctx := context.Background()
	infra := &testInfrastructure{}

	// Setup Kafka
	kafkaContainer, err := kafkamodule.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafkamodule.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	infra.kafkaBroker = brokers[0]
	infra.kafkaCleanup = func() {
		testcontainers.CleanupContainer(t, kafkaContainer)
	}

	// Setup ClickHouse
	clickhouseContainer, err := clickhousemodule.Run(ctx,
		"clickhouse/clickhouse-server:24.3",
		clickhousemodule.WithUsername("default"),
		clickhousemodule.WithPassword("clickhouse123"),
		clickhousemodule.WithDatabase("test_db"),
	)
	require.NoError(t, err)

	host, err := clickhouseContainer.Host(ctx)
	require.NoError(t, err)

	port, err := clickhouseContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	infra.clickhouseHost = host
	infra.clickhousePort = port.Int()
	infra.clickhouseCleanup = func() {
		testcontainers.CleanupContainer(t, clickhouseContainer)
	}

	// Create ClickHouse client
	client, err := clickhousepkg.NewClient(config.ClickHouseConfig{
		Host:     host,
		Port:     port.Int(),
		Database: "test_db",
		Username: "default",
		Password: "clickhouse123",
	})
	require.NoError(t, err)

	err = client.InitSchema(ctx)
	require.NoError(t, err)
	infra.clickhouseClient = client

	return infra
}

func (i *testInfrastructure) cleanup() {
	if i.clickhouseClient != nil {
		i.clickhouseClient.Close()
	}
	if i.kafkaCleanup != nil {
		i.kafkaCleanup()
	}
	if i.clickhouseCleanup != nil {
		i.clickhouseCleanup()
	}
}

func createTopic(t *testing.T, broker, topic string, numPartitions int) {
	conn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	// Wait for topic to be ready
	time.Sleep(500 * time.Millisecond)
}

func TestIntegration_EndToEnd_ProduceConsumeStore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	infra := setupTestInfrastructure(t)
	defer infra.cleanup()

	ctx := context.Background()
	topic := "integration-events"

	// Create topic first
	createTopic(t, infra.kafkaBroker, topic, 1)

	// Create producer
	producer := kafkapkg.NewProducer(config.KafkaConfig{
		Brokers: []string{infra.kafkaBroker},
		Topic:   topic,
	})
	defer producer.Close()

	// Create test events
	events := []*event.Event{
		{
			EventID:   "e2e-test-1",
			EventName: "user_signup",
			UserID:    "user-e2e-1",
			Channel:   "web",
			Timestamp: time.Now().Unix(),
		},
		{
			EventID:   "e2e-test-2",
			EventName: "user_signup",
			UserID:    "user-e2e-2",
			Channel:   "mobile_app",
			Timestamp: time.Now().Unix(),
		},
	}

	// Publish events
	err := producer.PublishBatch(ctx, events)
	require.NoError(t, err)

	// Wait for async write
	time.Sleep(1 * time.Second)

	// Create consumer to read and verify
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{infra.kafkaBroker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	reader.SetOffset(kafka.FirstOffset)

	// Read messages
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var receivedEvents []*event.Event
	for i := 0; i < len(events); i++ {
		msg, err := reader.ReadMessage(readCtx)
		require.NoError(t, err)

		var e event.Event
		err = json.Unmarshal(msg.Value, &e)
		require.NoError(t, err)
		receivedEvents = append(receivedEvents, &e)
	}

	require.Len(t, receivedEvents, 2)

	// Store events in ClickHouse
	repo := clickhousepkg.NewEventRepository(infra.clickhouseClient)
	err = repo.InsertBatch(ctx, receivedEvents)
	require.NoError(t, err)
}

func TestIntegration_RetryTopic_MessageFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	infra := setupTestInfrastructure(t)
	defer infra.cleanup()

	ctx := context.Background()
	mainTopic := "retry-test-events"
	retryTopic := mainTopic + ".retry"

	// Create topic first
	createTopic(t, infra.kafkaBroker, retryTopic, 1)

	// Create writer for retry topic
	retryWriter := &kafka.Writer{
		Addr:         kafka.TCP(infra.kafkaBroker),
		Topic:        retryTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer retryWriter.Close()

	// Write a message to retry topic with headers
	msg := kafka.Message{
		Key:   []byte("user-retry-1"),
		Value: []byte(`{"event_id":"retry-1","event_name":"test","user_id":"user-1","timestamp":1699999999}`),
		Headers: []kafka.Header{
			{Key: "retry_count", Value: []byte("2")},
			{Key: "error_type", Value: []byte("insert_failed")},
			{Key: "error_message", Value: []byte("connection error")},
		},
	}

	err := retryWriter.WriteMessages(ctx, msg)
	require.NoError(t, err)

	// Wait for write
	time.Sleep(500 * time.Millisecond)

	// Read from retry topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{infra.kafkaBroker},
		Topic:     retryTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	reader.SetOffset(kafka.FirstOffset)
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	receivedMsg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	// Verify headers
	var hasRetryCount bool
	for _, h := range receivedMsg.Headers {
		if h.Key == "retry_count" {
			hasRetryCount = true
			require.Equal(t, "2", string(h.Value))
		}
	}
	require.True(t, hasRetryCount, "retry_count header should be present")
}

func TestIntegration_DLQ_MessageFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	infra := setupTestInfrastructure(t)
	defer infra.cleanup()

	ctx := context.Background()
	mainTopic := "dlq-test-events"
	dlqTopic := mainTopic + ".dlq"

	// Create topic first
	createTopic(t, infra.kafkaBroker, dlqTopic, 1)

	// Create writer for DLQ
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(infra.kafkaBroker),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer dlqWriter.Close()

	// Write a message to DLQ with exhausted retries
	msg := kafka.Message{
		Key:   []byte("user-dlq-1"),
		Value: []byte(`{"event_id":"dlq-1","event_name":"failed_event","user_id":"user-1"}`),
		Headers: []kafka.Header{
			{Key: "final_retry_count", Value: []byte("5")},
			{Key: "error_type", Value: []byte("max_retries_exhausted")},
			{Key: "error_message", Value: []byte("failed after 5 attempts")},
			{Key: "sent_to_dlq_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	err := dlqWriter.WriteMessages(ctx, msg)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Read from DLQ
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{infra.kafkaBroker},
		Topic:     dlqTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	reader.SetOffset(kafka.FirstOffset)
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	receivedMsg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	// Verify DLQ headers
	var hasFinalRetryCount, hasSentToDLQAt bool
	for _, h := range receivedMsg.Headers {
		switch h.Key {
		case "final_retry_count":
			hasFinalRetryCount = true
			require.Equal(t, "5", string(h.Value))
		case "sent_to_dlq_at":
			hasSentToDLQAt = true
		}
	}
	require.True(t, hasFinalRetryCount, "final_retry_count header should be present")
	require.True(t, hasSentToDLQAt, "sent_to_dlq_at header should be present")
}

func TestIntegration_BatchProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	infra := setupTestInfrastructure(t)
	defer infra.cleanup()

	ctx := context.Background()

	// Create batch of events directly in ClickHouse
	repo := clickhousepkg.NewEventRepository(infra.clickhouseClient)

	// Insert multiple batches
	for batch := 0; batch < 3; batch++ {
		events := make([]*event.Event, 10)
		for i := 0; i < 10; i++ {
			events[i] = &event.Event{
				EventID:   "batch-" + string(rune('A'+batch)) + "-" + string(rune('0'+i)),
				EventName: "batch_test",
				UserID:    "user-" + string(rune('0'+i%5)),
				Channel:   []string{"web", "mobile_app", "api"}[i%3],
				Timestamp: time.Now().Unix(),
			}
		}

		err := repo.InsertBatch(ctx, events)
		require.NoError(t, err)
	}

	// Allow time for materialized view to update
	time.Sleep(2 * time.Second)

	// Query metrics
	query := &event.GetMetricsQuery{
		EventName: "batch_test",
		From:      time.Now().Add(-1 * time.Hour).Unix(),
		To:        time.Now().Add(1 * time.Hour).Unix(),
	}

	result, err := repo.GetMetrics(ctx, query)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.GreaterOrEqual(t, result.TotalCount, int64(30))
}
