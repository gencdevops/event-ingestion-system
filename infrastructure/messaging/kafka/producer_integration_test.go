//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func setupKafkaContainer(t *testing.T) (string, func()) {
	ctx := context.Background()

	container, err := kafkamodule.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafkamodule.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	cleanup := func() {
		testcontainers.CleanupContainer(t, container)
	}

	return brokers[0], cleanup
}

func createTestTopic(t *testing.T, broker, topic string) {
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
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	// Ignore error if topic already exists
	_ = err

	// Wait for topic to be ready
	time.Sleep(500 * time.Millisecond)
}

func TestIntegration_Publish_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	topic := "test-events"
	createTestTopic(t, broker, topic)

	// Create producer
	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer producer.Close()

	// Create test event
	e := &event.Event{
		EventID:   "integration-test-1",
		EventName: "user_signup",
		UserID:    "user-123",
		Channel:   "web",
		Timestamp: time.Now().Unix(),
	}

	ctx := context.Background()
	err := producer.Publish(ctx, e)
	require.NoError(t, err)
}

func TestIntegration_PublishBatch_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	topic := "test-events-batch"
	createTestTopic(t, broker, topic)

	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer producer.Close()

	events := []*event.Event{
		{
			EventID:   "batch-test-1",
			EventName: "page_view",
			UserID:    "user-1",
			Channel:   "web",
			Timestamp: time.Now().Unix(),
		},
		{
			EventID:   "batch-test-2",
			EventName: "page_view",
			UserID:    "user-2",
			Channel:   "mobile_app",
			Timestamp: time.Now().Unix(),
		},
		{
			EventID:   "batch-test-3",
			EventName: "page_view",
			UserID:    "user-3",
			Channel:   "api",
			Timestamp: time.Now().Unix(),
		},
	}

	ctx := context.Background()
	err := producer.PublishBatch(ctx, events)
	require.NoError(t, err)
}

func TestIntegration_PublishAndConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-consume"
	createTestTopic(t, broker, topic)

	// Create producer
	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer producer.Close()

	// Publish event
	e := &event.Event{
		EventID:   "consume-test-1",
		EventName: "button_click",
		UserID:    "user-456",
		Channel:   "web",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"test", "integration"},
	}

	ctx := context.Background()
	err := producer.Publish(ctx, e)
	require.NoError(t, err)

	// Wait for async write
	time.Sleep(500 * time.Millisecond)

	// Create consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set read deadline
	reader.SetOffset(kafka.FirstOffset)
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	// Verify message
	var receivedEvent event.Event
	err = json.Unmarshal(msg.Value, &receivedEvent)
	require.NoError(t, err)

	assert.Equal(t, e.EventID, receivedEvent.EventID)
	assert.Equal(t, e.EventName, receivedEvent.EventName)
	assert.Equal(t, e.UserID, receivedEvent.UserID)
	assert.Equal(t, e.Channel, receivedEvent.Channel)
}

func TestIntegration_PublishBatch_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   "test-empty-batch",
	})
	defer producer.Close()

	ctx := context.Background()
	err := producer.PublishBatch(ctx, []*event.Event{})
	require.NoError(t, err)
}

func TestIntegration_ProducerClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   "test-close",
	})

	err := producer.Close()
	require.NoError(t, err)
}

func TestIntegration_PublishBatch_LargeBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	topic := "test-large-batch"
	createTestTopic(t, broker, topic)

	producer := NewProducer(config.KafkaConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer producer.Close()

	// Create 50 events
	events := make([]*event.Event, 50)
	for i := 0; i < 50; i++ {
		events[i] = &event.Event{
			EventID:   "large-batch-" + string(rune('a'+i%26)),
			EventName: "bulk_event",
			UserID:    "user-" + string(rune('0'+i%10)),
			Channel:   []string{"web", "mobile_app", "api"}[i%3],
			Timestamp: time.Now().Unix(),
		}
	}

	ctx := context.Background()
	err := producer.PublishBatch(ctx, events)
	require.NoError(t, err)
}
