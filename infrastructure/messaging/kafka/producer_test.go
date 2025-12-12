package kafka

import (
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestEvent() *event.Event {
	return &event.Event{
		EventID:   "test-event-id",
		EventName: "test_event",
		UserID:    "user-123",
		Channel:   "web",
		Timestamp: time.Now().Unix(),
	}
}

func TestNewProducer(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-events",
	}

	producer := NewProducer(cfg)

	require.NotNil(t, producer)
	assert.Equal(t, "test-events", producer.topic)
	assert.NotNil(t, producer.writer)
}

func TestNewProducer_MultipleBrokers(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers: []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
		Topic:   "multi-broker-topic",
	}

	producer := NewProducer(cfg)

	require.NotNil(t, producer)
	assert.Equal(t, "multi-broker-topic", producer.topic)
}

func TestPublishBatch_EmptySlice(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-events",
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// Empty slice should return nil without error
	err := producer.PublishBatch(nil, []*event.Event{})
	require.NoError(t, err)
}

func TestPublishBatch_NilSlice(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-events",
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// Nil slice should return nil without error
	err := producer.PublishBatch(nil, nil)
	require.NoError(t, err)
}

func TestProducerClose(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-events",
	}

	producer := NewProducer(cfg)

	err := producer.Close()
	require.NoError(t, err)
}

func TestProducer_TopicAssignment(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected string
	}{
		{"simple topic", "events", "events"},
		{"topic with dots", "events.production", "events.production"},
		{"topic with hyphens", "my-events-topic", "my-events-topic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   tt.topic,
			}

			producer := NewProducer(cfg)
			defer producer.Close()

			assert.Equal(t, tt.expected, producer.topic)
		})
	}
}
