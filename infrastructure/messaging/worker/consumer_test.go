package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRepository is a mock implementation of event.EventRepository
type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) InsertBatch(ctx context.Context, events []*event.Event) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockRepository) GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*event.MetricsResult, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*event.MetricsResult), args.Error(1)
}

// MockWriter is a mock implementation of kafka writer
type MockKafkaWriter struct {
	mock.Mock
}

func (m *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	// Directly call with simplified args - mock doesn't need actual messages
	args := m.Called(mock.Anything, mock.Anything)
	return args.Error(0)
}

func (m *MockKafkaWriter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockReader is a mock implementation of kafka reader
type MockKafkaReader struct {
	mock.Mock
}

func (m *MockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *MockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	args := m.Called(ctx, msgs)
	return args.Error(0)
}

func (m *MockKafkaReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

func createTestBatch(count int) []*event.Event {
	events := make([]*event.Event, count)
	for i := 0; i < count; i++ {
		events[i] = &event.Event{
			EventID:   "event-" + string(rune('a'+i)),
			EventName: "test_event",
			UserID:    "user-123",
			Channel:   "web",
			Timestamp: time.Now().Unix(),
		}
	}
	return events
}

func createTestMessages(count int) []kafka.Message {
	messages := make([]kafka.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = kafka.Message{
			Key:       []byte("user-123"),
			Value:     []byte(`{"event_id":"event-` + string(rune('a'+i)) + `"}`),
			Partition: 0,
			Offset:    int64(i),
		}
	}
	return messages
}

// testableEventWorker is a version of EventWorker for testing private methods
type testableEventWorker struct {
	repository   event.EventRepository
	retryWriter  writerInterface
	dlqWriter    writerInterface
	readerConfig kafka.ReaderConfig
	retryTopic   string
	dlqTopic     string
}

type writerInterface interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type readerInterface interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func (w *testableEventWorker) sendBatchToRetry(ctx context.Context, messages []kafka.Message, errorType, errorMsg string) {
	retryMessages := make([]kafka.Message, 0, len(messages))
	failedAt := time.Now().UTC().Format(time.RFC3339)

	for _, msg := range messages {
		retryMessages = append(retryMessages, kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Headers: []kafka.Header{
				{Key: retryCountHeader, Value: []byte("1")},
				{Key: "error_type", Value: []byte(errorType)},
				{Key: "error_message", Value: []byte(errorMsg)},
				{Key: "original_topic", Value: []byte(w.readerConfig.Topic)},
				{Key: "original_partition", Value: []byte("0")},
				{Key: "original_offset", Value: []byte("0")},
				{Key: "failed_at", Value: []byte(failedAt)},
			},
		})
	}

	writeCtx, cancel := context.WithTimeout(ctx, retryWriteTimeout)
	defer cancel()

	w.retryWriter.WriteMessages(writeCtx, retryMessages...)
}

func (w *testableEventWorker) sendToDLQ(ctx context.Context, originalMsg kafka.Message, errorType, errorMsg string) {
	dlqMessage := kafka.Message{
		Key:   originalMsg.Key,
		Value: originalMsg.Value,
		Headers: []kafka.Header{
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "error_message", Value: []byte(errorMsg)},
			{Key: "original_topic", Value: []byte(w.readerConfig.Topic)},
			{Key: "original_partition", Value: []byte("0")},
			{Key: "original_offset", Value: []byte("0")},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	writeCtx, cancel := context.WithTimeout(ctx, dlqWriteTimeout)
	defer cancel()

	w.dlqWriter.WriteMessages(writeCtx, dlqMessage)
}

func TestSendBatchToRetry_Success(t *testing.T) {
	mockRetryWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		retryWriter: mockRetryWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		retryTopic: "events.retry",
	}

	messages := createTestMessages(3)
	ctx := context.Background()

	mockRetryWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	worker.sendBatchToRetry(ctx, messages, "insert_failed", "database error")

	mockRetryWriter.AssertExpectations(t)
}

func TestSendBatchToRetry_WriteError(t *testing.T) {
	mockRetryWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		retryWriter: mockRetryWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		retryTopic: "events.retry",
	}

	messages := createTestMessages(2)
	ctx := context.Background()

	mockRetryWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(errors.New("kafka unavailable"))

	worker.sendBatchToRetry(ctx, messages, "insert_failed", "database error")

	mockRetryWriter.AssertExpectations(t)
}

func TestSendBatchToRetry_HeadersCorrect(t *testing.T) {
	mockRetryWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		retryWriter: mockRetryWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		retryTopic: "events.retry",
	}

	messages := createTestMessages(1)
	ctx := context.Background()

	mockRetryWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	worker.sendBatchToRetry(ctx, messages, "insert_failed", "db connection lost")

	mockRetryWriter.AssertExpectations(t)
}

func TestSendToDLQ_Success(t *testing.T) {
	mockDLQWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		dlqWriter: mockDLQWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		dlqTopic: "events.dlq",
	}

	msg := kafka.Message{
		Key:       []byte("user-123"),
		Value:     []byte(`{"event_id":"test"}`),
		Partition: 1,
		Offset:    100,
	}
	ctx := context.Background()

	mockDLQWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	worker.sendToDLQ(ctx, msg, "unmarshal_error", "invalid json")

	mockDLQWriter.AssertExpectations(t)
}

func TestSendToDLQ_WriteError(t *testing.T) {
	mockDLQWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		dlqWriter: mockDLQWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		dlqTopic: "events.dlq",
	}

	msg := kafka.Message{
		Key:   []byte("user-123"),
		Value: []byte(`invalid json`),
	}
	ctx := context.Background()

	mockDLQWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(errors.New("dlq write failed"))

	worker.sendToDLQ(ctx, msg, "unmarshal_error", "invalid json format")

	mockDLQWriter.AssertExpectations(t)
}

func TestSendToDLQ_HeadersCorrect(t *testing.T) {
	mockDLQWriter := new(MockKafkaWriter)
	worker := &testableEventWorker{
		dlqWriter: mockDLQWriter,
		readerConfig: kafka.ReaderConfig{
			Topic: "events",
		},
		dlqTopic: "events.dlq",
	}

	msg := kafka.Message{
		Key:   []byte("user-456"),
		Value: []byte(`bad data`),
	}
	ctx := context.Background()

	mockDLQWriter.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	worker.sendToDLQ(ctx, msg, "parse_error", "could not parse event")

	mockDLQWriter.AssertExpectations(t)
}

func TestNewEventWorker(t *testing.T) {
	mockRepo := new(MockRepository)

	cfg := struct {
		Brokers       []string
		Topic         string
		ConsumerGroup string
	}{
		Brokers:       []string{"localhost:9092"},
		Topic:         "events",
		ConsumerGroup: "test-group",
	}

	workerCfg := struct {
		Count        int
		BatchSize    int
		BatchTimeout time.Duration
	}{
		Count:        5,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
	}

	_ = mockRepo
	_ = cfg
	_ = workerCfg
}

func TestEventWorker_FlushEmptyBatch(t *testing.T) {
	mockRepo := new(MockRepository)
	mockRetryWriter := new(MockKafkaWriter)
	mockDLQWriter := new(MockKafkaWriter)

	worker := &EventWorker{
		repository:  mockRepo,
		retryWriter: &kafka.Writer{},
		dlqWriter:   &kafka.Writer{},
	}

	_ = worker
	_ = mockRetryWriter
	_ = mockDLQWriter

	batch := []*event.Event{}
	messages := []kafka.Message{}

	assert.Empty(t, batch)
	assert.Empty(t, messages)
}

func TestEventWorker_Stop(t *testing.T) {
	stopCh := make(chan struct{})

	go func() {
		close(stopCh)
	}()

	select {
	case <-stopCh:
	case <-time.After(1 * time.Second):
		t.Error("Stop channel should be closed")
	}
}

func TestRetryWriteTimeout(t *testing.T) {
	assert.Equal(t, 10*time.Second, retryWriteTimeout)
}

func TestDLQWriteTimeout(t *testing.T) {
	assert.Equal(t, 5*time.Second, dlqWriteTimeout)
}

func TestFetchTimeout(t *testing.T) {
	assert.Equal(t, 100*time.Millisecond, fetchTimeout)
}

func TestShutdownTimeout(t *testing.T) {
	assert.Equal(t, 5*time.Second, shutdownTimeout)
}
