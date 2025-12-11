package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	retryCountHeader  = "retry_count"
	fetchTimeout      = 100 * time.Millisecond
	shutdownTimeout   = 5 * time.Second
	retryWriteTimeout = 10 * time.Second
	dlqWriteTimeout   = 5 * time.Second
)

type EventWorker struct {
	readerConfig kafkago.ReaderConfig
	retryWriter  *kafkago.Writer
	dlqWriter    *kafkago.Writer
	repository   event.EventRepository
	batchSize    int
	batchTimeout time.Duration
	workerCount  int
	wg           sync.WaitGroup
	stopCh       chan struct{}
	retryTopic   string
	dlqTopic     string
}

func NewEventWorker(cfg config.KafkaConfig, repository event.EventRepository, workerCfg config.WorkerConfig) *EventWorker {
	readerConfig := kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.Topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	}

	retryTopic := cfg.Topic + ".retry"
	retryWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        retryTopic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
	}

	dlqTopic := cfg.Topic + ".dlq"
	dlqWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
	}

	return &EventWorker{
		readerConfig: readerConfig,
		retryWriter:  retryWriter,
		dlqWriter:    dlqWriter,
		repository:   repository,
		batchSize:    workerCfg.BatchSize,
		batchTimeout: workerCfg.BatchTimeout,
		workerCount:  workerCfg.Count,
		stopCh:       make(chan struct{}),
		retryTopic:   retryTopic,
		dlqTopic:     dlqTopic,
	}
}

func (w *EventWorker) Start(ctx context.Context) {
	slog.Info("Starting event workers", "count", w.workerCount, "retryTopic", w.retryTopic, "dlqTopic", w.dlqTopic)

	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.run(ctx, i)
	}
}

func (w *EventWorker) Stop() {
	slog.Info("Stopping event workers")
	close(w.stopCh)
	w.wg.Wait()

	if err := w.retryWriter.Close(); err != nil {
		slog.Error("Failed to close retry writer", "error", err)
	}

	if err := w.dlqWriter.Close(); err != nil {
		slog.Error("Failed to close DLQ writer", "error", err)
	}

	slog.Info("All event workers stopped")
}

func (w *EventWorker) run(ctx context.Context, workerID int) {
	defer w.wg.Done()

	reader := kafkago.NewReader(w.readerConfig)
	defer reader.Close()

	slog.Info("Worker started", "workerID", workerID)

	batch := make([]*event.Event, 0, w.batchSize)
	messages := make([]kafkago.Message, 0, w.batchSize)
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, flushing remaining batch", "workerID", workerID)
			w.flushWithNewContext(reader, workerID, batch, messages)
			return

		case <-w.stopCh:
			slog.Info("Stop signal received, flushing remaining batch", "workerID", workerID)
			w.flushWithNewContext(reader, workerID, batch, messages)
			return

		case <-ticker.C:
			batch, messages = w.flush(ctx, reader, workerID, batch, messages)

		default:
			fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
			msg, err := reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if ctx.Err() != nil {
					slog.Info("Context cancelled during fetch, flushing", "workerID", workerID)
					w.flushWithNewContext(reader, workerID, batch, messages)
					return
				}
				continue
			}

			var e event.Event
			if err := json.Unmarshal(msg.Value, &e); err != nil {
				slog.Warn("Skipped malformed event, sending to DLQ",
					"workerID", workerID,
					"error", err,
					"partition", msg.Partition,
					"offset", msg.Offset,
				)
				w.sendToDLQ(ctx, msg, "unmarshal_error", err.Error())
				reader.CommitMessages(ctx, msg)
				continue
			}

			batch = append(batch, &e)
			messages = append(messages, msg)

			if len(batch) >= w.batchSize {
				batch, messages = w.flush(ctx, reader, workerID, batch, messages)
				ticker.Reset(w.batchTimeout)
			}
		}
	}
}

func (w *EventWorker) flushWithNewContext(reader *kafkago.Reader, workerID int, batch []*event.Event, messages []kafkago.Message) {
	if len(batch) == 0 {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	w.flush(shutdownCtx, reader, workerID, batch, messages)
}

func (w *EventWorker) flush(ctx context.Context, reader *kafkago.Reader, workerID int, batch []*event.Event, messages []kafkago.Message) ([]*event.Event, []kafkago.Message) {
	if len(batch) == 0 {
		return batch, messages
	}

	slog.Debug("Flushing batch", "workerID", workerID, "size", len(batch))

	if err := w.repository.InsertBatch(ctx, batch); err != nil {
		slog.Error("Batch insert failed, sending to retry topic",
			"workerID", workerID,
			"error", err,
			"count", len(batch),
		)

		w.sendBatchToRetry(ctx, messages, "insert_failed", err.Error())

		if commitErr := reader.CommitMessages(ctx, messages...); commitErr != nil {
			slog.Error("Offset commit failed", "workerID", workerID, "error", commitErr)
		}
		return batch[:0], messages[:0]
	}

	if err := reader.CommitMessages(ctx, messages...); err != nil {
		slog.Error("Offset commit failed", "workerID", workerID, "error", err)
	}

	return batch[:0], messages[:0]
}

func (w *EventWorker) sendBatchToRetry(ctx context.Context, messages []kafkago.Message, errorType, errorMsg string) {
	retryMessages := make([]kafkago.Message, 0, len(messages))
	failedAt := time.Now().UTC().Format(time.RFC3339)

	for _, msg := range messages {
		retryMessages = append(retryMessages, kafkago.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Headers: []kafkago.Header{
				{Key: retryCountHeader, Value: []byte("1")},
				{Key: "error_type", Value: []byte(errorType)},
				{Key: "error_message", Value: []byte(errorMsg)},
				{Key: "original_topic", Value: []byte(w.readerConfig.Topic)},
				{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
				{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
				{Key: "failed_at", Value: []byte(failedAt)},
			},
		})
	}

	writeCtx, cancel := context.WithTimeout(ctx, retryWriteTimeout)
	defer cancel()

	if err := w.retryWriter.WriteMessages(writeCtx, retryMessages...); err != nil {
		slog.Error("Failed to send batch to retry topic", "error", err, "count", len(messages))
	} else {
		slog.Debug("Sent batch to retry topic", "count", len(messages))
	}
}

func (w *EventWorker) sendToDLQ(ctx context.Context, originalMsg kafkago.Message, errorType, errorMsg string) {
	dlqMessage := kafkago.Message{
		Key:   originalMsg.Key,
		Value: originalMsg.Value,
		Headers: []kafkago.Header{
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "error_message", Value: []byte(errorMsg)},
			{Key: "original_topic", Value: []byte(w.readerConfig.Topic)},
			{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", originalMsg.Partition))},
			{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", originalMsg.Offset))},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	writeCtx, cancel := context.WithTimeout(ctx, dlqWriteTimeout)
	defer cancel()

	if err := w.dlqWriter.WriteMessages(writeCtx, dlqMessage); err != nil {
		slog.Error("Failed to send message to DLQ",
			"error", err,
			"partition", originalMsg.Partition,
			"offset", originalMsg.Offset,
		)
	}
}
