package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	defaultRetryCount = 1
	baseRetryDelay    = 2 * time.Second
	maxRetryDelay     = 60 * time.Second
)

type RetryWorker struct {
	readerConfig     kafkago.ReaderConfig
	retryWriter      *kafkago.Writer
	dlqWriter        *kafkago.Writer
	repository       event.EventRepository
	batchSize        int
	batchTimeout     time.Duration
	workerCount      int
	maxRetryAttempts int
	wg               sync.WaitGroup
	stopCh           chan struct{}
	retryTopic       string
	dlqTopic         string
}

func NewRetryWorker(cfg config.KafkaConfig, repository event.EventRepository, workerCfg config.WorkerConfig) *RetryWorker {
	retryTopic := cfg.Topic + ".retry"
	dlqTopic := cfg.Topic + ".dlq"

	readerConfig := kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup + "-retry",
		Topic:          retryTopic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	}

	retryWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        retryTopic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
	}

	dlqWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
	}

	return &RetryWorker{
		readerConfig:     readerConfig,
		retryWriter:      retryWriter,
		dlqWriter:        dlqWriter,
		repository:       repository,
		batchSize:        workerCfg.BatchSize,
		batchTimeout:     workerCfg.BatchTimeout,
		workerCount:      workerCfg.RetryWorkerCount,
		maxRetryAttempts: workerCfg.MaxRetryAttempts,
		stopCh:           make(chan struct{}),
		retryTopic:       retryTopic,
		dlqTopic:         dlqTopic,
	}
}

func (w *RetryWorker) Start(ctx context.Context) {
	slog.Info("Starting retry workers", "count", w.workerCount, "retryTopic", w.retryTopic, "dlqTopic", w.dlqTopic)

	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.run(ctx, i)
	}
}

func (w *RetryWorker) Stop() {
	slog.Info("Stopping retry workers")
	close(w.stopCh)
	w.wg.Wait()

	if err := w.retryWriter.Close(); err != nil {
		slog.Error("Failed to close retry writer", "error", err)
	}

	if err := w.dlqWriter.Close(); err != nil {
		slog.Error("Failed to close DLQ writer", "error", err)
	}

	slog.Info("All retry workers stopped")
}

func (w *RetryWorker) run(ctx context.Context, workerID int) {
	defer w.wg.Done()

	reader := kafkago.NewReader(w.readerConfig)
	defer reader.Close()

	slog.Info("Retry worker started", "workerID", workerID)

	batch := make([]*event.Event, 0, w.batchSize)
	messages := make([]kafkago.Message, 0, w.batchSize)
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, flushing remaining retry batch", "workerID", workerID)
			w.flushWithNewContext(reader, workerID, batch, messages)
			return

		case <-w.stopCh:
			slog.Info("Stop signal received, flushing remaining retry batch", "workerID", workerID)
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
				retryCount := w.getRetryCount(msg)
				slog.Warn("Skipped malformed event in retry, sending to DLQ",
					"workerID", workerID,
					"error", err,
					"retryCount", retryCount,
				)
				w.sendToDLQ(ctx, msg, "unmarshal_error", err.Error(), retryCount)
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

func (w *RetryWorker) flushWithNewContext(reader *kafkago.Reader, workerID int, batch []*event.Event, messages []kafkago.Message) {
	if len(batch) == 0 {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	w.flush(shutdownCtx, reader, workerID, batch, messages)
}

func (w *RetryWorker) flush(ctx context.Context, reader *kafkago.Reader, workerID int, batch []*event.Event, messages []kafkago.Message) ([]*event.Event, []kafkago.Message) {
	if len(batch) == 0 {
		return batch, messages
	}

	// Calculate max retry count in batch for backoff
	maxRetryInBatch := 0
	for _, msg := range messages {
		if rc := w.getRetryCount(msg); rc > maxRetryInBatch {
			maxRetryInBatch = rc
		}
	}

	// Exponential backoff before processing: 2s, 4s, 8s, 16s, 32s, max 60s
	if maxRetryInBatch > 0 {
		delay := w.calculateBackoff(maxRetryInBatch)
		slog.Debug("Applying retry backoff", "workerID", workerID, "retryCount", maxRetryInBatch, "delay", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return batch, messages
		case <-w.stopCh:
			return batch, messages
		}
	}

	slog.Debug("Flushing retry batch", "workerID", workerID, "size", len(batch))

	if err := w.repository.InsertBatch(ctx, batch); err != nil {
		slog.Error("Retry batch insert failed",
			"workerID", workerID,
			"error", err,
			"count", len(batch),
		)

		var toRetry []kafkago.Message
		var toDLQ []kafkago.Message

		for _, msg := range messages {
			retryCount := w.getRetryCount(msg)
			if retryCount >= w.maxRetryAttempts {
				toDLQ = append(toDLQ, msg)
			} else {
				toRetry = append(toRetry, msg)
			}
		}

		if len(toRetry) > 0 {
			w.sendBatchToRetry(ctx, toRetry, "insert_failed", err.Error())
		}

		if len(toDLQ) > 0 {
			w.sendBatchToDLQ(ctx, toDLQ, "max_retries_exhausted", err.Error())
		}

		if commitErr := reader.CommitMessages(ctx, messages...); commitErr != nil {
			slog.Error("Failed to commit after retry processing", "workerID", workerID, "error", commitErr)
		}

		return batch[:0], messages[:0]
	}

	if err := reader.CommitMessages(ctx, messages...); err != nil {
		slog.Error("Offset commit failed in retry worker", "workerID", workerID, "error", err)
	}

	return batch[:0], messages[:0]
}

func (w *RetryWorker) calculateBackoff(retryCount int) time.Duration {
	// Exponential backoff: 2^retryCount * baseDelay
	delay := baseRetryDelay * time.Duration(1<<uint(retryCount-1))
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	return delay
}

func (w *RetryWorker) getRetryCount(msg kafkago.Message) int {
	for _, h := range msg.Headers {
		if h.Key == retryCountHeader {
			count, err := strconv.Atoi(string(h.Value))
			if err != nil {
				slog.Warn("Invalid retry count header, using default", "value", string(h.Value))
				return defaultRetryCount
			}
			return count
		}
	}
	return defaultRetryCount
}

func (w *RetryWorker) sendBatchToRetry(ctx context.Context, messages []kafkago.Message, errorType, errorMsg string) {
	retryMessages := make([]kafkago.Message, 0, len(messages))
	lastRetryAt := time.Now().UTC().Format(time.RFC3339)

	for _, msg := range messages {
		currentRetryCount := w.getRetryCount(msg)
		newRetryCount := currentRetryCount + 1

		headers := []kafkago.Header{
			{Key: retryCountHeader, Value: []byte(strconv.Itoa(newRetryCount))},
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "error_message", Value: []byte(errorMsg)},
			{Key: "last_retry_at", Value: []byte(lastRetryAt)},
		}

		for _, h := range msg.Headers {
			if h.Key != retryCountHeader && h.Key != "error_type" && h.Key != "error_message" && h.Key != "last_retry_at" {
				headers = append(headers, h)
			}
		}

		retryMessages = append(retryMessages, kafkago.Message{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		})
	}

	writeCtx, cancel := context.WithTimeout(ctx, retryWriteTimeout)
	defer cancel()

	if err := w.retryWriter.WriteMessages(writeCtx, retryMessages...); err != nil {
		slog.Error("Failed to send batch to retry topic", "error", err, "count", len(messages))
	} else {
		slog.Debug("Requeued batch for retry", "count", len(messages))
	}
}

func (w *RetryWorker) sendBatchToDLQ(ctx context.Context, messages []kafkago.Message, errorType, errorMsg string) {
	dlqMessages := make([]kafkago.Message, 0, len(messages))
	sentToDLQAt := time.Now().UTC().Format(time.RFC3339)

	for _, msg := range messages {
		retryCount := w.getRetryCount(msg)

		headers := []kafkago.Header{
			{Key: "final_retry_count", Value: []byte(strconv.Itoa(retryCount))},
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "error_message", Value: []byte(errorMsg)},
			{Key: "sent_to_dlq_at", Value: []byte(sentToDLQAt)},
		}

		for _, h := range msg.Headers {
			if h.Key != retryCountHeader && h.Key != "error_type" && h.Key != "error_message" {
				headers = append(headers, h)
			}
		}

		dlqMessages = append(dlqMessages, kafkago.Message{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		})
	}

	writeCtx, cancel := context.WithTimeout(ctx, retryWriteTimeout)
	defer cancel()

	if err := w.dlqWriter.WriteMessages(writeCtx, dlqMessages...); err != nil {
		slog.Error("Failed to send batch to DLQ", "error", err, "count", len(messages))
	} else {
		slog.Debug("Sent batch to DLQ", "count", len(messages))
	}
}

func (w *RetryWorker) sendToDLQ(ctx context.Context, originalMsg kafkago.Message, errorType, errorMsg string, retryCount int) {
	w.sendBatchToDLQ(ctx, []kafkago.Message{originalMsg}, errorType, errorMsg)
}
