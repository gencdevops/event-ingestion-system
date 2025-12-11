package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/config"
	"github.com/segmentio/kafka-go"
)

// EventPublisher defines the interface for publishing events to a message broker
type EventPublisher interface {
	Publish(ctx context.Context, e *event.Event) error
	PublishBatch(ctx context.Context, events []*event.Event) error
	Close() error
}

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(cfg config.KafkaConfig) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    2000,
		BatchBytes:   1048676,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("Failed to deliver messages", "error", err, "count", len(messages))
			}
		},
	}

	return &Producer{
		writer: writer,
		topic:  cfg.Topic,
	}
}

func (p *Producer) Publish(ctx context.Context, e *event.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(e.UserID),
		Value: data,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(events))
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(e.UserID),
			Value: data,
		})
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type TopicConfig struct {
	Name       string
	Partitions int
}

func EnsureTopicsWithConfig(cfg config.KafkaConfig, topics []TopicConfig) error {
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		if err.Error() != "Topic with this name already exists" {
			return fmt.Errorf("failed to create topics: %w", err)
		}
		slog.Info("Topics already exist, continuing...")
	}

	topicNames := make([]string, len(topics))
	for i, t := range topics {
		topicNames[i] = t.Name
	}
	slog.Info("Ensured topics exist", "topics", topicNames)
	return nil
}
