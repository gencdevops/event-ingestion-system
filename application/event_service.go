package application

import (
	"context"
	"log/slog"
	"time"

	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/infrastructure/messaging/kafka"
)

type EventService interface {
	IngestEvent(ctx context.Context, cmd *event.IngestEventCommand) (*dto.EventResponse, error)
	IngestBulk(ctx context.Context, cmd *event.IngestBulkCommand) (*dto.BulkEventResponse, error)
}

type eventService struct {
	producer kafka.EventPublisher
}

func NewEventService(producer kafka.EventPublisher) EventService {
	return &eventService{
		producer: producer,
	}
}

func (s *eventService) IngestEvent(ctx context.Context, cmd *event.IngestEventCommand) (*dto.EventResponse, error) {
	e := s.prepareEvent(cmd)

	if err := e.ValidateAll(); err != nil {
		slog.Warn("Validation failed for event", "eventID", e.EventID, "eventName", e.EventName, "error", err)
		return nil, err
	}

	if err := s.producer.Publish(ctx, e); err != nil {
		slog.Error("Failed to publish event to Kafka", "eventID", e.EventID, "eventName", e.EventName, "error", err)
		return nil, err
	}

	slog.Info("Event published to Kafka", "eventID", e.EventID, "eventName", e.EventName, "userID", e.UserID)
	return &dto.EventResponse{
		EventID: e.EventID,
		Status:  "queued",
	}, nil
}

func (s *eventService) IngestBulk(ctx context.Context, cmd *event.IngestBulkCommand) (*dto.BulkEventResponse, error) {
	response := &dto.BulkEventResponse{
		Errors: make([]dto.BulkEventItemError, 0),
	}

	validEvents, failedIndices := s.validateBulkEvents(cmd, response)

	if len(validEvents) == 0 {
		slog.Warn("All bulk events failed validation", "total", len(cmd.Events))
		return response, nil
	}

	if err := s.producer.PublishBatch(ctx, validEvents); err != nil {
		slog.Error("Failed to publish bulk events to Kafka", "validCount", len(validEvents), "error", err)
		s.markRemainingAsFailed(cmd, failedIndices, response)
		return response, nil
	}

	response.SuccessCount = len(validEvents)
	slog.Info("Bulk events published to Kafka", "total", len(cmd.Events), "success", response.SuccessCount, "failed", response.FailedCount)
	return response, nil
}

func (s *eventService) prepareEvent(cmd *event.IngestEventCommand) *event.Event {
	e := cmd.ToEvent()
	e.GenerateEventID()
	e.CreatedAt = time.Now()
	return e
}

func (s *eventService) validateBulkEvents(cmd *event.IngestBulkCommand, response *dto.BulkEventResponse) ([]*event.Event, map[int]struct{}) {
	failedIndices := make(map[int]struct{})
	validEvents := make([]*event.Event, 0, len(cmd.Events))

	for i, eventCmd := range cmd.Events {
		e := s.prepareEvent(&eventCmd)

		if err := e.ValidateAll(); err != nil {
			failedIndices[i] = struct{}{}
			response.FailedCount++
			response.Errors = append(response.Errors, dto.BulkEventItemError{
				Index: i,
				Error: err.Error(),
			})
			continue
		}

		validEvents = append(validEvents, e)
	}

	return validEvents, failedIndices
}

func (s *eventService) markRemainingAsFailed(cmd *event.IngestBulkCommand, failedIndices map[int]struct{}, response *dto.BulkEventResponse) {
	for i := range cmd.Events {
		if _, failed := failedIndices[i]; !failed {
			response.Errors = append(response.Errors, dto.BulkEventItemError{
				Index: i,
				Error: "failed to publish event",
			})
		}
	}
	response.FailedCount = len(cmd.Events)
	response.SuccessCount = 0
}
