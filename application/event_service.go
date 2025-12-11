package application

import (
	"context"
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
	e := cmd.ToEvent()
	e.GenerateEventID()
	e.CreatedAt = time.Now()

	if err := e.ValidateAll(); err != nil {
		return nil, err
	}

	if err := s.producer.Publish(ctx, e); err != nil {
		return nil, err
	}

	return &dto.EventResponse{
		EventID: e.EventID,
		Status:  "queued",
	}, nil
}

func (s *eventService) IngestBulk(ctx context.Context, cmd *event.IngestBulkCommand) (*dto.BulkEventResponse, error) {
	response := &dto.BulkEventResponse{
		Errors: make([]dto.BulkEventItemError, 0),
	}

	validEvents := make([]*event.Event, 0, len(cmd.Events))

	for i, eventCmd := range cmd.Events {
		e := eventCmd.ToEvent()
		e.GenerateEventID()
		e.CreatedAt = time.Now()

		if err := e.ValidateAll(); err != nil {
			response.FailedCount++
			response.Errors = append(response.Errors, dto.BulkEventItemError{
				Index: i,
				Error: err.Error(),
			})
			continue
		}

		validEvents = append(validEvents, e)
	}

	if len(validEvents) > 0 {
		if err := s.producer.PublishBatch(ctx, validEvents); err != nil {
			// If batch publish fails, mark all valid events as failed
			for i := range cmd.Events {
				alreadyFailed := false
				for _, e := range response.Errors {
					if e.Index == i {
						alreadyFailed = true
						break
					}
				}
				if !alreadyFailed {
					response.Errors = append(response.Errors, dto.BulkEventItemError{
						Index: i,
						Error: "failed to publish event",
					})
				}
			}
			response.FailedCount = len(cmd.Events)
			response.SuccessCount = 0
			return response, nil
		}
	}

	response.SuccessCount = len(validEvents)

	return response, nil
}
