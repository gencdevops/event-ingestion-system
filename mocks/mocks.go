package mocks

import (
	"context"

	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/stretchr/testify/mock"
)

// MockEventPublisher is a mock implementation of kafka.EventPublisher
type MockEventPublisher struct {
	mock.Mock
}

func (m *MockEventPublisher) Publish(ctx context.Context, e *event.Event) error {
	args := m.Called(ctx, e)
	return args.Error(0)
}

func (m *MockEventPublisher) PublishBatch(ctx context.Context, events []*event.Event) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockEventPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockEventRepository is a mock implementation of event.EventRepository
type MockEventRepository struct {
	mock.Mock
}

func (m *MockEventRepository) InsertBatch(ctx context.Context, events []*event.Event) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockEventRepository) GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*event.MetricsResult, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*event.MetricsResult), args.Error(1)
}

// MockEventService is a mock implementation of application.EventService
type MockEventService struct {
	mock.Mock
}

func (m *MockEventService) IngestEvent(ctx context.Context, cmd *event.IngestEventCommand) (*dto.EventResponse, error) {
	args := m.Called(ctx, cmd)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*dto.EventResponse), args.Error(1)
}

func (m *MockEventService) IngestBulk(ctx context.Context, cmd *event.IngestBulkCommand) (*dto.BulkEventResponse, error) {
	args := m.Called(ctx, cmd)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*dto.BulkEventResponse), args.Error(1)
}

// MockMetricsService is a mock implementation of application.MetricsService
type MockMetricsService struct {
	mock.Mock
}

func (m *MockMetricsService) GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*dto.MetricsResponse, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*dto.MetricsResponse), args.Error(1)
}
