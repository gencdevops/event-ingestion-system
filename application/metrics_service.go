package application

import (
	"context"

	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
)

type MetricsService interface {
	GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*dto.MetricsResponse, error)
}

type metricsService struct {
	repository event.EventRepository
}

func NewMetricsService(repository event.EventRepository) MetricsService {
	return &metricsService{
		repository: repository,
	}
}

func (s *metricsService) GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*dto.MetricsResponse, error) {
	result, err := s.repository.GetMetrics(ctx, query)
	if err != nil {
		return nil, err
	}

	response := &dto.MetricsResponse{
		TotalCount:  result.TotalCount,
		UniqueUsers: result.UniqueUsers,
	}

	if len(result.GroupedData) > 0 {
		response.GroupedData = make([]dto.GroupedData, len(result.GroupedData))
		for i, g := range result.GroupedData {
			response.GroupedData[i] = dto.GroupedData{
				Key:         g.Key,
				TotalCount:  g.TotalCount,
				UniqueUsers: g.UniqueUsers,
			}
		}
	}

	return response, nil
}
