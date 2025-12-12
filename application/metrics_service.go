package application

import (
	"context"
	"log/slog"

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
		slog.Error("Failed to fetch metrics from repository", "eventName", query.EventName, "from", query.From, "to", query.To, "error", err)
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

	slog.Info("Metrics fetched", "eventName", query.EventName, "totalCount", result.TotalCount, "uniqueUsers", result.UniqueUsers, "groupedCount", len(result.GroupedData))
	return response, nil
}
