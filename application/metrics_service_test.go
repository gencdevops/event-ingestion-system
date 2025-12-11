package application

import (
	"context"
	"errors"
	"testing"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetricsService_GetMetrics(t *testing.T) {
	t.Run("successful query with grouped data", func(t *testing.T) {
		mockRepo := new(mocks.MockEventRepository)
		service := NewMetricsService(mockRepo)

		query := &event.GetMetricsQuery{
			EventName: "product_view",
			From:      1723000000,
			To:        1723100000,
			Channel:   "web",
			GroupBy:   "channel",
		}

		repoResult := &event.MetricsResult{
			TotalCount:  100,
			UniqueUsers: 50,
			GroupedData: []event.GroupedMetric{
				{Key: "web", TotalCount: 60, UniqueUsers: 30},
				{Key: "mobile_app", TotalCount: 40, UniqueUsers: 20},
			},
		}

		mockRepo.On("GetMetrics", mock.Anything, query).Return(repoResult, nil)

		resp, err := service.GetMetrics(context.Background(), query)

		require.NoError(t, err)
		assert.Equal(t, int64(100), resp.TotalCount)
		assert.Equal(t, int64(50), resp.UniqueUsers)
		assert.Len(t, resp.GroupedData, 2)
		assert.Equal(t, "web", resp.GroupedData[0].Key)
		assert.Equal(t, int64(60), resp.GroupedData[0].TotalCount)
		assert.Equal(t, int64(30), resp.GroupedData[0].UniqueUsers)
		mockRepo.AssertExpectations(t)
	})

	t.Run("successful query without grouped data", func(t *testing.T) {
		mockRepo := new(mocks.MockEventRepository)
		service := NewMetricsService(mockRepo)

		query := &event.GetMetricsQuery{
			EventName: "product_view",
			From:      1723000000,
			To:        1723100000,
		}

		repoResult := &event.MetricsResult{
			TotalCount:  100,
			UniqueUsers: 50,
			GroupedData: nil,
		}

		mockRepo.On("GetMetrics", mock.Anything, query).Return(repoResult, nil)

		resp, err := service.GetMetrics(context.Background(), query)

		require.NoError(t, err)
		assert.Equal(t, int64(100), resp.TotalCount)
		assert.Equal(t, int64(50), resp.UniqueUsers)
		assert.Nil(t, resp.GroupedData)
		mockRepo.AssertExpectations(t)
	})

	t.Run("repository error", func(t *testing.T) {
		mockRepo := new(mocks.MockEventRepository)
		service := NewMetricsService(mockRepo)

		query := &event.GetMetricsQuery{
			EventName: "product_view",
			From:      1723000000,
			To:        1723100000,
		}

		repoErr := errors.New("database connection failed")
		mockRepo.On("GetMetrics", mock.Anything, query).Return(nil, repoErr)

		resp, err := service.GetMetrics(context.Background(), query)

		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, repoErr, err)
		mockRepo.AssertExpectations(t)
	})

	t.Run("empty grouped data array", func(t *testing.T) {
		mockRepo := new(mocks.MockEventRepository)
		service := NewMetricsService(mockRepo)

		query := &event.GetMetricsQuery{
			EventName: "product_view",
			From:      1723000000,
			To:        1723100000,
			GroupBy:   "hour",
		}

		repoResult := &event.MetricsResult{
			TotalCount:  0,
			UniqueUsers: 0,
			GroupedData: []event.GroupedMetric{}, // empty but not nil
		}

		mockRepo.On("GetMetrics", mock.Anything, query).Return(repoResult, nil)

		resp, err := service.GetMetrics(context.Background(), query)

		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.TotalCount)
		assert.Equal(t, int64(0), resp.UniqueUsers)
		assert.Nil(t, resp.GroupedData) // empty slice doesn't get converted
		mockRepo.AssertExpectations(t)
	})
}
