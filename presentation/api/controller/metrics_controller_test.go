package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/mocks"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func setupMetricsControllerTest(mockService *mocks.MockMetricsService) *fiber.App {
	app := fiber.New()
	NewMetricsController(app, mockService)
	return app
}

func TestMetricsController_GetMetrics(t *testing.T) {
	t.Run("successful request returns 200", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		expectedResp := &dto.MetricsResponse{
			TotalCount:  100,
			UniqueUsers: 50,
			GroupedData: []dto.GroupedData{
				{Key: "web", TotalCount: 60, UniqueUsers: 30},
				{Key: "mobile_app", TotalCount: 40, UniqueUsers: 20},
			},
		}

		mockService.On("GetMetrics", mock.Anything, mock.AnythingOfType("*event.GetMetricsQuery")).Return(expectedResp, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&channel=web&group_by=channel", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result dto.MetricsResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(100), result.TotalCount)
		assert.Equal(t, int64(50), result.UniqueUsers)
		assert.Len(t, result.GroupedData, 2)
		mockService.AssertExpectations(t)
	})

	t.Run("missing event_name returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?from=1723000000&to=1723100000", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		errors := result["errors"].([]interface{})
		assert.Len(t, errors, 1)
		firstErr := errors[0].(map[string]interface{})
		assert.Equal(t, "event_name", firstErr["field"])
		mockService.AssertNotCalled(t, "GetMetrics")
	})

	t.Run("missing from timestamp returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&to=1723100000", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		errors := result["errors"].([]interface{})
		firstErr := errors[0].(map[string]interface{})
		assert.Equal(t, "from", firstErr["field"])
		mockService.AssertNotCalled(t, "GetMetrics")
	})

	t.Run("missing to timestamp returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		errors := result["errors"].([]interface{})
		firstErr := errors[0].(map[string]interface{})
		assert.Equal(t, "to", firstErr["field"])
		mockService.AssertNotCalled(t, "GetMetrics")
	})

	t.Run("to less than or equal to from returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		// to equals from
		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723000000", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		mockService.AssertNotCalled(t, "GetMetrics")

		// to less than from
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723100000&to=1723000000", nil)

		resp2, err := app.Test(req2)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	})

	t.Run("invalid channel returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&channel=invalid", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		errors := result["errors"].([]interface{})
		firstErr := errors[0].(map[string]interface{})
		assert.Equal(t, "channel", firstErr["field"])
		mockService.AssertNotCalled(t, "GetMetrics")
	})

	t.Run("invalid group_by returns 400", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&group_by=invalid", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		errors := result["errors"].([]interface{})
		firstErr := errors[0].(map[string]interface{})
		assert.Equal(t, "group_by", firstErr["field"])
		mockService.AssertNotCalled(t, "GetMetrics")
	})

	t.Run("valid channel values are accepted", func(t *testing.T) {
		validChannels := []string{"web", "mobile_app", "api"}

		for _, channel := range validChannels {
			t.Run(fmt.Sprintf("channel_%s", channel), func(t *testing.T) {
				mockService := new(mocks.MockMetricsService)
				app := setupMetricsControllerTest(mockService)

				expectedResp := &dto.MetricsResponse{
					TotalCount:  10,
					UniqueUsers: 5,
				}

				mockService.On("GetMetrics", mock.Anything, mock.AnythingOfType("*event.GetMetricsQuery")).Return(expectedResp, nil)

				url := fmt.Sprintf("/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&channel=%s", channel)
				req := httptest.NewRequest(http.MethodGet, url, nil)

				resp, err := app.Test(req)
				require.NoError(t, err)
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				mockService.AssertExpectations(t)
			})
		}
	})

	t.Run("valid group_by values are accepted", func(t *testing.T) {
		validGroupBys := []string{"channel", "hour", "day"}

		for _, groupBy := range validGroupBys {
			t.Run(fmt.Sprintf("group_by_%s", groupBy), func(t *testing.T) {
				mockService := new(mocks.MockMetricsService)
				app := setupMetricsControllerTest(mockService)

				expectedResp := &dto.MetricsResponse{
					TotalCount:  10,
					UniqueUsers: 5,
				}

				mockService.On("GetMetrics", mock.Anything, mock.AnythingOfType("*event.GetMetricsQuery")).Return(expectedResp, nil)

				url := fmt.Sprintf("/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&group_by=%s", groupBy)
				req := httptest.NewRequest(http.MethodGet, url, nil)

				resp, err := app.Test(req)
				require.NoError(t, err)
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				mockService.AssertExpectations(t)
			})
		}
	})

	t.Run("service error returns 500", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		mockService.On("GetMetrics", mock.Anything, mock.AnythingOfType("*event.GetMetricsQuery")).
			Return(nil, assert.AnError)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

		var result dto.ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "failed to get metrics", result.Error)
		mockService.AssertExpectations(t)
	})

	t.Run("query parameters are correctly parsed", func(t *testing.T) {
		mockService := new(mocks.MockMetricsService)
		app := setupMetricsControllerTest(mockService)

		var capturedQuery *event.GetMetricsQuery

		mockService.On("GetMetrics", mock.Anything, mock.AnythingOfType("*event.GetMetricsQuery")).
			Run(func(args mock.Arguments) {
				capturedQuery = args.Get(1).(*event.GetMetricsQuery)
			}).
			Return(&dto.MetricsResponse{}, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?event_name=product_view&from=1723000000&to=1723100000&channel=web&group_by=hour", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		require.NotNil(t, capturedQuery)
		assert.Equal(t, "product_view", capturedQuery.EventName)
		assert.Equal(t, int64(1723000000), capturedQuery.From)
		assert.Equal(t, int64(1723100000), capturedQuery.To)
		assert.Equal(t, "web", capturedQuery.Channel)
		assert.Equal(t, "hour", capturedQuery.GroupBy)
		mockService.AssertExpectations(t)
	})
}
