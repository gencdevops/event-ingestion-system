package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/mocks"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func setupEventControllerTest(mockService *mocks.MockEventService) *fiber.App {
	app := fiber.New()
	NewEventController(app, mockService)
	return app
}

func TestEventController_IngestEvent(t *testing.T) {
	t.Run("successful ingestion returns 202", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestEventCommand{
			EventName: "product_view",
			Channel:   "web",
			UserID:    "user_123",
			Timestamp: time.Now().Add(-1 * time.Hour).Unix(),
			Tags:      []string{"electronics"},
			Metadata:  map[string]any{"product_id": "prod-789"},
		}

		expectedResp := &dto.EventResponse{
			EventID: "abc123def456",
			Status:  "queued",
		}

		mockService.On("IngestEvent", mock.Anything, mock.AnythingOfType("*event.IngestEventCommand")).Return(expectedResp, nil)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusAccepted, resp.StatusCode)

		var result dto.EventResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, expectedResp.EventID, result.EventID)
		assert.Equal(t, expectedResp.Status, result.Status)
		mockService.AssertExpectations(t)
	})

	t.Run("validation error returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestEventCommand{
			EventName: "",
			UserID:    "user_123",
			Timestamp: time.Now().Unix(),
		}

		validationErr := event.NewValidationError()
		validationErr.Add(event.ErrorDetail{
			Field:   "event_name",
			Code:    "validation.required.error",
			Message: "event_name is required",
		})

		mockService.On("IngestEvent", mock.Anything, mock.AnythingOfType("*event.IngestEventCommand")).Return(nil, validationErr)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "validation failed", result["error"])
		assert.NotNil(t, result["errors"])
		mockService.AssertExpectations(t)
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result dto.ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "invalid request body", result.Error)
		mockService.AssertNotCalled(t, "IngestEvent")
	})

	t.Run("internal error returns 500", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestEventCommand{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: time.Now().Unix(),
		}

		mockService.On("IngestEvent", mock.Anything, mock.AnythingOfType("*event.IngestEventCommand")).
			Return(nil, assert.AnError)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		mockService.AssertExpectations(t)
	})
}

func TestHealthController_Health(t *testing.T) {
	t.Run("returns 200 ok", func(t *testing.T) {
		app := fiber.New()
		NewHealthController(app)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]string
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "ok", result["status"])
	})
}

func TestEventController_IngestBulk(t *testing.T) {
	t.Run("all events successful returns 202", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				{EventName: "event1", UserID: "user1", Timestamp: time.Now().Unix()},
				{EventName: "event2", UserID: "user2", Timestamp: time.Now().Unix()},
			},
		}

		expectedResp := &dto.BulkEventResponse{
			SuccessCount: 2,
			FailedCount:  0,
			Errors:       []dto.BulkEventItemError{},
		}

		mockService.On("IngestBulk", mock.Anything, mock.AnythingOfType("*event.IngestBulkCommand")).Return(expectedResp, nil)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusAccepted, resp.StatusCode)

		var result dto.BulkEventResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, 2, result.SuccessCount)
		assert.Equal(t, 0, result.FailedCount)
		mockService.AssertExpectations(t)
	})

	t.Run("partial success returns 207", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				{EventName: "event1", UserID: "user1", Timestamp: time.Now().Unix()},
				{EventName: "", UserID: "user2", Timestamp: time.Now().Unix()},
			},
		}

		expectedResp := &dto.BulkEventResponse{
			SuccessCount: 1,
			FailedCount:  1,
			Errors: []dto.BulkEventItemError{
				{Index: 1, Error: "validation failed"},
			},
		}

		mockService.On("IngestBulk", mock.Anything, mock.AnythingOfType("*event.IngestBulkCommand")).Return(expectedResp, nil)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusMultiStatus, resp.StatusCode)

		var result dto.BulkEventResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, 1, result.SuccessCount)
		assert.Equal(t, 1, result.FailedCount)
		mockService.AssertExpectations(t)
	})

	t.Run("all events failed returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				{EventName: "", UserID: "user1", Timestamp: time.Now().Unix()},
				{EventName: "", UserID: "user2", Timestamp: time.Now().Unix()},
			},
		}

		expectedResp := &dto.BulkEventResponse{
			SuccessCount: 0,
			FailedCount:  2,
			Errors: []dto.BulkEventItemError{
				{Index: 0, Error: "validation failed"},
				{Index: 1, Error: "validation failed"},
			},
		}

		mockService.On("IngestBulk", mock.Anything, mock.AnythingOfType("*event.IngestBulkCommand")).Return(expectedResp, nil)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		mockService.AssertExpectations(t)
	})

	t.Run("empty events array returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestBulkCommand{
			Events: []event.IngestEventCommand{},
		}

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result dto.ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "events array cannot be empty", result.Error)
		mockService.AssertNotCalled(t, "IngestBulk")
	})

	t.Run("exceeds 1000 events limit returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		events := make([]event.IngestEventCommand, 1001)
		for i := 0; i < 1001; i++ {
			events[i] = event.IngestEventCommand{
				EventName: "event",
				UserID:    "user",
				Timestamp: time.Now().Unix(),
			}
		}

		cmd := event.IngestBulkCommand{Events: events}

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result dto.ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "events array cannot exceed 1000 items", result.Error)
		mockService.AssertNotCalled(t, "IngestBulk")
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result dto.ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "invalid request body", result.Error)
		mockService.AssertNotCalled(t, "IngestBulk")
	})

	t.Run("internal error returns 500", func(t *testing.T) {
		mockService := new(mocks.MockEventService)
		app := setupEventControllerTest(mockService)

		cmd := event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				{EventName: "event1", UserID: "user1", Timestamp: time.Now().Unix()},
			},
		}

		mockService.On("IngestBulk", mock.Anything, mock.AnythingOfType("*event.IngestBulkCommand")).
			Return(nil, assert.AnError)

		body, _ := json.Marshal(cmd)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/bulk", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		mockService.AssertExpectations(t)
	})
}
