package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/event-ingestion/domain/event"
	"github.com/event-ingestion/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func validEventCommand() *event.IngestEventCommand {
	return &event.IngestEventCommand{
		EventName:  "product_view",
		Channel:    "web",
		CampaignID: "cmp_123",
		UserID:     "user_123",
		Timestamp:  time.Now().Add(-1 * time.Hour).Unix(),
		Tags:       []string{"electronics"},
		Metadata:   map[string]any{"product_id": "prod-789"},
	}
}

func TestEventService_IngestEvent(t *testing.T) {
	t.Run("successful event ingestion", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()

		mockPublisher.On("Publish", mock.Anything, mock.AnythingOfType("*event.Event")).Return(nil)

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.NoError(t, err)
		assert.NotEmpty(t, resp.EventID)
		assert.Equal(t, "queued", resp.Status)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("validation error - empty event name", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()
		cmd.EventName = ""

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.Error(t, err)
		assert.Nil(t, resp)

		validationErr, ok := err.(*event.ValidationError)
		require.True(t, ok)
		assert.True(t, validationErr.HasErrors())
		mockPublisher.AssertNotCalled(t, "Publish")
	})

	t.Run("validation error - empty user ID", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()
		cmd.UserID = ""

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.Error(t, err)
		assert.Nil(t, resp)
		mockPublisher.AssertNotCalled(t, "Publish")
	})

	t.Run("validation error - future timestamp", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()
		cmd.Timestamp = time.Now().Add(1 * time.Hour).Unix()

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.Error(t, err)
		assert.Nil(t, resp)
		mockPublisher.AssertNotCalled(t, "Publish")
	})

	t.Run("kafka publish error", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()
		publishErr := errors.New("kafka connection failed")

		mockPublisher.On("Publish", mock.Anything, mock.AnythingOfType("*event.Event")).Return(publishErr)

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, publishErr, err)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("event ID is generated correctly", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := validEventCommand()
		var capturedEvent *event.Event

		mockPublisher.On("Publish", mock.Anything, mock.AnythingOfType("*event.Event")).
			Run(func(args mock.Arguments) {
				capturedEvent = args.Get(1).(*event.Event)
			}).
			Return(nil)

		resp, err := service.IngestEvent(context.Background(), cmd)

		require.NoError(t, err)
		assert.NotEmpty(t, capturedEvent.EventID)
		assert.Equal(t, capturedEvent.EventID, resp.EventID)
		assert.Len(t, capturedEvent.EventID, 64) // SHA256 hex string
	})
}

func TestEventService_IngestBulk(t *testing.T) {
	t.Run("all events successful", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				*validEventCommand(),
				*validEventCommand(),
			},
		}
		cmd.Events[1].UserID = "user_456"

		mockPublisher.On("PublishBatch", mock.Anything, mock.AnythingOfType("[]*event.Event")).Return(nil)

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err)
		assert.Equal(t, 2, resp.SuccessCount)
		assert.Equal(t, 0, resp.FailedCount)
		assert.Empty(t, resp.Errors)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("partial success - some events invalid", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		validCmd := validEventCommand()
		invalidCmd := validEventCommand()
		invalidCmd.EventName = "" // invalid

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				*validCmd,
				*invalidCmd,
				*validCmd,
			},
		}
		cmd.Events[2].UserID = "user_789"

		mockPublisher.On("PublishBatch", mock.Anything, mock.AnythingOfType("[]*event.Event")).Return(nil)

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err)
		assert.Equal(t, 2, resp.SuccessCount)
		assert.Equal(t, 1, resp.FailedCount)
		assert.Len(t, resp.Errors, 1)
		assert.Equal(t, 1, resp.Errors[0].Index) // second event (index 1) failed
		mockPublisher.AssertExpectations(t)
	})

	t.Run("all events invalid", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		invalidCmd := validEventCommand()
		invalidCmd.EventName = ""

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				*invalidCmd,
				*invalidCmd,
			},
		}

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err)
		assert.Equal(t, 0, resp.SuccessCount)
		assert.Equal(t, 2, resp.FailedCount)
		assert.Len(t, resp.Errors, 2)
		mockPublisher.AssertNotCalled(t, "PublishBatch")
	})

	t.Run("kafka batch publish failure marks all valid events as failed", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				*validEventCommand(),
				*validEventCommand(),
			},
		}
		cmd.Events[1].UserID = "user_456"

		publishErr := errors.New("kafka batch write failed")
		mockPublisher.On("PublishBatch", mock.Anything, mock.AnythingOfType("[]*event.Event")).Return(publishErr)

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err) // Note: IngestBulk returns nil error even when publish fails
		assert.Equal(t, 0, resp.SuccessCount)
		assert.Equal(t, 2, resp.FailedCount)
		assert.Len(t, resp.Errors, 2)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("empty events array", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{},
		}

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err)
		assert.Equal(t, 0, resp.SuccessCount)
		assert.Equal(t, 0, resp.FailedCount)
		mockPublisher.AssertNotCalled(t, "PublishBatch")
	})

	t.Run("mixed validation errors tracked with correct indices", func(t *testing.T) {
		mockPublisher := new(mocks.MockEventPublisher)
		service := NewEventService(mockPublisher)

		cmd := &event.IngestBulkCommand{
			Events: []event.IngestEventCommand{
				*validEventCommand(),                                   // index 0 - valid
				{EventName: "", UserID: "u1", Timestamp: time.Now().Add(-1 * time.Hour).Unix()}, // index 1 - invalid
				*validEventCommand(),                                   // index 2 - valid
				{EventName: "e", UserID: "", Timestamp: time.Now().Add(-1 * time.Hour).Unix()},  // index 3 - invalid
				*validEventCommand(),                                   // index 4 - valid
			},
		}
		cmd.Events[0].UserID = "user_0"
		cmd.Events[2].UserID = "user_2"
		cmd.Events[4].UserID = "user_4"

		mockPublisher.On("PublishBatch", mock.Anything, mock.AnythingOfType("[]*event.Event")).Return(nil)

		resp, err := service.IngestBulk(context.Background(), cmd)

		require.NoError(t, err)
		assert.Equal(t, 3, resp.SuccessCount)
		assert.Equal(t, 2, resp.FailedCount)
		assert.Len(t, resp.Errors, 2)

		// Check error indices
		errorIndices := make([]int, len(resp.Errors))
		for i, e := range resp.Errors {
			errorIndices[i] = e.Index
		}
		assert.Contains(t, errorIndices, 1)
		assert.Contains(t, errorIndices, 3)
		mockPublisher.AssertExpectations(t)
	})
}
