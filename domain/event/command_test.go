package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestEventCommand_ToEvent(t *testing.T) {
	cmd := &IngestEventCommand{
		EventName:  "user_signup",
		Channel:    "web",
		CampaignID: "camp-123",
		UserID:     "user-456",
		Timestamp:  1699999999,
		Tags:       []string{"tag1", "tag2"},
		Metadata:   map[string]any{"key": "value"},
	}

	event := cmd.ToEvent()

	require.NotNil(t, event)
	assert.Equal(t, "user_signup", event.EventName)
	assert.Equal(t, "web", event.Channel)
	assert.Equal(t, "camp-123", event.CampaignID)
	assert.Equal(t, "user-456", event.UserID)
	assert.Equal(t, int64(1699999999), event.Timestamp)
	assert.Equal(t, []string{"tag1", "tag2"}, event.Tags)
	assert.Equal(t, map[string]any{"key": "value"}, event.Metadata)
	assert.False(t, event.CreatedAt.IsZero())
}

func TestIngestEventCommand_ToEvent_EmptyFields(t *testing.T) {
	cmd := &IngestEventCommand{
		EventName: "test_event",
		UserID:    "user-123",
		Timestamp: 1699999999,
	}

	event := cmd.ToEvent()

	require.NotNil(t, event)
	assert.Equal(t, "test_event", event.EventName)
	assert.Equal(t, "", event.Channel)
	assert.Equal(t, "", event.CampaignID)
	assert.Nil(t, event.Tags)
	assert.Nil(t, event.Metadata)
}

func TestIngestBulkCommand_Validate_Empty(t *testing.T) {
	cmd := &IngestBulkCommand{
		Events: []IngestEventCommand{},
	}

	err := cmd.Validate()

	require.NotNil(t, err)
	assert.Equal(t, "events", err.Field)
	assert.Equal(t, ErrCodeValidationRequired, err.Code)
	assert.Contains(t, err.Message, "cannot be empty")
}

func TestIngestBulkCommand_Validate_ExceedsMax(t *testing.T) {
	events := make([]IngestEventCommand, MaxBulkEvents+1)
	for i := range events {
		events[i] = IngestEventCommand{
			EventName: "test_event",
			UserID:    "user-123",
			Timestamp: 1699999999,
		}
	}

	cmd := &IngestBulkCommand{
		Events: events,
	}

	err := cmd.Validate()

	require.NotNil(t, err)
	assert.Equal(t, "events", err.Field)
	assert.Equal(t, ErrCodeValidationMaxLength, err.Code)
	assert.Contains(t, err.Message, "cannot exceed 1000")
}

func TestIngestBulkCommand_Validate_Valid(t *testing.T) {
	cmd := &IngestBulkCommand{
		Events: []IngestEventCommand{
			{EventName: "event1", UserID: "user1", Timestamp: 1699999999},
			{EventName: "event2", UserID: "user2", Timestamp: 1699999999},
		},
	}

	err := cmd.Validate()

	assert.Nil(t, err)
}

func TestIngestBulkCommand_Validate_ExactlyMax(t *testing.T) {
	events := make([]IngestEventCommand, MaxBulkEvents)
	for i := range events {
		events[i] = IngestEventCommand{
			EventName: "test_event",
			UserID:    "user-123",
			Timestamp: 1699999999,
		}
	}

	cmd := &IngestBulkCommand{
		Events: events,
	}

	err := cmd.Validate()

	assert.Nil(t, err)
}

func TestIngestBulkCommand_ToEvents(t *testing.T) {
	cmd := &IngestBulkCommand{
		Events: []IngestEventCommand{
			{EventName: "event1", UserID: "user1", Timestamp: 1699999999},
			{EventName: "event2", UserID: "user2", Timestamp: 1699999999},
			{EventName: "event3", UserID: "user3", Timestamp: 1699999999},
		},
	}

	events := cmd.ToEvents()

	require.Len(t, events, 3)
	assert.Equal(t, "event1", events[0].EventName)
	assert.Equal(t, "user1", events[0].UserID)
	assert.Equal(t, "event2", events[1].EventName)
	assert.Equal(t, "user2", events[1].UserID)
	assert.Equal(t, "event3", events[2].EventName)
	assert.Equal(t, "user3", events[2].UserID)
}

func TestIngestBulkCommand_ToEvents_Empty(t *testing.T) {
	cmd := &IngestBulkCommand{
		Events: []IngestEventCommand{},
	}

	events := cmd.ToEvents()

	require.Empty(t, events)
}

func TestMaxBulkEvents_Value(t *testing.T) {
	assert.Equal(t, 1000, MaxBulkEvents)
}
