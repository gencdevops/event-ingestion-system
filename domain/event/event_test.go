package event

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEventName(t *testing.T) {
	tests := []struct {
		name      string
		eventName string
		wantErr   bool
		errCode   string
	}{
		{
			name:      "valid event name",
			eventName: "product_view",
			wantErr:   false,
		},
		{
			name:      "empty event name",
			eventName: "",
			wantErr:   true,
			errCode:   ErrCodeValidationRequired,
		},
		{
			name:      "event name exceeds max length",
			eventName: strings.Repeat("a", MaxEventNameLength+1),
			wantErr:   true,
			errCode:   ErrCodeValidationMaxLength,
		},
		{
			name:      "event name at max length",
			eventName: strings.Repeat("a", MaxEventNameLength),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{EventName: tt.eventName}
			err := ValidateEventName(e)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, tt.errCode, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateUserID(t *testing.T) {
	tests := []struct {
		name    string
		userID  string
		wantErr bool
		errCode string
	}{
		{
			name:    "valid user ID",
			userID:  "user_123",
			wantErr: false,
		},
		{
			name:    "empty user ID",
			userID:  "",
			wantErr: true,
			errCode: ErrCodeValidationRequired,
		},
		{
			name:    "user ID exceeds max length",
			userID:  strings.Repeat("u", MaxUserIDLength+1),
			wantErr: true,
			errCode: ErrCodeValidationMaxLength,
		},
		{
			name:    "user ID at max length",
			userID:  strings.Repeat("u", MaxUserIDLength),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{UserID: tt.userID}
			err := ValidateUserID(e)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, tt.errCode, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateTimestamp(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		timestamp int64
		wantErr   bool
		errCode   string
	}{
		{
			name:      "valid timestamp",
			timestamp: now.Add(-1 * time.Hour).Unix(),
			wantErr:   false,
		},
		{
			name:      "zero timestamp",
			timestamp: 0,
			wantErr:   true,
			errCode:   ErrCodeValidationRequired,
		},
		{
			name:      "future timestamp",
			timestamp: now.Add(1 * time.Hour).Unix(),
			wantErr:   true,
			errCode:   ErrCodeTimestampFuture,
		},
		{
			name:      "timestamp older than 30 days",
			timestamp: now.Add(-31 * 24 * time.Hour).Unix(),
			wantErr:   true,
			errCode:   ErrCodeTimestampExpired,
		},
		{
			name:      "timestamp exactly 30 days old (boundary)",
			timestamp: now.Add(-29 * 24 * time.Hour).Unix(),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{Timestamp: tt.timestamp}
			err := ValidateTimestamp(e)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, tt.errCode, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateChannel(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		wantErr bool
		errCode string
	}{
		{
			name:    "valid channel web",
			channel: ChannelWeb,
			wantErr: false,
		},
		{
			name:    "valid channel mobile_app",
			channel: ChannelMobileApp,
			wantErr: false,
		},
		{
			name:    "valid channel api",
			channel: ChannelAPI,
			wantErr: false,
		},
		{
			name:    "empty channel is valid",
			channel: "",
			wantErr: false,
		},
		{
			name:    "invalid channel",
			channel: "invalid_channel",
			wantErr: true,
			errCode: ErrCodeInvalidChannel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{Channel: tt.channel}
			err := ValidateChannel(e)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, tt.errCode, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestValidateTags(t *testing.T) {
	tests := []struct {
		name    string
		tags    []string
		wantErr bool
		errCode string
	}{
		{
			name:    "valid tags",
			tags:    []string{"electronics", "homepage"},
			wantErr: false,
		},
		{
			name:    "nil tags is valid",
			tags:    nil,
			wantErr: false,
		},
		{
			name:    "empty tags is valid",
			tags:    []string{},
			wantErr: false,
		},
		{
			name:    "max tags allowed",
			tags:    make([]string, MaxTags),
			wantErr: false,
		},
		{
			name:    "exceeds max tags",
			tags:    make([]string, MaxTags+1),
			wantErr: true,
			errCode: ErrCodeMaxTagsExceeded,
		},
		{
			name:    "tag exceeds max length",
			tags:    []string{strings.Repeat("t", MaxTagLength+1)},
			wantErr: true,
			errCode: ErrCodeTagTooLong,
		},
		{
			name:    "tag at max length",
			tags:    []string{strings.Repeat("t", MaxTagLength)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{Tags: tt.tags}
			err := ValidateTags(e)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, tt.errCode, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestGenerateEventID(t *testing.T) {
	t.Run("generates deterministic ID", func(t *testing.T) {
		e1 := &Event{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: 1723475612,
			Channel:   "web",
		}
		e2 := &Event{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: 1723475612,
			Channel:   "web",
		}

		e1.GenerateEventID()
		e2.GenerateEventID()

		assert.NotEmpty(t, e1.EventID)
		assert.Equal(t, e1.EventID, e2.EventID)
	})

	t.Run("different inputs produce different IDs", func(t *testing.T) {
		e1 := &Event{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: 1723475612,
			Channel:   "web",
		}
		e2 := &Event{
			EventName: "product_view",
			UserID:    "user_456",
			Timestamp: 1723475612,
			Channel:   "web",
		}

		e1.GenerateEventID()
		e2.GenerateEventID()

		assert.NotEqual(t, e1.EventID, e2.EventID)
	})

	t.Run("ID is 64 character hex string (SHA256)", func(t *testing.T) {
		e := &Event{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: 1723475612,
			Channel:   "web",
		}

		e.GenerateEventID()

		assert.Len(t, e.EventID, 64)
	})
}

func TestValidateAll(t *testing.T) {
	now := time.Now()

	t.Run("valid event passes all validations", func(t *testing.T) {
		e := &Event{
			EventName: "product_view",
			UserID:    "user_123",
			Timestamp: now.Add(-1 * time.Hour).Unix(),
			Channel:   "web",
			Tags:      []string{"electronics"},
		}

		err := e.ValidateAll()
		assert.NoError(t, err)
	})

	t.Run("collects multiple validation errors", func(t *testing.T) {
		e := &Event{
			EventName: "",
			UserID:    "",
			Timestamp: 0,
			Channel:   "invalid",
		}

		err := e.ValidateAll()
		require.Error(t, err)

		validationErr, ok := err.(*ValidationError)
		require.True(t, ok)
		assert.GreaterOrEqual(t, len(validationErr.Errors), 3)
	})

	t.Run("validation error string contains all field errors", func(t *testing.T) {
		e := &Event{
			EventName: "",
			UserID:    "",
			Timestamp: now.Unix(),
			Channel:   "web",
		}

		err := e.ValidateAll()
		require.Error(t, err)

		errStr := err.Error()
		assert.Contains(t, errStr, "event_name")
		assert.Contains(t, errStr, "user_id")
	})
}

func TestValidationError(t *testing.T) {
	t.Run("empty validation error has no errors", func(t *testing.T) {
		ve := NewValidationError()
		assert.False(t, ve.HasErrors())
		assert.Equal(t, "validation error", ve.Error())
	})

	t.Run("add error and check has errors", func(t *testing.T) {
		ve := NewValidationError()
		ve.Add(ErrorDetail{
			Field:   "test_field",
			Code:    "test_code",
			Message: "test message",
		})

		assert.True(t, ve.HasErrors())
		assert.Len(t, ve.Errors, 1)
	})

	t.Run("error string format", func(t *testing.T) {
		ve := NewValidationError()
		ve.Add(ErrorDetail{
			Field:   "field1",
			Code:    "code1",
			Message: "message1",
		})
		ve.Add(ErrorDetail{
			Field:   "field2",
			Code:    "code2",
			Message: "message2",
		})

		errStr := ve.Error()
		assert.Contains(t, errStr, "field1: message1")
		assert.Contains(t, errStr, "field2: message2")
	})
}

func TestEventHelperMethods(t *testing.T) {
	t.Run("GetTimestamp returns correct time", func(t *testing.T) {
		ts := int64(1723475612)
		e := &Event{Timestamp: ts}

		result := e.GetTimestamp()
		assert.Equal(t, time.Unix(ts, 0), result)
	})

	t.Run("MetadataJSON with nil metadata", func(t *testing.T) {
		e := &Event{Metadata: nil}
		assert.Equal(t, "{}", e.MetadataJSON())
	})

	t.Run("MetadataJSON with valid metadata", func(t *testing.T) {
		e := &Event{
			Metadata: map[string]any{
				"key": "value",
			},
		}
		result := e.MetadataJSON()
		assert.Contains(t, result, "key")
		assert.Contains(t, result, "value")
	})

	t.Run("GetTags with nil tags", func(t *testing.T) {
		e := &Event{Tags: nil}
		assert.Equal(t, []string{}, e.GetTags())
	})

	t.Run("GetTags with tags", func(t *testing.T) {
		tags := []string{"tag1", "tag2"}
		e := &Event{Tags: tags}
		assert.Equal(t, tags, e.GetTags())
	})
}

func TestEventBatch(t *testing.T) {
	t.Run("new batch has correct capacity", func(t *testing.T) {
		batch := NewEventBatch(10)
		assert.Equal(t, 0, batch.Size())
		assert.True(t, batch.IsEmpty())
	})

	t.Run("add events to batch", func(t *testing.T) {
		batch := NewEventBatch(10)
		batch.Add(&Event{EventName: "test1"})
		batch.Add(&Event{EventName: "test2"})

		assert.Equal(t, 2, batch.Size())
		assert.False(t, batch.IsEmpty())
	})

	t.Run("clear batch", func(t *testing.T) {
		batch := NewEventBatch(10)
		batch.Add(&Event{EventName: "test1"})
		batch.Clear()

		assert.Equal(t, 0, batch.Size())
		assert.True(t, batch.IsEmpty())
	})
}
