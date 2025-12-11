package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	ChannelWeb       = "web"
	ChannelMobileApp = "mobile_app"
	ChannelAPI       = "api"

	MaxEventNameLength = 100
	MaxUserIDLength    = 100
	MaxCampaignIDLength = 100
	MaxTags            = 20
	MaxTagLength       = 100
	MaxTimestampAge    = 30 * 24 * time.Hour // 30 days
)

var validChannels = map[string]bool{
	ChannelWeb:       true,
	ChannelMobileApp: true,
	ChannelAPI:       true,
	"":               true, // empty is valid (optional)
}

type Event struct {
	EventID    string         `json:"event_id"`
	EventName  string         `json:"event_name"`
	Channel    string         `json:"channel"`
	CampaignID string         `json:"campaign_id"`
	UserID     string         `json:"user_id"`
	Timestamp  int64          `json:"timestamp"`
	Tags       []string       `json:"tags"`
	Metadata   map[string]any `json:"metadata"`
	CreatedAt  time.Time      `json:"created_at"`
}

func (e *Event) GenerateEventID() {
	data := fmt.Sprintf("%s|%s|%d|%s", e.EventName, e.UserID, e.Timestamp, e.Channel)
	hash := sha256.Sum256([]byte(data))
	e.EventID = hex.EncodeToString(hash[:])
}

func (e *Event) GetTimestamp() time.Time {
	return time.Unix(e.Timestamp, 0)
}

func (e *Event) MetadataJSON() string {
	if e.Metadata == nil {
		return "{}"
	}
	data, err := json.Marshal(e.Metadata)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func (e *Event) GetTags() []string {
	if e.Tags == nil {
		return []string{}
	}
	return e.Tags
}

type ValidateFunc func(e *Event) *ErrorDetail

var ValidateEventName ValidateFunc = func(e *Event) *ErrorDetail {
	if e.EventName == "" {
		return &ErrorDetail{
			Field:   "event_name",
			Code:    ErrCodeValidationRequired,
			Message: "event_name is required",
		}
	}
	if len(e.EventName) > MaxEventNameLength {
		return &ErrorDetail{
			Field:   "event_name",
			Code:    ErrCodeValidationMaxLength,
			Message: fmt.Sprintf("event_name must be at most %d characters", MaxEventNameLength),
		}
	}
	return nil
}

var ValidateUserID ValidateFunc = func(e *Event) *ErrorDetail {
	if e.UserID == "" {
		return &ErrorDetail{
			Field:   "user_id",
			Code:    ErrCodeValidationRequired,
			Message: "user_id is required",
		}
	}
	if len(e.UserID) > MaxUserIDLength {
		return &ErrorDetail{
			Field:   "user_id",
			Code:    ErrCodeValidationMaxLength,
			Message: fmt.Sprintf("user_id must be at most %d characters", MaxUserIDLength),
		}
	}
	return nil
}

var ValidateTimestamp ValidateFunc = func(e *Event) *ErrorDetail {
	if e.Timestamp == 0 {
		return &ErrorDetail{
			Field:   "timestamp",
			Code:    ErrCodeValidationRequired,
			Message: "timestamp is required",
		}
	}

	eventTime := e.GetTimestamp()
	now := time.Now()

	if eventTime.After(now) {
		return &ErrorDetail{
			Field:   "timestamp",
			Code:    ErrCodeTimestampFuture,
			Message: "timestamp cannot be in the future",
		}
	}

	oldestAllowed := now.Add(-MaxTimestampAge)
	if eventTime.Before(oldestAllowed) {
		return &ErrorDetail{
			Field:   "timestamp",
			Code:    ErrCodeTimestampExpired,
			Message: "timestamp cannot be older than 30 days",
		}
	}

	return nil
}

var ValidateChannel ValidateFunc = func(e *Event) *ErrorDetail {
	if !validChannels[e.Channel] {
		return &ErrorDetail{
			Field:   "channel",
			Code:    ErrCodeInvalidChannel,
			Message: fmt.Sprintf("channel must be one of: %s, %s, %s", ChannelWeb, ChannelMobileApp, ChannelAPI),
		}
	}
	return nil
}

var ValidateTags ValidateFunc = func(e *Event) *ErrorDetail {
	if len(e.Tags) > MaxTags {
		return &ErrorDetail{
			Field:   "tags",
			Code:    ErrCodeMaxTagsExceeded,
			Message: fmt.Sprintf("tags must have at most %d items", MaxTags),
		}
	}
	for i, tag := range e.Tags {
		if len(tag) > MaxTagLength {
			return &ErrorDetail{
				Field:   fmt.Sprintf("tags[%d]", i),
				Code:    ErrCodeTagTooLong,
				Message: fmt.Sprintf("tag must be at most %d characters", MaxTagLength),
			}
		}
	}
	return nil
}

func (e *Event) Validate(functions ...ValidateFunc) error {
	validationErr := NewValidationError()

	for _, fn := range functions {
		if err := fn(e); err != nil {
			validationErr.Add(*err)
		}
	}

	if validationErr.HasErrors() {
		return validationErr
	}
	return nil
}

func (e *Event) ValidateAll() error {
	return e.Validate(
		ValidateEventName,
		ValidateUserID,
		ValidateTimestamp,
		ValidateChannel,
		ValidateTags,
	)
}

type EventBatch struct {
	Events []*Event
}

func NewEventBatch(capacity int) *EventBatch {
	return &EventBatch{
		Events: make([]*Event, 0, capacity),
	}
}

func (b *EventBatch) Add(event *Event) {
	b.Events = append(b.Events, event)
}

func (b *EventBatch) Size() int {
	return len(b.Events)
}

func (b *EventBatch) Clear() {
	b.Events = b.Events[:0]
}

func (b *EventBatch) IsEmpty() bool {
	return len(b.Events) == 0
}
