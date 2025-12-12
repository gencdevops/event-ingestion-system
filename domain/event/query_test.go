package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMetricsQuery_Validate_Valid(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
	}

	err := query.Validate()
	assert.Nil(t, err)
}

func TestGetMetricsQuery_Validate_ValidWithOptionals(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
		Channel:   "web",
		GroupBy:   "channel",
	}

	err := query.Validate()
	assert.Nil(t, err)
}

func TestGetMetricsQuery_Validate_MissingEventName(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "",
		From:      1699999999,
		To:        1700000099,
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)
	require.True(t, validationErr.HasErrors())

	hasEventNameError := false
	for _, e := range validationErr.Errors {
		if e.Field == "event_name" {
			hasEventNameError = true
			assert.Equal(t, ErrCodeValidationRequired, e.Code)
		}
	}
	assert.True(t, hasEventNameError)
}

func TestGetMetricsQuery_Validate_MissingFrom(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      0,
		To:        1700000099,
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)

	hasFromError := false
	for _, e := range validationErr.Errors {
		if e.Field == "from" {
			hasFromError = true
			assert.Equal(t, ErrCodeValidationRequired, e.Code)
		}
	}
	assert.True(t, hasFromError)
}

func TestGetMetricsQuery_Validate_MissingTo(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        0,
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)

	hasToError := false
	for _, e := range validationErr.Errors {
		if e.Field == "to" {
			hasToError = true
			assert.Equal(t, ErrCodeValidationRequired, e.Code)
		}
	}
	assert.True(t, hasToError)
}

func TestGetMetricsQuery_Validate_ToLessThanOrEqualFrom(t *testing.T) {
	tests := []struct {
		name string
		from int64
		to   int64
	}{
		{"to less than from", 1700000099, 1699999999},
		{"to equal to from", 1699999999, 1699999999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &GetMetricsQuery{
				EventName: "user_signup",
				From:      tt.from,
				To:        tt.to,
			}

			err := query.Validate()
			require.NotNil(t, err)

			validationErr, ok := err.(*ValidationError)
			require.True(t, ok)

			hasRangeError := false
			for _, e := range validationErr.Errors {
				if e.Field == "to" && e.Code == ErrCodeInvalidRange {
					hasRangeError = true
				}
			}
			assert.True(t, hasRangeError)
		})
	}
}

func TestGetMetricsQuery_Validate_InvalidChannel(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
		Channel:   "invalid_channel",
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)

	hasChannelError := false
	for _, e := range validationErr.Errors {
		if e.Field == "channel" {
			hasChannelError = true
			assert.Equal(t, ErrCodeInvalidChannel, e.Code)
		}
	}
	assert.True(t, hasChannelError)
}

func TestGetMetricsQuery_Validate_ValidChannels(t *testing.T) {
	channels := []string{"web", "mobile_app", "api"}

	for _, channel := range channels {
		t.Run("channel_"+channel, func(t *testing.T) {
			query := &GetMetricsQuery{
				EventName: "user_signup",
				From:      1699999999,
				To:        1700000099,
				Channel:   channel,
			}

			err := query.Validate()
			assert.Nil(t, err)
		})
	}
}

func TestGetMetricsQuery_Validate_InvalidGroupBy(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
		GroupBy:   "invalid_group",
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)

	hasGroupByError := false
	for _, e := range validationErr.Errors {
		if e.Field == "group_by" {
			hasGroupByError = true
			assert.Equal(t, ErrCodeInvalidGroupBy, e.Code)
		}
	}
	assert.True(t, hasGroupByError)
}

func TestGetMetricsQuery_Validate_ValidGroupBy(t *testing.T) {
	groupBys := []string{"channel", "hour", "day"}

	for _, groupBy := range groupBys {
		t.Run("groupBy_"+groupBy, func(t *testing.T) {
			query := &GetMetricsQuery{
				EventName: "user_signup",
				From:      1699999999,
				To:        1700000099,
				GroupBy:   groupBy,
			}

			err := query.Validate()
			assert.Nil(t, err)
		})
	}
}

func TestGetMetricsQuery_Validate_MultipleErrors(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "",
		From:      0,
		To:        0,
		Channel:   "invalid",
		GroupBy:   "invalid",
	}

	err := query.Validate()
	require.NotNil(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)

	// Should have at least 5 errors
	assert.GreaterOrEqual(t, len(validationErr.Errors), 4)
}

func TestGetMetricsQuery_Validate_EmptyGroupBy(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
		GroupBy:   "",
	}

	err := query.Validate()
	assert.Nil(t, err)
}

func TestGetMetricsQuery_Validate_EmptyChannel(t *testing.T) {
	query := &GetMetricsQuery{
		EventName: "user_signup",
		From:      1699999999,
		To:        1700000099,
		Channel:   "",
	}

	err := query.Validate()
	assert.Nil(t, err)
}
