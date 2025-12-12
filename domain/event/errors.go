package event

import (
	"fmt"
	"strings"
)

const (
	ErrCodeValidationRequired  = "validation.required.error"
	ErrCodeValidationMaxLength = "validation.max-length.error"
	ErrCodeTimestampFuture     = "validation.timestamp-future.error"
	ErrCodeTimestampExpired    = "validation.timestamp-expired.error"
	ErrCodeInvalidChannel      = "validation.invalid-channel.error"
	ErrCodeMaxTagsExceeded     = "validation.max-tags.error"
	ErrCodeTagTooLong          = "validation.tag-too-long.error"
	ErrCodeInvalidGroupBy      = "validation.invalid-group-by.error"
	ErrCodeInvalidRange        = "validation.invalid-range.error"
)

type ErrorDetail struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ValidationError struct {
	Errors []ErrorDetail `json:"errors"`
}

func (e *ValidationError) Error() string {
	if len(e.Errors) == 0 {
		return "validation error"
	}

	messages := make([]string, 0, len(e.Errors))
	for _, err := range e.Errors {
		messages = append(messages, fmt.Sprintf("%s: %s", err.Field, err.Message))
	}
	return strings.Join(messages, "; ")
}

func (e *ValidationError) Add(detail ErrorDetail) {
	e.Errors = append(e.Errors, detail)
}

func (e *ValidationError) HasErrors() bool {
	return len(e.Errors) > 0
}

func NewValidationError() *ValidationError {
	return &ValidationError{
		Errors: make([]ErrorDetail, 0),
	}
}
