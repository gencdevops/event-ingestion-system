package worker

import (
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestCalculateBackoff(t *testing.T) {
	// Create a minimal RetryWorker to test the method
	w := &RetryWorker{}

	tests := []struct {
		name       string
		retryCount int
		expected   time.Duration
	}{
		{
			name:       "retry count 1 - 2 seconds",
			retryCount: 1,
			expected:   2 * time.Second,
		},
		{
			name:       "retry count 2 - 4 seconds",
			retryCount: 2,
			expected:   4 * time.Second,
		},
		{
			name:       "retry count 3 - 8 seconds",
			retryCount: 3,
			expected:   8 * time.Second,
		},
		{
			name:       "retry count 4 - 16 seconds",
			retryCount: 4,
			expected:   16 * time.Second,
		},
		{
			name:       "retry count 5 - 32 seconds",
			retryCount: 5,
			expected:   32 * time.Second,
		},
		{
			name:       "retry count 6 - capped at 60 seconds",
			retryCount: 6,
			expected:   60 * time.Second,
		},
		{
			name:       "retry count 10 - still capped at 60 seconds",
			retryCount: 10,
			expected:   60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := w.calculateBackoff(tt.retryCount)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetRetryCount(t *testing.T) {
	w := &RetryWorker{}

	tests := []struct {
		name     string
		headers  []kafkago.Header
		expected int
	}{
		{
			name:     "no headers - returns default",
			headers:  nil,
			expected: defaultRetryCount,
		},
		{
			name:     "empty headers - returns default",
			headers:  []kafkago.Header{},
			expected: defaultRetryCount,
		},
		{
			name: "retry count header present with valid value",
			headers: []kafkago.Header{
				{Key: retryCountHeader, Value: []byte("3")},
			},
			expected: 3,
		},
		{
			name: "retry count header present with value 5",
			headers: []kafkago.Header{
				{Key: retryCountHeader, Value: []byte("5")},
			},
			expected: 5,
		},
		{
			name: "retry count header with invalid value - returns default",
			headers: []kafkago.Header{
				{Key: retryCountHeader, Value: []byte("invalid")},
			},
			expected: defaultRetryCount,
		},
		{
			name: "retry count header with empty value - returns default",
			headers: []kafkago.Header{
				{Key: retryCountHeader, Value: []byte("")},
			},
			expected: defaultRetryCount,
		},
		{
			name: "multiple headers - finds retry count",
			headers: []kafkago.Header{
				{Key: "other_header", Value: []byte("value")},
				{Key: retryCountHeader, Value: []byte("4")},
				{Key: "another_header", Value: []byte("value2")},
			},
			expected: 4,
		},
		{
			name: "other headers only - returns default",
			headers: []kafkago.Header{
				{Key: "error_type", Value: []byte("insert_failed")},
				{Key: "error_message", Value: []byte("connection error")},
			},
			expected: defaultRetryCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := kafkago.Message{
				Headers: tt.headers,
			}
			result := w.getRetryCount(msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackoffFormula(t *testing.T) {
	w := &RetryWorker{}

	// Verify the exponential backoff formula: baseDelay * 2^(retryCount-1)
	// baseDelay = 2s
	t.Run("exponential growth verification", func(t *testing.T) {
		// retryCount 1: 2 * 2^0 = 2s
		assert.Equal(t, 2*time.Second, w.calculateBackoff(1))

		// retryCount 2: 2 * 2^1 = 4s
		assert.Equal(t, 4*time.Second, w.calculateBackoff(2))

		// retryCount 3: 2 * 2^2 = 8s
		assert.Equal(t, 8*time.Second, w.calculateBackoff(3))

		// retryCount 4: 2 * 2^3 = 16s
		assert.Equal(t, 16*time.Second, w.calculateBackoff(4))

		// retryCount 5: 2 * 2^4 = 32s
		assert.Equal(t, 32*time.Second, w.calculateBackoff(5))

		// retryCount 6: 2 * 2^5 = 64s, but capped at 60s
		assert.Equal(t, 60*time.Second, w.calculateBackoff(6))
	})

	t.Run("max delay cap", func(t *testing.T) {
		// Any retry count that would produce > 60s should be capped
		for i := 6; i <= 20; i++ {
			result := w.calculateBackoff(i)
			assert.Equal(t, maxRetryDelay, result, "retry count %d should be capped at maxRetryDelay", i)
		}
	})
}
