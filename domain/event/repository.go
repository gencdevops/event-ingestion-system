package event

import "context"

type EventRepository interface {
	InsertBatch(ctx context.Context, events []*Event) error
	GetMetrics(ctx context.Context, query *GetMetricsQuery) (*MetricsResult, error)
}
