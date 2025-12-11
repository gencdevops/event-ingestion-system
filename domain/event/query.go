package event

type GetMetricsQuery struct {
	EventName string `query:"event_name" validate:"required"`
	From      int64  `query:"from" validate:"required"`
	To        int64  `query:"to" validate:"required,gtfield=From"`
	Channel   string `query:"channel" validate:"omitempty,oneof=web mobile_app api"`
	GroupBy   string `query:"group_by" validate:"omitempty,oneof=channel hour day"`
}

type MetricsResult struct {
	TotalCount  int64           `json:"total_count"`
	UniqueUsers int64           `json:"unique_users"`
	GroupedData []GroupedMetric `json:"grouped_data,omitempty"`
}

type GroupedMetric struct {
	Key         string `json:"key"`
	TotalCount  int64  `json:"total_count"`
	UniqueUsers int64  `json:"unique_users"`
}
