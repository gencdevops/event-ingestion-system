package dto

type MetricsResponse struct {
	TotalCount  int64         `json:"total_count"`
	UniqueUsers int64         `json:"unique_users"`
	GroupedData []GroupedData `json:"grouped_data,omitempty"`
}

type GroupedData struct {
	Key         string `json:"key"`
	TotalCount  int64  `json:"total_count"`
	UniqueUsers int64  `json:"unique_users"`
}
