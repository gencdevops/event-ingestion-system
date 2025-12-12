package event

var validGroupBy = map[string]bool{"channel": true, "hour": true, "day": true}

type GetMetricsQuery struct {
	EventName string `query:"event_name" validate:"required"`
	From      int64  `query:"from" validate:"required"`
	To        int64  `query:"to" validate:"required,gtfield=From"`
	Channel   string `query:"channel" validate:"omitempty,oneof=web mobile_app api"`
	GroupBy   string `query:"group_by" validate:"omitempty,oneof=channel hour day"`
}

func (q *GetMetricsQuery) Validate() error {
	validationErr := NewValidationError()

	if q.EventName == "" {
		validationErr.Add(ErrorDetail{
			Field:   "event_name",
			Code:    ErrCodeValidationRequired,
			Message: "event_name is required",
		})
	}

	if q.From == 0 {
		validationErr.Add(ErrorDetail{
			Field:   "from",
			Code:    ErrCodeValidationRequired,
			Message: "from timestamp is required",
		})
	}

	if q.To == 0 {
		validationErr.Add(ErrorDetail{
			Field:   "to",
			Code:    ErrCodeValidationRequired,
			Message: "to timestamp is required",
		})
	}

	if q.From != 0 && q.To != 0 && q.To <= q.From {
		validationErr.Add(ErrorDetail{
			Field:   "to",
			Code:    ErrCodeInvalidRange,
			Message: "to timestamp must be greater than from timestamp",
		})
	}

	if q.Channel != "" && !validChannels[q.Channel] {
		validationErr.Add(ErrorDetail{
			Field:   "channel",
			Code:    ErrCodeInvalidChannel,
			Message: "invalid channel value, must be one of: web, mobile_app, api",
		})
	}

	if q.GroupBy != "" && !validGroupBy[q.GroupBy] {
		validationErr.Add(ErrorDetail{
			Field:   "group_by",
			Code:    ErrCodeInvalidGroupBy,
			Message: "invalid group_by value, must be one of: channel, hour, day",
		})
	}

	if validationErr.HasErrors() {
		return validationErr
	}

	return nil
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
