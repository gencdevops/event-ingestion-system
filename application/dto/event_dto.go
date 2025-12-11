package dto

type EventResponse struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}

type BulkEventResponse struct {
	SuccessCount int                  `json:"success_count"`
	FailedCount  int                  `json:"failed_count"`
	Errors       []BulkEventItemError `json:"errors,omitempty"`
}

type BulkEventItemError struct {
	Index int    `json:"index"`
	Error string `json:"error"`
}

type ErrorResponse struct {
	Error   string            `json:"error"`
	Details map[string]string `json:"details,omitempty"`
}
