package event

import "time"

type IngestEventCommand struct {
	EventName  string         `json:"event_name"`
	Channel    string         `json:"channel"`
	CampaignID string         `json:"campaign_id"`
	UserID     string         `json:"user_id"`
	Timestamp  int64          `json:"timestamp"`
	Tags       []string       `json:"tags"`
	Metadata   map[string]any `json:"metadata"`
}

func (cmd *IngestEventCommand) ToEvent() *Event {
	return &Event{
		EventName:  cmd.EventName,
		Channel:    cmd.Channel,
		CampaignID: cmd.CampaignID,
		UserID:     cmd.UserID,
		Timestamp:  cmd.Timestamp,
		Tags:       cmd.Tags,
		Metadata:   cmd.Metadata,
		CreatedAt:  time.Now(),
	}
}

type IngestBulkCommand struct {
	Events []IngestEventCommand `json:"events"`
}

func (cmd *IngestBulkCommand) ToEvents() []*Event {
	events := make([]*Event, 0, len(cmd.Events))
	for _, e := range cmd.Events {
		events = append(events, e.ToEvent())
	}
	return events
}
