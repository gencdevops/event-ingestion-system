package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/event-ingestion/domain/event"
)

type EventRepository struct {
	conn     driver.Conn
	database string
}

func NewEventRepository(client *Client) *EventRepository {
	return &EventRepository{conn: client.Conn(), database: client.Database()}
}

func (r *EventRepository) InsertBatch(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO %s.events (
			event_id, event_name, channel, campaign_id, user_id,
			timestamp, tags, metadata, created_at
		)
	`, r.database))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, e := range events {
		err := batch.Append(
			e.EventID,
			e.EventName,
			e.Channel,
			e.CampaignID,
			e.UserID,
			e.GetTimestamp(),
			e.GetTags(),
			e.MetadataJSON(),
			time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

func (r *EventRepository) GetMetrics(ctx context.Context, query *event.GetMetricsQuery) (*event.MetricsResult, error) {
	fromTime := time.Unix(query.From, 0)
	toTime := time.Unix(query.To, 0)

	var conditions []string
	var args []interface{}

	conditions = append(conditions, "event_name = ?")
	args = append(args, query.EventName)

	conditions = append(conditions, "hour >= ?")
	args = append(args, fromTime)

	conditions = append(conditions, "hour <= ?")
	args = append(args, toTime)

	if query.Channel != "" {
		conditions = append(conditions, "channel = ?")
		args = append(args, query.Channel)
	}

	whereClause := strings.Join(conditions, " AND ")

	var totalCount, uniqueUsers uint64
	baseQuery := fmt.Sprintf(`
		SELECT
			sum(event_count) as total_count,
			uniqMerge(unique_users_state) as unique_users
		FROM %s.events_hourly
		WHERE %s
	`, r.database, whereClause)

	row := r.conn.QueryRow(ctx, baseQuery, args...)
	if err := row.Scan(&totalCount, &uniqueUsers); err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}

	result := &event.MetricsResult{
		TotalCount:  int64(totalCount),
		UniqueUsers: int64(uniqueUsers),
	}

	if query.GroupBy != "" {
		groupedData, err := r.getGroupedMetrics(ctx, query, whereClause, args)
		if err != nil {
			return nil, err
		}
		result.GroupedData = groupedData
	}

	return result, nil
}

func (r *EventRepository) getGroupedMetrics(ctx context.Context, query *event.GetMetricsQuery, whereClause string, args []interface{}) ([]event.GroupedMetric, error) {
	var groupByColumn string
	var keyFormat string

	switch query.GroupBy {
	case "channel":
		groupByColumn = "channel"
		keyFormat = "channel"
	case "hour":
		groupByColumn = "hour"
		keyFormat = "toString(hour)"
	case "day":
		groupByColumn = "toStartOfDay(hour)"
		keyFormat = "toString(toStartOfDay(hour))"
	default:
		return nil, nil
	}

	groupQuery := fmt.Sprintf(`
		SELECT
			%s as key,
			sum(event_count) as total_count,
			uniqMerge(unique_users_state) as unique_users
		FROM %s.events_hourly
		WHERE %s
		GROUP BY %s
		ORDER BY %s
	`, keyFormat, r.database, whereClause, groupByColumn, groupByColumn)

	rows, err := r.conn.Query(ctx, groupQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get grouped metrics: %w", err)
	}
	defer rows.Close()

	var result []event.GroupedMetric
	for rows.Next() {
		var data event.GroupedMetric
		var totalCount, uniqueUsers uint64
		if err := rows.Scan(&data.Key, &totalCount, &uniqueUsers); err != nil {
			return nil, fmt.Errorf("failed to scan grouped metrics: %w", err)
		}
		data.TotalCount = int64(totalCount)
		data.UniqueUsers = int64(uniqueUsers)
		result = append(result, data)
	}

	return result, nil
}
