# Event Ingestion System

High-throughput event ingestion and metric aggregation system built with Go, Kafka, and ClickHouse.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  HTTP API   │────▶│  Kafka Cluster   │────▶│  Consumer Pool  │────▶│  ClickHouse  │
│  (Fiber)    │     │  (2 brokers)     │     │  (20 workers)   │     │              │
└─────────────┘     └──────────────────┘     └─────────────────┘     └──────────────┘
      │                                                                │
      │                                                                │
      └────────────────────────GET /metrics────────────────────────────┘
```

### Kafka Cluster Configuration

| Topic | Partitions | Replication Factor | Description |
|-------|------------|-------------------|-------------|
| events | 20 | 2 | Main event topic |
| events.retry | 10 | 2 | Failed events for retry |
| events.dlq | 5 | 2 | Dead letter queue |

- **2 Brokers**: High availability with automatic failover
- **Replication Factor 2**: Every message stored on both brokers
- **Min In-Sync Replicas**: All replicas must be in sync

### Key Design Decisions

- **DDD Architecture**: Domain-Driven Design with clear layer separation
- **Kafka for Async Processing**: Events are immediately queued for processing
- **ClickHouse for Analytics**: Optimized for high-volume time-series data and aggregations
- **Batch Processing**: Workers accumulate events (2000 or 50ms timeout) before inserting
- **Idempotency**: Event ID generated from SHA256(event_name + user_id + timestamp + channel)
- **At-Least-Once Delivery**: Kafka offsets committed only after successful ClickHouse insert
- **Exponential Backoff**: Retry delays increase exponentially (2s, 4s, 8s, 16s, 32s, max 60s)

### Performance Targets

- Average: ~2,000 events/second
- Peak: ~20,000 events/second

## Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose

### Run

```bash
make run
```

This starts Kafka, ClickHouse, and the application.

### Stop

```bash
make stop
```

### Service URLs

| Service | URL |
|---------|-----|
| API | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger/index.html |
| Kafka UI | http://localhost:9090 |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/events | Ingest single event |
| POST | /api/v1/events/bulk | Ingest bulk events (max 1000) |
| GET | /api/v1/metrics | Query aggregated metrics |
| GET | /health | Health check |
| GET | /swagger/* | Swagger UI |

## Example Requests

### Ingest Single Event

```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "product_view",
    "channel": "web",
    "campaign_id": "cmp_987",
    "user_id": "user_123",
    "timestamp": 1733900000,
    "tags": ["electronics", "homepage"],
    "metadata": {
      "product_id": "prod-789",
      "price": 129.99,
      "currency": "TRY",
      "referrer": "google"
    }
  }'
```

**Response (202 Accepted):**
```json
{
  "event_id": "a1b2c3d4e5f6...",
  "status": "accepted"
}
```

### Ingest Bulk Events

```bash
curl -X POST http://localhost:8080/api/v1/events/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "event_name": "product_view",
        "user_id": "user_123",
        "timestamp": 1733900000,
        "channel": "web"
      },
      {
        "event_name": "add_to_cart",
        "user_id": "user_456",
        "timestamp": 1733900100,
        "channel": "mobile_app"
      }
    ]
  }'
```

**Response (202 Accepted / 207 Multi-Status):**
```json
{
  "success_count": 2,
  "failed_count": 0,
  "errors": []
}
```

### Query Metrics

```bash
# Basic query
curl "http://localhost:8080/api/v1/metrics?event_name=product_view&from=1733800000&to=1734000000"

# With channel filter
curl "http://localhost:8080/api/v1/metrics?event_name=product_view&from=1733800000&to=1734000000&channel=web"

# With grouping by channel
curl "http://localhost:8080/api/v1/metrics?event_name=product_view&from=1733800000&to=1734000000&group_by=channel"

# With grouping by hour
curl "http://localhost:8080/api/v1/metrics?event_name=product_view&from=1733800000&to=1734000000&group_by=hour"
```

**Response:**
```json
{
  "total_count": 15000,
  "unique_users": 3500,
  "grouped_data": [
    {"key": "web", "total_count": 10000, "unique_users": 2500},
    {"key": "mobile_app", "total_count": 5000, "unique_users": 1000}
  ]
}
```

## Validation Rules

| Field | Rule |
|-------|------|
| event_name | Required, 1-100 characters |
| user_id | Required, 1-100 characters |
| timestamp | Required, Unix epoch, cannot be in the future, max 30 days old |
| channel | Optional, one of: web, mobile_app, api |
| tags | Optional, max 20 items, each max 100 characters |
| metadata | Optional, free-form JSON object |

## Project Structure (DDD)

```
event-ingestion/
├── domain/
│   └── event/
│       ├── event.go           # Entity with validation
│       ├── command.go         # Write commands
│       ├── query.go           # Read queries
│       ├── repository.go      # Repository interface
│       └── errors.go          # Domain errors
├── application/
│   ├── event_service.go       # Event ingestion use cases
│   ├── metrics_service.go     # Metrics query use cases
│   └── dto/                   # Data Transfer Objects
├── infrastructure/
│   ├── config/                # Configuration
│   ├── persistence/
│   │   └── clickhouse/        # ClickHouse implementation
│   └── messaging/
│       └── kafka/             # Kafka producer & consumer
├── presentation/
│   └── api/
│       ├── controller/        # HTTP handlers
│       └── middleware/        # Logging, recovery
├── worker/
│   └── consumer.go            # Kafka consumer workers
├── main.go                    # Entry point
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Configuration

Environment variables (see `.env.example`):

```bash
# Server
SERVER_PORT=8080
SERVER_READ_TIMEOUT=5s
SERVER_WRITE_TIMEOUT=10s

# Kafka
KAFKA_BROKERS=localhost:29092,localhost:29093
KAFKA_TOPIC=events
KAFKA_CONSUMER_GROUP=event-consumers
KAFKA_PARTITIONS=20
KAFKA_REPLICATION_FACTOR=2

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=events_db
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=clickhouse123

# Worker
WORKER_COUNT=20
WORKER_BATCH_SIZE=2000
WORKER_BATCH_TIMEOUT=50ms
RETRY_WORKER_COUNT=2
MAX_RETRY_ATTEMPTS=5

# Logging
LOG_LEVEL=info
```

## Trade-offs & Decisions

### Why Kafka?
- **Async Processing**: API returns immediately (202 Accepted), processing happens in background
- **Buffering**: Handles traffic spikes without losing events
- **Scalability**: Can add more consumers (horizontally) when needed
- **Durability**: Events persisted to disk, survives crashes

### Why ClickHouse?
- **Analytics Optimized**: Designed for OLAP workloads and aggregations
- **Columnar Storage**: Efficient for time-series data
- **ReplacingMergeTree**: Handles duplicate events automatically

### ClickHouse Schema Design

```
┌─────────────────┐      ┌──────────────────────┐      ┌─────────────────┐
│     events      │──MV──│   events_hourly_mv   │─────▶│  events_hourly  │
│ (ReplacingMT)   │      │  (Materialized View) │      │ (AggregatingMT) │
└─────────────────┘      └──────────────────────┘      └─────────────────┘
        │                                                       │
        │                                                       │
   INSERT here                                          SELECT from here
   (raw events)                                         (metrics queries)
```

