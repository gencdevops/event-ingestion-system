.PHONY: help run stop build test swagger

help:
	@echo "Event Ingestion Service"
	@echo ""
	@echo "  make run    - Start infrastructure and application"
	@echo "  make stop   - Stop everything"
	@echo "  make build  - Build binary"
	@echo "  make test   - Run tests"

run:
	@echo "Checking port 8080..."
	@lsof -ti:8080 | xargs kill -9 2>/dev/null || true
	@echo "Starting infrastructure..."
	docker-compose up -d
	@echo "Waiting for Kafka..."
	@until docker-compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do \
		sleep 2; \
	done
	@echo "Waiting for ClickHouse..."
	@until curl -s http://localhost:8123/ping >/dev/null 2>&1; do \
		sleep 2; \
	done
	@echo "Starting application..."
	go run main.go

stop:
	@echo "Stopping..."
	-@pkill -f "event-ingestion" 2>/dev/null || true
	docker-compose down

build:
	go build -o bin/event-ingestion main.go

test:
	go test -v ./...

swagger:
	~/go/bin/swag init
