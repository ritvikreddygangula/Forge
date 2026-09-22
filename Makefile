.PHONY: build test test-integration lint run-coordinator run-worker proto

build:
	go build ./...

proto:
	buf generate

compose-up:
	docker compose -f deploy/docker-compose.yml up -d

compose-down:
	docker compose -f deploy/docker-compose.yml down

test:
	go test -race ./...

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run

run-coordinator:
	go run ./cmd/coordinator

run-worker:
	go run ./cmd/worker
