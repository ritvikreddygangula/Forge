.PHONY: build test test-integration lint run-coordinator run-worker

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run

run-coordinator:
	go run ./cmd/coordinator

run-worker:
	go run ./cmd/worker
