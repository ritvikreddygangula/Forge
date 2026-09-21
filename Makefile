.PHONY: build test test-integration lint run-coordinator run-worker proto

build:
	go build ./...

proto:
	buf generate

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
