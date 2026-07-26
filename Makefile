.PHONY: build test test-race

GOCACHE ?= /tmp/dkv-go-cache

build:
	GOCACHE=$(GOCACHE) go build -buildvcs=false -o bin/dkv ./cmd/dkv

test:
	GOCACHE=$(GOCACHE) go test ./...

test-race:
	GOCACHE=$(GOCACHE) go test -race ./...
