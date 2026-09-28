# Pinned code generator versions. protoc itself (v36.2) comes from your package
# manager, e.g. `brew install protobuf`.
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

# go install puts the plugins in $(go env GOPATH)/bin, which may not be on PATH.
GOBIN := $(shell go env GOPATH)/bin

.PHONY: build test proto proto-tools clean

## build: build worker-server and worker into bin/
build:
	go build -o bin/ ./cmd/worker-server ./cmd/worker

## test: run every test with the race detector
test:
	go test -race ./...

## proto: regenerate gen/ from proto/ (the command in jobworker.proto)
proto:
	PATH="$(GOBIN):$$PATH" protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		proto/jobworker/v1/jobworker.proto

## proto-tools: install the pinned protoc plugins
proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

clean:
	rm -rf bin
