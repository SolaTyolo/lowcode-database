PROTO_DIR := proto
GEN_DIR := gen

PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')

# Use project-local proto/google/ for google/api/*.proto; ensure Go proto plugins are on PATH.
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: all proto clean run

all: proto

proto: $(PROTO_FILES)
	@echo "==> Generating Go code from proto"
	@mkdir -p $(GEN_DIR)
	protoc \
		-I $(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=$(GEN_DIR) --grpc-gateway_opt=paths=source_relative \
		proto/lowcode/v1/lowcode_service.proto

clean:
	@echo "==> Cleaning generated code"
	rm -rf $(GEN_DIR)

run:
	go run ./cmd/server
