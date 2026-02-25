PROTO_DIR := proto
GEN_DIR := gen

PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')

.PHONY: all proto clean run

all: proto

proto: $(PROTO_FILES)
	@echo "==> Generating Go code from proto"
	protoc \
		-I $(PROTO_DIR) \
		-I $(shell dirname $$(which protoc))/../include \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=$(GEN_DIR) --grpc-gateway_opt=paths=source_relative \
		$(PROTO_FILES)

clean:
	@echo "==> Cleaning generated code"
	rm -rf $(GEN_DIR)

run:
	go run ./cmd/server

