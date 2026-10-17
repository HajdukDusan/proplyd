GO      ?= GOTOOLCHAIN=local go
BIN     ?= bin/proplyd
PKGS    := ./...

.PHONY: all build proto test test-short lint tidy docker clean

all: lint test build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/proplyd

# Requires buf, protoc-gen-go v1.36.6 and protoc-gen-go-grpc v1.5.1 on PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
proto:
	buf lint
	buf generate

# Full suite with the race detector. Needs redis-server on PATH (or
# PROPLYD_REDIS_SERVER); embedded etcd is started in-process.
test:
	$(GO) test -race -count=1 $(PKGS)

# Skips embedded etcd.
test-short:
	$(GO) test -short -count=1 $(PKGS)

lint:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	$(GO) vet $(PKGS)
	staticcheck $(PKGS)

tidy:
	$(GO) mod tidy

docker:
	docker build -t c12s/proplyd:latest .

clean:
	rm -rf bin
