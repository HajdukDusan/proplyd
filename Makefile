GO      ?= go
BIN     ?= bin/proplyd
PKGS    := ./...

.PHONY: all build proto test test-short race stress chaos lint tidy docker clean

all: lint test build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/proplyd

# Requires: go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
#           go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
#           go install github.com/bufbuild/buf/cmd/buf@latest
proto:
	buf lint
	buf generate

# Full suite (embedded etcd, stress, chaos) with the race detector.
test:
	$(GO) test -race -count=1 $(PKGS)

# Fast suite: skips embedded etcd and shortens stress/chaos.
test-short:
	$(GO) test -short -count=1 $(PKGS)

stress:
	PROPLYD_STRESS_DURATION=30s PROPLYD_STRESS_CLIENTS=16 $(GO) test -race -count=1 -run TestStress -v ./tests/

chaos:
	PROPLYD_CHAOS_DURATION=60s $(GO) test -race -count=1 -run TestChaos -v ./tests/

lint:
	gofmt -l . | (! grep .)
	$(GO) vet $(PKGS)
	staticcheck $(PKGS)

tidy:
	$(GO) mod tidy

docker:
	docker build -t c12s/proplyd:latest .

clean:
	rm -rf bin
