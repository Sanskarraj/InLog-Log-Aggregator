.PHONY: all build test test-race test-crash bench run-daemon clean

BINARY_DIR=bin
LSMD_BIN=$(BINARY_DIR)/lsmd
LSMCTL_BIN=$(BINARY_DIR)/lsmctl

all: build

build:
	@mkdir -p $(BINARY_DIR)
	@echo "Building lsmd (LSM Daemon)..."
	go build -o $(LSMD_BIN) ./cmd/lsmd
	@echo "Building lsmctl (CLI)..."
	go build -o $(LSMCTL_BIN) ./cmd/lsmctl
	@echo "Binaries created in $(BINARY_DIR)/"

test:
	go test -v ./pkg/...

test-race:
	go test -race -v ./pkg/...

test-crash:
	go test -v -run TestCrashRecovery ./test/...

bench:
	go test -bench=. -benchmem ./test/...

run-daemon: build
	./$(LSMD_BIN) -data-dir=./data -port=8080 -sync-policy=always

clean:
	rm -rf $(BINARY_DIR) data
