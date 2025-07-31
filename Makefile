.PHONY: build test clean install release-local

# Variables
BINARY_NAME=nsqtop
VERSION ?= dev
LDFLAGS=-ldflags "-s -w -X main.version=$(VERSION)"

# Default target
all: build

# Build for current platform
build:
	go build $(LDFLAGS) -o $(BINARY_NAME) main.go

# Run tests
test:
	go test -v ./...
	go test -race ./...

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME)-*

# Install to GOPATH/bin
install:
	go install $(LDFLAGS) .

# Build for multiple platforms (release)
release-local: clean
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 main.go
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-arm64 main.go
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 main.go
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 main.go
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe main.go

# Development
dev: build
	./$(BINARY_NAME) --help

# Lint and format
lint:
	go fmt ./...
	go vet ./...
	which staticcheck > /dev/null && staticcheck ./... || echo "staticcheck not installed"

# Show help
help:
	@echo "Available targets:"
	@echo "  build        - Build for current platform"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  install      - Install to GOPATH/bin"
	@echo "  release-local- Build for all platforms"
	@echo "  dev          - Build and show help"
	@echo "  lint         - Run linters and formatters"
	@echo "  help         - Show this help"
