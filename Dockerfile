# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install git (needed for go mod download with some modules)
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY main.go ./

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o nsqtop .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests and ncurses-terminfo for terminal support
RUN apk --no-cache add ca-certificates ncurses-terminfo-base

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/nsqtop .

# Create a non-root user
RUN adduser -D -s /bin/sh nsquser
USER nsquser

# Set the binary as the entrypoint
ENTRYPOINT ["./nsqtop"]
