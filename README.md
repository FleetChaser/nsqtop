# NSQTop

A `top`-like monitoring tool for NSQ clusters, built in Go with a rich terminal interface.

## Features

- **Real-time monitoring**: Live updates of NSQ cluster statistics
- **Rich terminal UI**: Table-based display with color coding and sparkline trends
- **Multi-server support**: Monitor multiple nsqlookupd and nsqd instances
- **Rate tracking**: Display both per-second and per-minute message rates
- **Cross-platform**: Available for Linux, macOS, and Windows
- **Docker support**: Pre-built Docker images available

## Installation

### Download Binary

Download the latest release for your platform from the [releases page](../../releases):

```bash
# Linux x64
wget https://github.com/FleetChaser/nsqtop/releases/latest/download/nsqtop-linux-amd64.tar.gz
tar -xzf nsqtop-linux-amd64.tar.gz

# macOS x64
wget https://github.com/FleetChaser/nsqtop/releases/latest/download/nsqtop-darwin-amd64.tar.gz
tar -xzf nsqtop-darwin-amd64.tar.gz

# Windows x64
# Download nsqtop-windows-amd64.zip and extract
```

### Build from Source

```bash
git clone https://github.com/FleetChaser/nsqtop.git
cd nsqtop
go build -o nsqtop main.go
```

### Docker

```bash
# Pull the image
docker pull ghcr.io/fleetchaser/nsqtop:latest

# Run with docker
docker run --rm -it ghcr.io/fleetchaser/nsqtop:latest --lookupd-http-address "your-nsqlookupd:4161"
```

## Usage

### Basic Usage

```bash
# Monitor a single nsqlookupd instance
./nsqtop --lookupd-http-address localhost:4161

# Monitor multiple nsqlookupd instances
./nsqtop --lookupd-http-address "localhost:4161,localhost:4162"

# Custom refresh interval (default: 2 seconds)
./nsqtop --lookupd-http-address localhost:4161 --interval 5
```

### Environment Variables

You can also configure nsqtop using environment variables:

```bash
export NSQTOP_LOOKUPD_ADDRESSES="localhost:4161,localhost:4162"
export NSQTOP_INTERVAL=3
./nsqtop
```

### Command Line Options

```
Usage:
  nsqtop [flags]

Flags:
  -h, --help                          help for nsqtop
  -i, --interval int                  Refresh interval in seconds (default 2)
  -l, --lookupd-http-address string   Comma-separated HTTP addresses of nsqlookupd instances
```

## Interface

The terminal interface displays:

- **Header**: Current time and connection status
- **Summary**: Total in-flight messages, channel count, and trend sparkline
- **Table**: Detailed statistics for each topic/channel combination
  - **Topic/Channel**: Name of the topic and channel
  - **Depth**: Number of queued messages (color-coded by severity)
  - **In-Flight**: Number of messages currently being processed
  - **Rate/sec**: Messages processed per second
  - **Rate/min**: Messages processed per minute

### Color Coding

- **Green**: Normal depth (< 100 messages)
- **Yellow**: Warning depth (100-999 messages)
- **Red**: Critical depth (≥ 1000 messages)

## Development

### Prerequisites

- Go 1.21 or later
- Git

### Building

```bash
# Get dependencies
go mod download

# Build for current platform
go build -o nsqtop main.go

# Build for specific platform
GOOS=linux GOARCH=amd64 go build -o nsqtop-linux-amd64 main.go
```

### Testing

```bash
# Run tests
go test ./...

# Run with race detection
go test -race ./...

# Static analysis
go vet ./...
staticcheck ./...
```

## Release Process

This project uses GitHub Actions for automated building and releasing:

1. **Push a tag**: `git tag v1.0.0 && git push origin v1.0.0`
2. **Automatic build**: GitHub Actions builds binaries for multiple platforms
3. **Release creation**: A GitHub release is created with downloadable assets
4. **Docker images**: Multi-architecture Docker images are built and pushed

### Supported Platforms

- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

## Migration from Python Version

This Go version provides the same functionality as the Python version with these improvements:

- **Better performance**: Native binary with lower resource usage
- **No dependencies**: Single binary with no external runtime requirements
- **Faster startup**: Immediate execution without interpreter overhead
- **Better distribution**: Pre-compiled binaries for all major platforms

## License

This project is open source and available under the [MIT License](LICENSE).

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
