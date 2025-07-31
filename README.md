# nsq_top

A `top`-like monitoring tool for NSQ clusters that provides real-time visibility into NSQ topics, channels, and message flow.

## Features

- Real-time monitoring of NSQ clusters
- Support for multiple nsqlookupd servers for high availability
- Visual sparkline charts showing in-flight message trends
- Color-coded backlog depth indicators
- Interactive terminal UI with live updates

## Building the Docker Image

To build the Docker image, run the following command from this directory:

```bash
docker build -t nsq_top .
```

## Running the Container

### Using Environment Variables (Recommended)

Configure the container using environment variables for easy deployment:

**Single nsqlookupd server:**
```bash
docker run -it --rm \
  -e NSQ_LOOKUPD_ADDRESSES=your-nsqlookupd:4161 \
  nsq_top
```

**Multiple nsqlookupd servers (for high availability):**
```bash
docker run -it --rm \
  -e NSQ_LOOKUPD_ADDRESSES=nsqlookupd1:4161,nsqlookupd2:4161,nsqlookupd3:4161 \
  nsq_top
```

**With custom refresh interval:**
```bash
docker run -it --rm \
  -e NSQ_LOOKUPD_ADDRESSES=your-nsqlookupd:4161 \
  -e NSQ_TOP_INTERVAL=5 \
  nsq_top
```

### Using Command-Line Arguments

You can also pass configuration via command-line arguments:

**Single server:**
```bash
docker run -it --rm nsq_top python nsq_top.py --lookupd-http-address your-nsqlookupd:4161
```

**Multiple servers:**
```bash
docker run -it --rm nsq_top python nsq_top.py --lookupd-http-address "nsqlookupd1:4161,nsqlookupd2:4161"
```

## Configuration Options

### Environment Variables

- `NSQ_LOOKUPD_ADDRESSES` (required): Comma-separated list of nsqlookupd HTTP addresses (http:// prefix optional)
- `NSQ_LOOKUPD_ADDRESS` (fallback): Single nsqlookupd HTTP address (for backward compatibility)
- `NSQ_TOP_INTERVAL` (optional): Refresh interval in seconds. Defaults to `2`

### Command-Line Arguments

- `--lookupd-http-address`: Comma-separated HTTP addresses of nsqlookupd instances (http:// prefix optional)
- `--interval`: Refresh interval in seconds

## Running Locally (without Docker)

If you have Python and `uv` installed, you can run the tool directly:

```bash
# Install dependencies
uv sync

# Run with environment variables (using uv run)
NSQ_LOOKUPD_ADDRESSES=localhost:4161 uv run python nsq_top.py

# Or activate the virtual environment and run directly
source .venv/bin/activate
NSQ_LOOKUPD_ADDRESSES=localhost:4161 python nsq_top.py

# Or with command-line arguments
.venv/bin/python nsq_top.py --lookupd-http-address localhost:4161

# Alternative: using uv run (if you prefer)
uv run python nsq_top.py --lookupd-http-address localhost:4161
```

## Docker Compose Example

For easy deployment with NSQ, here's a docker-compose example:

```yaml
version: '3.8'
services:
  nsq_top:
    build: .
    environment:
      - NSQ_LOOKUPD_ADDRESSES=nsqlookupd:4161
      - NSQ_TOP_INTERVAL=3
    depends_on:
      - nsqlookupd
    tty: true
    stdin_open: true
```

## Requirements

- Python 3.8+
- Dependencies: `requests`, `blessed`
- Terminal with color support for best experience
