# frkr-ingest-gateway

Ingest Gateway service for frkr.

## Overview

The Ingest Gateway receives mirrored HTTP requests from SDKs, authenticates them, and publishes them to a Kafka-compatible message broker for streaming to CLI clients.

## Features

- HTTP endpoint for message ingestion
- Basic authentication support
- Automatic topic routing based on stream configuration
- Health check endpoint
- Kafka-compatible message broker integration

## Installation

```bash
go get github.com/frkr-io/frkr-ingest-gateway
```

## Building

```bash
make build
```

The binary will be created in the `bin/` directory as `bin/gateway`.

To clean build artifacts:

```bash
make clean
```

## Configuration

The gateway can be configured via command-line flags or environment variables:

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--http-port` | `HTTP_PORT` | `8080` | HTTP server port |
| `--db-url` | `DB_URL` | `postgres://root@localhost:26257/frkrdb?sslmode=disable` | Database connection URL |
| `--broker-url` | `BROKER_URL` | `localhost:19092` | Kafka-compatible broker URL |

## Usage

### Start the Gateway

```bash
./bin/gateway \
  --http-port=8082 \
  --db-url="postgres://user@localhost/dbname?sslmode=disable" \
  --broker-url="localhost:9092"
```

### Send a Request

```bash
curl -X POST http://localhost:8082/ingest \
  -H "Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=" \
  -H "Content-Type: application/json" \
  -d '{
    "stream_id": "my-api",
    "request": {
      "method": "GET",
      "path": "/api/users",
      "headers": {"Content-Type": "application/json"},
      "body": "",
      "query": {},
      "timestamp_ns": 1234567890,
      "request_id": "req-123"
    }
  }'
```

### Health Check

```bash
curl http://localhost:8082/health
```

## API Reference

### POST /ingest

Ingests a mirrored HTTP request.

**Headers:**
- `Authorization: Basic <base64-encoded-credentials>` (required)

**Request Body:**
```json
{
  "stream_id": "string",
  "request": {
    "method": "string",
    "path": "string",
    "headers": {},
    "body": "string",
    "query": {},
    "timestamp_ns": 0,
    "request_id": "string"
  }
}
```

**Response:**
- `202 Accepted` - Request ingested successfully
- `400 Bad Request` - Invalid request format
- `401 Unauthorized` - Authentication failed
- `404 Not Found` - Stream not found
- `500 Internal Server Error` - Server error

### GET /health

Health check endpoint.

**Response:**
- `200 OK` - Service is healthy

## Requirements

- Go 1.21 or later
- PostgreSQL-compatible database
- Kafka-compatible message broker

## License

Apache 2.0
