# frkr-ingest-gateway

Ingest Gateway service for the Traffic Mirroring Platform.

## Purpose

The Ingest Gateway receives mirrored requests from SDKs, encrypts them, and stores them in Redpanda/Kafka for streaming to CLI clients.

## Features

- gRPC and HTTP endpoints for message ingestion
- Plugin-based authentication (Basic Auth or OIDC)
- Hybrid encryption (RSA + AES-256-GCM)
- Redpanda/Kafka message production
- Health checks and metrics

## License

Apache 2.0

