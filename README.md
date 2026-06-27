# GhostMQ

GhostMQ is a high-throughput, in-memory queue designed for server environments. It provides a lightweight yet powerful messaging solution, focusing on performance and reliability for microservices and distributed systems. GhostMQ is an open-source project.

## Features

*   **Core Engine & Memory Structures**: Thread-safe foundation with `Message`, `Queue`, and `QueueManager` structs.
*   **Backpressure Router**: Configurable backpressure modes ("block", "drop", "error") to manage queue overflow.
*   **At-Least-Once Consumer Reliability**: Ensures message delivery even if consumers crash, using in-flight tracking and visibility timeouts.
*   **HTTP REST API Layer**: Exposes core functionality via a clean `net/http` interface for easy integration.
*   **Configuration & Lifecycle Infrastructure**: YAML-based configuration for queue topology and graceful shutdown handling.

## Getting Started

1. Configure queues in `ghostmq.yaml`.
3. Run the server:
   ```bash
    docker compose up --build
    ```

### REST API

* `GET /health` - health check
* `GET /metrics` - queue operation metrics
* `GET /queues` - list configured queues
* `POST /queues` - create a new queue
  * body: `{"name":"myqueue","maxSize":1000,"backpressureMode":"block"}`
* `POST /queues/{queue}` - push a JSON message into a queue
  * body: any valid JSON payload
* `GET /queues/{queue}` - pop a message from the queue
* `POST /queues/{queue}/ack` - acknowledge a message
  * body: `{"id":"<message-id>"}`

### Message Delivery

GhostMQ now supports at-least-once message delivery with a visibility timeout. If a popped message is not acknowledged within the queue's visibility timeout, it is returned to the queue.

## Use Cases

GhostMQ is a strong fit when you need a lightweight, low-latency, in-memory queue for internal service communication.

### Good Fit Scenarios

*   AI agent task handoff and orchestration between worker steps
*   Fast request buffering between services in a single deployment environment
*   Lightweight job dispatching for background workers
*   Internal event fan-out for microservices that need simple, fast delivery
*   Burst handling for short-lived traffic spikes without the overhead of a larger broker
*   Low-complexity messaging where durability and replay are not the primary requirement

### Why Choose GhostMQ Instead of RabbitMQ, Kafka, or Redis Streams?

*   Simpler operational model: no heavy broker topology or complex cluster planning
*   Lower overhead for small-to-medium workloads that do not need full-stream semantics
*   Better fit for in-memory, ultra-low-latency use cases where speed matters more than persistence
*   Easier to run and reason about for teams that want a straightforward queue rather than a full event platform

GhostMQ is not meant to replace Kafka for high-volume event streaming or RabbitMQ for feature-rich broker routing. Instead, it is best positioned as a fast, pragmatic queue for simple in-process or single-node service communication.

## Examples and Integration

### Create a queue

```bash
curl -X POST http://localhost:8080/queues \
  -H "Content-Type: application/json" \
  -d '{"name":"agent-jobs","maxSize":1000,"backpressureMode":"block"}'
```

### Publish a message

```bash
curl -X POST http://localhost:8080/queues/agent-jobs \
  -H "Content-Type: application/json" \
  -d '{"task":"summarize","payload":"hello"}'
```

### Consume a message

```bash
curl http://localhost:8080/queues/agent-jobs
```

### Acknowledge a message

```bash
curl -X POST http://localhost:8080/queues/agent-jobs/ack \
  -H "Content-Type: application/json" \
  -d '{"id":"<message-id>"}'
```

### Architecture Overview

For a deeper look at the system structure and request flow, see [doc/architecture.md](doc/architecture.md).

## Contributing

Information on how to contribute to the GhostMQ project will be added here.

## License

This project is licensed under the [LICENSE](LICENSE) file.
