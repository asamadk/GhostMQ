# GhostMQ

GhostMQ is a high-throughput, in-memory queue designed for server environments. It provides a lightweight yet powerful messaging solution, focusing on performance and reliability for microservices and distributed systems. GhostMQ is an open-source project.

## Features (Planned)

*   **Core Engine & Memory Structures**: Thread-safe foundation with `Message`, `Queue`, and `QueueManager` structs.
*   **Backpressure Router**: Configurable backpressure modes ("block", "drop", "error") to manage queue overflow.
*   **At-Least-Once Consumer Reliability**: Ensures message delivery even if consumers crash, using in-flight tracking and visibility timeouts.
*   **HTTP REST API Layer**: Exposes core functionality via a clean `net/http` interface for easy integration.
*   **Configuration & Lifecycle Infrastructure**: YAML-based configuration for queue topology and graceful shutdown handling.

## Getting Started

1. Install Go 1.24 or later.
2. Configure queues in `ghostmq.yaml`.
3. Run the server:

   ```bash
   go run main.go
   ```

### REST API

* `GET /health` - health check
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

## Contributing

Information on how to contribute to the GhostMQ project will be added here.

## License

This project is licensed under the [LICENSE](LICENSE) file.
